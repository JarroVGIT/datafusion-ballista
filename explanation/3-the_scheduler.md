# 3. The scheduler

**What you will know after reading this:** the types and modules that make up the scheduler process, what state it tracks, how it turns a submitted plan into running tasks, what the stage state machine actually does, and where the knobs are.

This file assumes you have read file 2 and have the cluster topology in your head. We are now opening the box labelled "scheduler" and looking at what is inside.

---

## 3.1 The module map

Top of [ballista/scheduler/src/lib.rs](../ballista/scheduler/src/lib.rs):

```rust
pub mod api;                  // REST API endpoints (feature: rest-api)
pub mod cluster;              // Cluster backend trait and in-memory implementation
pub mod config;               // SchedulerConfig and CLI flags
pub mod display;              // Pretty-printing for plans and metrics
pub mod metrics;              // Metrics collection (Prometheus optional)
pub mod physical_optimizer;   // Distributed physical optimizer rules
pub mod planner;              // Top-level planning utilities for distribution
pub mod scheduler_process;    // Process lifecycle (boot, shutdown)
pub mod scheduler_server;     // Core gRPC server and event loop
pub mod standalone;           // In-process scheduler used by SessionContext::standalone()
pub mod state;                // SchedulerState, ExecutionGraph, ExecutionStage, ...
```

The two heavyweights are `scheduler_server` and `state`. Everything else is supporting cast.

---

## 3.2 The top-level types

### `SchedulerServer`

Defined in [ballista/scheduler/src/scheduler_server/mod.rs](../ballista/scheduler/src/scheduler_server/mod.rs). This is the long-lived service object. It owns:

- A `SchedulerState<T, U>` (the cluster's worldview).
- An event-loop sender for `QueryStageSchedulerEvent` ([ballista/scheduler/src/scheduler_server/event.rs](../ballista/scheduler/src/scheduler_server/event.rs)).
- gRPC service handlers ([ballista/scheduler/src/scheduler_server/grpc.rs](../ballista/scheduler/src/scheduler_server/grpc.rs)).
- The query-stage scheduler ([ballista/scheduler/src/scheduler_server/query_stage_scheduler.rs](../ballista/scheduler/src/scheduler_server/query_stage_scheduler.rs)) which translates events into state transitions and task dispatches.
- The external scaler interface ([ballista/scheduler/src/scheduler_server/external_scaler.rs](../ballista/scheduler/src/scheduler_server/external_scaler.rs)) used by KEDA when the `keda-scaler` feature is on.

The two type parameters `T: AsLogicalPlan` and `U: AsExecutionPlan` are the codec hooks. The default is the Ballista protobuf codec. If you want to use Substrait or your own format, you parameterize the server with different types. See file 6 for what those traits mean.

### `SchedulerState`

Defined in [ballista/scheduler/src/state/mod.rs](../ballista/scheduler/src/state/mod.rs). This is the in-memory view of the world. It owns three managers:

- **`ExecutorManager`** ([ballista/scheduler/src/state/executor_manager.rs](../ballista/scheduler/src/state/executor_manager.rs)) — registry of which executors are alive, what slots they have, when they last heartbeated, what their `ExecutorMetadata` looks like.
- **`TaskManager`** ([ballista/scheduler/src/state/task_manager.rs](../ballista/scheduler/src/state/task_manager.rs)) — which tasks are queued, running, succeeded, failed; the `TaskLauncher` that actually dispatches tasks (push vs pull).
- **`SessionManager`** ([ballista/scheduler/src/state/session_manager.rs](../ballista/scheduler/src/state/session_manager.rs)) — per-session config and registered tables, so that the scheduler can reconstruct a `SessionState` to plan against.

These three are the data plane of the scheduler. The event loop reads and mutates them in response to incoming events.

### `BallistaCluster`

Defined in [ballista/scheduler/src/cluster/mod.rs](../ballista/scheduler/src/cluster/mod.rs). This is the *backend* for cluster state — the thing the managers above persist to. The trait abstracts away "where does this state actually live" so that in principle you could plug in a distributed store (etcd, Postgres, Redis) and run multiple schedulers against it. In practice the only implementation today is [ballista/scheduler/src/cluster/memory.rs](../ballista/scheduler/src/cluster/memory.rs), which is plain `Arc<Mutex<...>>`. The seams exist; the persistent backends do not.

---

## 3.3 The ExecutionGraph: how a query becomes a DAG

When a job is submitted, the scheduler builds an `ExecutionGraph` ([ballista/scheduler/src/state/execution_graph.rs](../ballista/scheduler/src/state/execution_graph.rs)). This is the central data structure of the scheduler. Think of it as the query plan, broken at shuffle boundaries, with each chunk turned into a stage and edges representing data dependencies between stages.

The graph holds:

- A list of `ExecutionStage`s, keyed by `stage_id`.
- Edges: which stages feed which other stages.
- The output schema of the final stage.
- Aggregate stats: how many tasks total, how many done, how many failed.
- A `JobStatus` ([ballista/core/proto/ballista.proto](../ballista/core/proto/ballista.proto)) summary.

The graph is built once at job submission and mutated in place as stages progress. There is a GraphViz exporter at [ballista/scheduler/src/state/execution_graph_dot.rs](../ballista/scheduler/src/state/execution_graph_dot.rs) (feature: `graphviz-support`) which is invaluable when debugging — for a complex query, generating the DOT and rendering it shows the stage structure at a glance.

---

## 3.4 The stage state machine

This is the conceptual core of the scheduler. Every stage moves through a small state machine that is documented in the code at [ballista/scheduler/src/state/execution_stage.rs:45](../ballista/scheduler/src/state/execution_stage.rs#L45):

```text
UnResolvedStage           FailedStage
      ↓            ↙           ↑
 ResolvedStage     →     RunningStage
                               ↓
                        SuccessfulStage
```

The five variants of the enum at [ballista/scheduler/src/state/execution_stage.rs:62](../ballista/scheduler/src/state/execution_stage.rs#L62):

- **`UnResolved`** — At least one upstream stage has not produced its output yet. The plan still contains `UnresolvedShuffleExec` placeholders. The stage cannot run because it does not yet know where to read its inputs from.
- **`Resolved`** — All upstream stages are `Successful`. The scheduler has rewritten the plan, replacing every `UnresolvedShuffleExec` with a real `ShuffleReaderExec` populated with concrete `PartitionLocation`s. The stage is ready to be scheduled but no tasks have been dispatched yet.
- **`Running`** — At least one task has been dispatched. Some may already have completed; others may still be queued at the executor. Per-partition task status is tracked.
- **`Successful`** — Every task in the stage has reported success. The stage's output partitions exist on disk on the reporting executors, and their locations are known. Downstream stages can now transition to `Resolved`.
- **`Failed`** — A task failed terminally (after `--task-max-failures` retries) or the stage was cancelled. The job is failing.

Two ideas are worth pausing on.

**"Resolved" means more than scheduled.** It specifically means *every input partition location is known*. Building this set requires walking the upstream stages, collecting their `PartitionLocation` outputs from `UpdateTaskStatus` calls, and stitching them into a `ShuffleReaderExec`. The "stitching" is what `physical_optimizer` rules do at runtime, not just at planning time.

**Stages can go back.** If an upstream executor dies and takes its shuffle outputs with it, a downstream `Resolved` or `Running` stage may report `ResultLost` ([ballista/core/proto/ballista.proto:494](../ballista/core/proto/ballista.proto#L494)). The scheduler then transitions the upstream stage back to `Resolved` (or further), reruns it, and re-resolves the downstream. This is the loop that gives Ballista its limited fault tolerance.

---

## 3.5 The two scheduling policies

Set on both ends via `--scheduling-policy` / `--task-scheduling-policy`. They must match.

- **`pull-staged`** (default) — Executors poll the scheduler. They call `PollWork` ([ballista/core/proto/ballista.proto:825](../ballista/core/proto/ballista.proto#L825)) periodically; the scheduler responds with task definitions for any work it has assigned to that executor. The execution loop on the executor side is [ballista/executor/src/execution_loop.rs](../ballista/executor/src/execution_loop.rs).

  Strength: simple to reason about; scheduler does not have to know how to reach executors (firewalls, NATs, dynamic IPs become non-issues).

  Weakness: latency floor of the poll interval; more chattiness; somewhat harder to express priority.

- **`push-staged`** — Scheduler pushes. When it assigns a task, it calls `LaunchTask` ([ballista/core/proto/ballista.proto:856](../ballista/core/proto/ballista.proto#L856)) directly on the chosen executor's gRPC service. The executor's gRPC handler is at [ballista/executor/src/executor_server.rs](../ballista/executor/src/executor_server.rs).

  Strength: lower latency for individual tasks; scheduler controls the dispatch order precisely.

  Weakness: scheduler must be able to reach every executor on its `bind_grpc_port`.

For most deployments, `pull-staged` is the safer default. `push-staged` shines when you have low-latency, intra-VPC executors.

Within a policy, the *task distribution* across executors is controlled by `TaskDistributionPolicy` ([ballista/scheduler/src/config.rs:462](../ballista/scheduler/src/config.rs#L462)):

- `Bias` — pack as many tasks as possible onto the first available executor (default). Good for cache locality.
- `RoundRobin` — spread tasks evenly across executors. Good for utilization.
- `Custom(Arc<dyn DistributionPolicy>)` — your own policy. Hook point for affinity-aware scheduling, GPU executors, multi-tenant fairness.

---

## 3.6 The event loop

The scheduler does not implement gRPC handlers as direct mutations of state. Instead, the gRPC handlers emit events into an in-process channel; a single consumer task drains the channel and applies state transitions. The event type is `QueryStageSchedulerEvent` ([ballista/scheduler/src/scheduler_server/event.rs](../ballista/scheduler/src/scheduler_server/event.rs)).

This matters for three reasons:

1. **Serialized state updates.** All mutations of the cluster state go through one consumer, so you do not need to reason about concurrent reads and writes to the `ExecutionGraph`.
2. **Backpressure.** The channel has a fixed size (`--event-loop-buffer-size`, default 10,000 in the library, 1,000 in the CLI default). If you swamp the scheduler with submissions, the channel fills up and gRPC handlers block. There is a `--scheduler-event-expected-processing-duration` flag to warn (or fail) if a single event takes too long.
3. **Audit and replay potential.** Because every state mutation is an event, in principle you could ship the event stream to an external store for HA. The current code does not do this, but the architecture supports it.

If you ever extend the scheduler — for example, to handle a new RPC for tenant management — you almost certainly want to follow this pattern: gRPC handler enqueues an event, the event loop applies it.

---

## 3.7 Job submission RPC, end to end

The most useful walkthrough is `ExecuteQuery` ([ballista/core/proto/ballista.proto:841](../ballista/core/proto/ballista.proto#L841)). When the client calls it:

1. The gRPC handler in `grpc.rs` deserializes the `ExecuteQueryParams`, which contains the logical plan (protobuf-encoded via the configured `LogicalExtensionCodec`) and session settings.
2. The handler asks the `SessionManager` for the `SessionContext` corresponding to the client's session ID, creating one if needed.
3. The logical plan is deserialized back into a DataFusion `LogicalPlan` using the codec.
4. DataFusion's standard physical planner produces a physical plan.
5. The distributed physical optimizer in [ballista/scheduler/src/physical_optimizer/](../ballista/scheduler/src/physical_optimizer/) walks the physical plan and inserts stage boundaries (file 5 covers exactly how).
6. An `ExecutionGraph` is built. Stages with no inputs are `Resolved`; the rest are `UnResolved`.
7. The graph is stored in the `TaskManager`. An event is enqueued to start scheduling.
8. The handler returns `ExecuteQueryResult { job_id }` to the client.

The client then polls `GetJobStatus` ([ballista/core/proto/ballista.proto:843](../ballista/core/proto/ballista.proto#L843)) (or holds open the `ExecuteQueryPush` server stream) until the job completes, then fetches results.

The push variant `ExecuteQueryPush` streams `GetJobStatusResult` updates to the client as the job progresses. Use it if you want UI-style progress reporting; use `ExecuteQuery` + polling for simpler clients.

---

## 3.8 Adaptive Query Execution (AQE)

The `state/aqe/` directory ([ballista/scheduler/src/state/aqe/](../ballista/scheduler/src/state/aqe/)) holds the Adaptive Query Execution machinery. AQE is the idea, borrowed from Spark, that you can re-optimize parts of the plan at runtime once you have real partition statistics from already-completed upstream stages.

Concretely, AQE in Ballista today:

- Looks at the actual row counts and byte sizes coming out of completed upstream stages.
- Can decide to coalesce many small downstream partitions into a few larger ones (`CoalescePlan` in [ballista/core/proto/ballista.proto:104](../ballista/core/proto/ballista.proto#L104)) to avoid task startup overhead.
- Can switch join strategies if the actual upstream size turns out to be small enough for a broadcast.
- Wires this into the stage state machine: when an upstream `Successful` stage's outputs are about to be wired into a downstream stage, AQE rules run during the `UnResolved → Resolved` transition.

AQE is an active area of work. The roadmap entry "Implement Adaptive Query Execution" ([ROADMAP.md](../ROADMAP.md)) is still partial. For now: assume AQE will improve simple cases (partition coalescing) but do not rely on it for sophisticated mid-flight replanning. If your workload depends on this, plan to contribute or pin specific optimizations yourself.

---

## 3.9 The REST API

If you compile the scheduler with `--features rest-api`, you also get an HTTP server multiplexed onto the same port. Routes are defined in [ballista/scheduler/src/api/](../ballista/scheduler/src/api/). What it gives you:

- Listing executors, with their status and slot availability.
- Listing jobs and inspecting their `ExecutionGraph`.
- Cancelling jobs.
- Submitting queries (an HTTP wrapper around `ExecuteQuery`).

This is what the TUI in [ballista-cli/src/tui/](../ballista-cli/src/tui/) talks to — see file 8. It is also what you would put a UI dashboard against if you build one for Eur Data operators.

The REST API is not authenticated. If you expose it outside trusted networks you must put a gateway in front.

---

## 3.10 Metrics

[ballista/scheduler/src/metrics/](../ballista/scheduler/src/metrics/) holds the metrics interface. With the `prometheus-metrics` feature on, the scheduler exposes a Prometheus scrape endpoint. Metrics include job count, stage count, task duration histograms, executor count, event loop latency. For production, turn this on and scrape it; the alternative is staring at logs.

For per-job metrics (operator-level row counts, scan times, etc.), use `GetJobMetrics` ([ballista/core/proto/ballista.proto:845](../ballista/core/proto/ballista.proto#L845)) — this returns per-operator metrics aggregated from executors. The TUI uses this.

---

## 3.11 What the scheduler does *not* do

Calling these out so you do not look for them and waste time:

- **Authentication and authorization.** The scheduler gRPC accepts whatever calls in. There is mTLS support ([examples/examples/mtls-cluster.rs](../examples/examples/mtls-cluster.rs)) but no user/role model.
- **Cost-based optimization across queries.** Optimization is per-query and based on DataFusion's optimizer plus the local stats Ballista can collect during planning. There is no workload manager that, say, decides to run query A on warm caches because it shares a join with query B.
- **Result caching.** Each query is independent. Nothing in the scheduler remembers "I just ran this query, here is the cached result." You would layer this above the scheduler.
- **Storage of query history past the cleanup interval.** Job state evaporates after `--finished-job-state-clean-up-interval-seconds`. If you want a permanent audit log, ship events to an external store.
- **Persistence at all.** Already said in file 2, worth repeating: in-memory state, restart loses it.

---

## 3.12 Where to next

You now know the control plane. Next: the data plane.

- Next: [4-the_executor.md](4-the_executor.md) — what is inside an executor.
- Then: [5-planning_stages_and_shuffles.md](5-planning_stages_and_shuffles.md) — how stage boundaries actually get inserted into the plan.
