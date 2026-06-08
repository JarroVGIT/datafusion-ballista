# 2. Cluster topology and lifecycle

**What you will know after reading this:** the three roles in a Ballista cluster, what ports they use, how they discover each other at startup, and the eight steps a query goes through from `ctx.sql(...)` to a `RecordBatch` in your hands. Enough that you can draw the box-and-arrow diagram on a whiteboard and reason about what happens when something dies.

This file deliberately stays at the box-and-arrow level. Files 3 and 4 dive into the scheduler and executor internals; this one is the map you keep open while reading them.

---

## 2.1 The three roles

A Ballista cluster has three roles. Any process can play one or more of them.

### Client

The application you write. It holds a `SessionContext` that has been "upgraded" for Ballista. Concretely, the upgrade does two things:

1. Installs a `BallistaQueryPlanner` ([ballista/core/src/planner.rs:41](../ballista/core/src/planner.rs#L41)) into the `SessionState`. This is the hook that intercepts physical planning and ships work to the scheduler instead of running locally.
2. Sets a handful of Ballista-specific defaults on the `SessionConfig` (codec choice, broadcast threshold, gRPC message size, etc.).

The client never talks to executors directly for control flow. It only talks to the scheduler. It *may* receive results streamed from executors via Arrow Flight, depending on configuration (the default in standalone mode pulls results back through the scheduler's gRPC stream).

### Scheduler

A separate process. One per cluster today; multi-scheduler is on the roadmap ([ROADMAP.md](../ROADMAP.md)). Its job:

- Accept job submissions from clients over gRPC.
- Take a logical plan, break it into a DAG of stages, and decide which executor should run each task.
- Track executor registration and heartbeats.
- Track task and stage status.
- Return results back to the client.

The scheduler is the only piece that knows the *whole picture* of a query. Executors only see the slice of the plan they have been assigned.

Binary: built from [ballista/scheduler/src/bin/main.rs](../ballista/scheduler/src/bin/main.rs). Library entry point: [ballista/scheduler/src/lib.rs](../ballista/scheduler/src/lib.rs). Lifecycle: [ballista/scheduler/src/scheduler_process.rs](../ballista/scheduler/src/scheduler_process.rs).

### Executor

N processes. Each one registers with the scheduler at startup, heartbeats periodically, and runs tasks the scheduler hands it. Its job:

- Receive task definitions over its gRPC service.
- Decode the physical plan, execute it against the partitions it owns.
- Write shuffle outputs (Arrow IPC files) to local disk.
- Serve shuffle reads to other executors via Arrow Flight.
- Report task status back to the scheduler.

Binary: built from [ballista/executor/src/bin/main.rs](../ballista/executor/src/bin/main.rs). Lifecycle: [ballista/executor/src/executor_process.rs](../ballista/executor/src/executor_process.rs).

---

## 2.2 Port and network layout

Default ports, all configurable. Knowing them makes log lines and firewall rules legible.

| Process | Port | Protocol | Purpose |
|---|---|---|---|
| Scheduler | 50050 | gRPC | Job submission, executor registration, task status updates |
| Scheduler | 50050 | HTTP (REST) | Optional REST API and TUI backend (requires `rest-api` feature) |
| Executor | 50051 | Arrow Flight (gRPC) | Shuffle data reads from other executors |
| Executor | 50052 | gRPC | Task launch and control from scheduler |

All flags live in:

- [ballista/scheduler/src/config.rs:41](../ballista/scheduler/src/config.rs#L41) — `--bind-host`, `--external-host`, `--bind-port` (scheduler gRPC).
- [ballista/executor/src/config.rs:52](../ballista/executor/src/config.rs#L52) — `--scheduler-host`, `--scheduler-port`, `--bind-host`, `--bind-port` (Flight), `--bind-grpc-port` (control), `--external-host`.

The distinction between `bind-host` and `external-host` matters in containerized deployments: you typically bind to `0.0.0.0` inside the container but advertise a routable hostname to peers. The scheduler uses `external-host` when telling executors how to reach each other for shuffles. Get this wrong and your query stalls at the first shuffle.

---

## 2.3 Cold-start sequence

What happens when you bring up a cluster from nothing.

1. **Scheduler starts.**
   - Parses flags ([ballista/scheduler/src/bin/main.rs](../ballista/scheduler/src/bin/main.rs)).
   - Constructs a `SchedulerConfig` ([ballista/scheduler/src/config.rs:202](../ballista/scheduler/src/config.rs#L202)).
   - Builds a `BallistaCluster` backend. The only one shipped today is the in-memory backend at [ballista/scheduler/src/cluster/memory.rs](../ballista/scheduler/src/cluster/memory.rs). All cluster state (executors, sessions, jobs) lives in RAM. Restart loses everything.
   - Starts the gRPC server on `bind-port`.
   - If `rest-api` feature is enabled and `--disable-rest-api` is not set, also starts the HTTP server on the same port (multiplexed) ([ballista/scheduler/src/api/](../ballista/scheduler/src/api/)).
   - Spawns a background loop that scans for dead executors every `--expire-dead-executor-interval-seconds` (default 15s) and reaps executors that have missed heartbeats for `--executor-timeout-seconds` (default 180s).
   - Spawns a background loop that cleans up finished job data every `--finished-job-data-clean-up-interval-seconds` (default 300s).

2. **Executor starts.**
   - Parses flags ([ballista/executor/src/bin/main.rs](../ballista/executor/src/bin/main.rs)).
   - Constructs an `ExecutorProcessConfig` from `Config` (see [ballista/executor/src/config.rs:183](../ballista/executor/src/config.rs#L183) for the conversion).
   - Connects to the scheduler at `scheduler_host:scheduler_port`. If `scheduler-connect-timeout-seconds` is 0 it gives up after the first failed attempt; otherwise it retries for that many seconds.
   - Calls `RegisterExecutor` gRPC ([ballista/core/proto/ballista.proto:827](../ballista/core/proto/ballista.proto#L827)). The scheduler now knows this executor exists and how to reach it.
   - Starts its own gRPC server on `bind-grpc-port` (for the scheduler to push tasks at it, if push-staged scheduling).
   - Starts an Arrow Flight server on `bind-port` ([ballista/executor/src/flight_service.rs](../ballista/executor/src/flight_service.rs)) so peer executors can fetch shuffle outputs.
   - Spawns the heartbeat task: every `--executor-heartbeat-interval-seconds` (default 60s) it calls `HeartBeatFromExecutor` ([ballista/core/proto/ballista.proto:831](../ballista/core/proto/ballista.proto#L831)).
   - If `--task-scheduling-policy=pull-staged`, also starts the [execution loop](../ballista/executor/src/execution_loop.rs) that polls `PollWork` ([ballista/core/proto/ballista.proto:825](../ballista/core/proto/ballista.proto#L825)) for new tasks.

3. **Client connects.**
   - Calls `SessionContext::remote("df://host:50050")` ([ballista/client/src/extension.rs:113](../ballista/client/src/extension.rs#L113)).
   - Parses the URL, builds a `SessionState` via `new_ballista_state` (which installs the `BallistaQueryPlanner` and Ballista codec defaults).
   - The first gRPC call typically happens lazily, on the first query.

The cluster is now ready. The scheduler knows about every registered executor and their available task slots; executors know how to reach the scheduler; clients know how to reach the scheduler.

---

## 2.4 The two deployment shapes

### Standalone (single process)

`SessionContext::standalone()` ([ballista/client/src/extension.rs:146](../ballista/client/src/extension.rs#L146)) collapses all three roles into one process. The internals are revealing:

1. Calls `ballista_scheduler::standalone::new_standalone_scheduler()` ([ballista/scheduler/src/standalone.rs](../ballista/scheduler/src/standalone.rs)) which binds the scheduler gRPC on a random local port and returns the `SocketAddr`.
2. Polls `SchedulerGrpcClient::connect()` until the scheduler is reachable.
3. Calls `ballista_executor::new_standalone_executor()` ([ballista/executor/src/standalone.rs](../ballista/executor/src/standalone.rs)) which spins up an in-process executor connected to that scheduler, with `concurrent_tasks` set from `ballista_standalone_parallelism`.
4. Builds a `SessionState` whose query planner points at `http://localhost:<port>` and returns the `SessionContext`.

The implication: standalone mode is *the same code paths* as distributed mode. The plan still gets protobuf-encoded and shipped over gRPC; it just happens to be `localhost`. This means standalone mode is a faithful integration test for distributed behavior, not a special fast-path. It also means standalone mode is slower than raw DataFusion for trivial queries because of the serialization overhead.

Use standalone for: dev loop, integration tests, small single-machine workloads, anything where you want a DataFusion-shaped API but ever expect to scale out.

### Distributed (separate processes)

Each role runs as its own process or container. The typical layouts are:

- **Bare metal / VMs:** run the `ballista-scheduler` binary on one host, `ballista-executor` on N hosts. Clients connect by URL.
- **Docker Compose:** [docker-compose.yml](../docker-compose.yml) at the repo root spins up one scheduler container and two executor containers. Read this file when planning your own compose setup; it has the minimal correct flag and dependency wiring.
- **Kubernetes:** scheduler as a `Deployment` or `StatefulSet`, executors as a `Deployment` with N replicas, optionally autoscaled via KEDA ([ballista/scheduler/proto/keda.proto](../ballista/scheduler/proto/keda.proto), `keda-scaler` feature). See [docs/source/user-guide/deployment/kubernetes.md](../docs/source/user-guide/deployment/kubernetes.md).

Use distributed for: anything that has to scale past one machine, anything in production, anything where the lifetimes of the cluster and the client are different.

File 9 goes deep on deployment.

---

## 2.5 End-to-end query lifecycle

This is the part you will reread the most. Eight steps, from `ctx.sql(...)` to results.

1. **Logical planning (client).** DataFusion parses your SQL, resolves table references against the registered catalogs, and produces an optimized `LogicalPlan`. This is identical to single-node DataFusion. Nothing distributed has happened yet.

2. **Physical planning interception (client).** DataFusion would normally hand the logical plan to a `PhysicalPlanner`. Because the `SessionState` has `BallistaQueryPlanner` installed as its `QueryPlanner`, control passes to [ballista/core/src/planner.rs](../ballista/core/src/planner.rs) instead. `BallistaQueryPlanner` wraps the whole logical plan in a `DistributedQueryExec` ([ballista/core/src/execution_plans/distributed_query.rs](../ballista/core/src/execution_plans/distributed_query.rs)). This is a placeholder `ExecutionPlan` that, when polled, will submit the *logical* plan to the scheduler and stream back results.

3. **Job submission (client to scheduler).** When the user calls `df.show()` or otherwise pulls on the stream, `DistributedQueryExec::execute` opens a gRPC connection to the scheduler and calls `ExecuteQuery` ([ballista/core/proto/ballista.proto:841](../ballista/core/proto/ballista.proto#L841)) with the serialized logical plan and the session config. The scheduler returns a `job_id`.

4. **Distributed physical planning (scheduler).** The scheduler picks up the logical plan, runs DataFusion's physical planner against it, then runs the **distributed** physical optimizer ([ballista/scheduler/src/physical_optimizer/](../ballista/scheduler/src/physical_optimizer/)) on top. The distributed optimizer's job is to insert `ShuffleWriterExec` / `UnresolvedShuffleExec` nodes at points where data has to be redistributed, breaking the tree into **stages**. The result is an `ExecutionGraph` ([ballista/scheduler/src/state/execution_graph.rs](../ballista/scheduler/src/state/execution_graph.rs)) of stages with explicit dependencies between them. Each stage starts in the `UnResolved` state ([ballista/scheduler/src/state/execution_stage.rs:62](../ballista/scheduler/src/state/execution_stage.rs#L62)).

5. **Stage scheduling (scheduler).** A stage with no incomplete inputs transitions from `UnResolved` to `Resolved`. The `TaskManager` ([ballista/scheduler/src/state/task_manager.rs](../ballista/scheduler/src/state/task_manager.rs)) picks an executor for each partition in the stage according to the configured `TaskDistributionPolicy` ([ballista/scheduler/src/config.rs:462](../ballista/scheduler/src/config.rs#L462)) — `Bias` (pack greedily), `RoundRobin` (spread evenly), or a custom `DistributionPolicy`. If the scheduling policy is `PushStaged`, the scheduler calls `LaunchTask` ([ballista/core/proto/ballista.proto:856](../ballista/core/proto/ballista.proto#L856)) on the chosen executor; if `PullStaged`, the executors pick up the task via their `PollWork` loop on the next poll.

6. **Task execution (executor).** The executor receives a `TaskDefinition`, decodes the physical plan with the configured `PhysicalExtensionCodec`, executes it against its assigned partitions, and writes the output. For non-final stages, the output is a set of Arrow IPC shuffle files in the executor's `--work-dir`, indexed by output partition. The executor reports `RUNNING`, then `SUCCESSFUL` (or `FAILED`) via `UpdateTaskStatus` ([ballista/core/proto/ballista.proto:833](../ballista/core/proto/ballista.proto#L833)). The status message includes `PartitionLocation` records that tell the scheduler where each output partition now lives.

7. **Stage transitions (scheduler).** As tasks succeed, the scheduler updates the stage. When all tasks in a stage succeed, the stage transitions to `Successful`. Downstream stages that were waiting on it can now be `Resolved`: the scheduler walks the plan, replaces every `UnresolvedShuffleExec` in their plans with a real `ShuffleReaderExec` populated with the `PartitionLocation`s from upstream, and the cycle repeats. This continues until the final stage completes.

8. **Result delivery (scheduler to client).** The client has been polling `GetJobStatus` ([ballista/core/proto/ballista.proto:843](../ballista/core/proto/ballista.proto#L843)) (or holding open the `ExecuteQueryPush` stream). Once the final stage is `Successful`, the scheduler returns the locations of the final partitions. The `DistributedQueryExec` on the client fetches them via Arrow Flight from the executors and yields `RecordBatch`es into the user's stream. From the user's perspective, `df.show()` just produced rows.

If anything in steps 4 through 7 fails, the scheduler can retry the failed task up to `--task-max-failures` (default 4) and the whole stage up to `--stage-max-failures` (default 4) before failing the job and returning an error to the client.

File 5 walks through this with a concrete `GROUP BY` query and shows the exact stage graph it produces.

---

## 2.6 Where cluster state lives, and what restart means

This is the operational gotcha that affects every architectural decision downstream.

Cluster state is held in the `BallistaCluster` trait ([ballista/scheduler/src/cluster/mod.rs](../ballista/scheduler/src/cluster/mod.rs)). The only implementation shipped is `InMemoryCluster` ([ballista/scheduler/src/cluster/memory.rs](../ballista/scheduler/src/cluster/memory.rs)). What this means:

- **Executor registry: in-memory.** If the scheduler restarts, the executor list is empty until executors re-register on their next heartbeat (default 60s) or you restart them.
- **Job state: in-memory.** Running jobs are lost on scheduler restart. The client will see its gRPC stream close. There is no resumption.
- **Job history: in-memory and capped.** The scheduler trims finished job state after `--finished-job-state-clean-up-interval-seconds` (default 3600s). For long-term audit you need to ship events to an external store yourself.
- **Sessions: in-memory.** Catalog registrations done through `CreateUpdateSession` ([ballista/core/proto/ballista.proto:835](../ballista/core/proto/ballista.proto#L835)) are lost on restart.

There is no etcd, no sled, no Postgres backend in the codebase today. The cluster trait is designed to allow one, but you would need to implement it yourself. The roadmap calls this out under "Make production ready / Better error handling / Scheduler restart" ([ROADMAP.md](../ROADMAP.md)).

For Eur Data this is one of the first decisions you have to confront. File 10 returns to it.

---

## 2.7 Failure modes worth knowing now

You will hit these. Better to know what they look like ahead of time.

- **Executor disappears mid-job.** Heartbeat misses past `--executor-timeout-seconds` cause the scheduler to mark the executor dead. Tasks it owned get rescheduled on surviving executors. Stages may transition back to `UnResolved` if their inputs are lost. If `--task-max-failures` or `--stage-max-failures` is exceeded, the job fails.
- **Scheduler restarts mid-job.** The client's stream dies. The executor heartbeats reconnect to the new scheduler instance. The job is gone.
- **Shuffle file disappears.** A `ResultLost` ([ballista/core/proto/ballista.proto:494](../ballista/core/proto/ballista.proto#L494)) is reported when a downstream stage cannot fetch a partition. The scheduler may rerun the upstream stage to regenerate it.
- **Network partition between executors.** Shuffle reads fail; downstream stage tasks report `FetchPartitionError`. Retry logic kicks in.
- **gRPC message size exceeded.** Default 16 MiB on both ends ([ballista/scheduler/src/config.rs:149](../ballista/scheduler/src/config.rs#L149) and the matching executor flag). Large logical plans (think wide SELECT lists, many UDFs) can blow this. Increase both ends.
- **Bind vs external host mismatch.** Symptom: scheduler accepts the job, executor accepts the task, then shuffle reads stall because the consuming executor cannot resolve the producer's advertised address. Fix the `--external-host` flag.

---

## 2.8 Where to next

You now have the map. The next files zoom into each piece.

- Next: [3-the_scheduler.md](3-the_scheduler.md) — what is *inside* the scheduler process.
- Then: [4-the_executor.md](4-the_executor.md) — what is inside an executor.
- Then: [5-planning_stages_and_shuffles.md](5-planning_stages_and_shuffles.md) — the planning pipeline and shuffle semantics that this lifecycle glosses over.
