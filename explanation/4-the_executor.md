# 4. The executor

**What you will know after reading this:** the modules inside an executor process, how it receives and runs a task, how shuffle writes and reads work in practice, the executor's concurrency and memory model, and where it can be customized.

This file mirrors file 3 but for the data plane. Read them as a pair.

---

## 4.1 The module map

[ballista/executor/src/](../ballista/executor/src/):

```
bin/main.rs                # Binary entry point
lib.rs                     # Module declarations and standalone helpers
config.rs                  # CLI flags and ExecutorProcessConfig conversion
executor.rs                # Executor struct: the actual task runner
executor_server.rs         # gRPC service exposed to the scheduler (push mode)
executor_process.rs        # Lifecycle: boot, registration, shutdown
execution_loop.rs          # Pull-mode loop: poll scheduler for work
execution_engine.rs        # Pluggable engine trait (for custom runtimes)
flight_service.rs          # Arrow Flight server for shuffle reads
collect.rs                 # Final-result collection plan node
cpu_bound_executor.rs      # Thread pool for compute-heavy work
client_pool.rs             # Connection pool to peer executors
metrics/                   # Metrics collection and policy
shutdown.rs / terminate.rs # Graceful shutdown signaling
standalone.rs              # In-process executor used by SessionContext::standalone()
```

The three you spend the most time in: `executor.rs` (runs tasks), `executor_server.rs` (receives tasks from scheduler), `flight_service.rs` (serves shuffle outputs to peers).

---

## 4.2 Top-level types

### `Executor`

Defined in [ballista/executor/src/executor.rs](../ballista/executor/src/executor.rs). The runtime object. It holds:

- The executor's `metadata` (ID, host, port, slot count).
- A `RuntimeEnv` built from the configured runtime producer (object stores, function registry, etc.).
- The work directory path.
- A `CpuBoundExecutor` ([ballista/executor/src/cpu_bound_executor.rs](../ballista/executor/src/cpu_bound_executor.rs)) for blocking compute.
- A `client_pool` ([ballista/executor/src/client_pool.rs](../ballista/executor/src/client_pool.rs)) for connections to peer executors (shuffle reads).
- The configured `ExecutionEngine` ([ballista/executor/src/execution_engine.rs](../ballista/executor/src/execution_engine.rs)).

This is what `executor_server.rs` and `execution_loop.rs` both delegate to. Whether a task arrives via push or pull, the same `Executor::execute_query_stage` (or similar) actually runs it.

### `ExecutorServer`

Defined in [ballista/executor/src/executor_server.rs](../ballista/executor/src/executor_server.rs). The gRPC service implementation. It exposes:

- `LaunchTask` ([ballista/core/proto/ballista.proto:856](../ballista/core/proto/ballista.proto#L856)) — receive a single task from the scheduler (push mode).
- `LaunchMultiTask` ([ballista/core/proto/ballista.proto:858](../ballista/core/proto/ballista.proto#L858)) — receive a batch of tasks at once.
- `StopExecutor` — graceful shutdown trigger.
- `CancelTasks` — cancel one or more running tasks.
- `RemoveJobData` — clean up a finished job's shuffle files.

When `LaunchTask` arrives, the server deserializes the `TaskDefinition`, hands it to the `Executor`, and returns immediately. The actual work runs in the background; status is reported back to the scheduler via `UpdateTaskStatus` on the scheduler's gRPC.

### `ExecutionLoop`

Defined in [ballista/executor/src/execution_loop.rs](../ballista/executor/src/execution_loop.rs). The pull-mode counterpart. Runs in a background tokio task. Loop body:

1. Compute available slots (total minus running).
2. If zero, sleep briefly and retry.
3. Call `PollWork` on the scheduler with available slot count.
4. For each `TaskDefinition` returned, dispatch it to the `Executor` and start tracking its status.
5. Heartbeat (in some setups; in others the heartbeat is a separate task).

Whether push or pull is in use is determined at registration time by the `--task-scheduling-policy` flag. The two modes are mutually exclusive: an executor either runs the pull loop or accepts pushes, not both.

### `ExecutorProcess`

Defined in [ballista/executor/src/executor_process.rs](../ballista/executor/src/executor_process.rs). Wraps everything together: parses config, builds the `Executor`, starts the gRPC server, starts Flight, starts (if pull) the execution loop, starts the heartbeat task, installs signal handlers, waits for shutdown.

This is the moral equivalent of `main.rs` for an executor — `bin/main.rs` is a thin wrapper around it.

---

## 4.3 Inside `Executor::execute_query_stage`

The path a task takes once it has been received, regardless of how it arrived:

1. **Decode the physical plan.** The `TaskDefinition` carries the plan as a protobuf blob. The configured `PhysicalExtensionCodec` ([ballista/core/src/serde/](../ballista/core/src/serde/)) is used to deserialize into a DataFusion `Arc<dyn ExecutionPlan>`. If the plan contains an `UnresolvedShuffleExec`, something is wrong upstream — the scheduler should have resolved it before dispatching.
2. **Reconstruct the session config.** The task includes serialized config key-value pairs. The configured `ConfigProducer` rebuilds a `SessionConfig`, and the `RuntimeProducer` builds a `RuntimeEnv` from it (this is where S3 credentials, custom object stores, function registries are applied — file 7 covers exactly how).
3. **Wrap with a partition range.** Each task is responsible for a specific subset of the plan's output partitions. The executor wraps execution so it only produces those partitions.
4. **Run.** The plan is executed via DataFusion's normal `execute()` machinery. The resulting `RecordBatch` stream is consumed by either the shuffle writer (for intermediate stages) or sent to the result collector (for the final stage).
5. **Write shuffle output.** If the root of the plan is a `ShuffleWriterExec`, each output `RecordBatch` is hashed (or sorted) on the partitioning key, split into the configured number of output partitions, and written to disk under `--work-dir/<job_id>/<stage_id>/<partition_id>/`. One Arrow IPC file per output partition.
6. **Report status.** When the task finishes, the executor calls `UpdateTaskStatus` on the scheduler, including a list of `PartitionLocation`s for each output partition (executor ID, host, port, file path). For the final stage, the locations point at the final result partitions which the client will fetch via Flight.

If the task fails at any point, the failure reason is captured in a `FailedTask` message ([ballista/core/proto/ballista.proto:456](../ballista/core/proto/ballista.proto#L456)) and reported to the scheduler. The scheduler decides whether to retry.

---

## 4.4 Concurrency and memory

Three flags control how an executor uses its host:

- **`--concurrent-tasks`** ([ballista/executor/src/config.rs:85](../ballista/executor/src/config.rs#L85)) — maximum number of tasks running in parallel. Default 0, which means "use all CPU cores." Each running task consumes one slot in the executor's slot count, which is what the scheduler sees when deciding where to place work.
- **`--memory-pool-size`** ([ballista/executor/src/config.rs:167](../ballista/executor/src/config.rs#L167)) — optional total memory budget. Accepts human-readable sizes like `8GB`, `512MiB`. When set, each concurrent task gets a `FairSpillPool` of size `memory_pool_size / concurrent_tasks`. When unset, DataFusion's default unbounded memory tracking is used and your executor can OOM under pressure.
- **`--work-dir`** ([ballista/executor/src/config.rs:82](../ballista/executor/src/config.rs#L82)) — where shuffle files land. If unset, a tempdir is used. For production set it explicitly to a fast local disk (NVMe ideal); shuffle I/O is on the critical path.

Internally, the executor runs CPU-bound work on a dedicated thread pool (`CpuBoundExecutor`) separate from the tokio runtime. This keeps long compute from starving the gRPC server and heartbeat tasks. If you write a custom `ExecutionEngine` that does heavy CPU work, you should follow the same pattern — do not block the tokio runtime.

---

## 4.5 Shuffle writes

The shuffle writer is the bridge between "data was produced by an executor" and "data can be consumed by another executor." It lives at [ballista/core/src/execution_plans/shuffle_writer.rs](../ballista/core/src/execution_plans/shuffle_writer.rs).

Two variants:

- **Hash shuffle (`ShuffleWriterExec`).** The default. The writer takes each input `RecordBatch`, applies the partitioning expression (typically a hash of one or more columns) to bucket each row into one of N output partitions, and appends to N open IPC writers. When the input stream completes, all writers are closed and N files exist on disk. Each file is an Arrow IPC stream containing only the rows that belong to that output partition.

- **Sort shuffle (`SortShuffleWriterExec`).** Defined in [ballista/core/src/execution_plans/sort_shuffle/](../ballista/core/src/execution_plans/sort_shuffle/). Used when the downstream operator needs sorted input (notably some join strategies). The writer sorts input within each output partition before writing, producing a single sorted file per output partition plus an index file. This trades write-side CPU for read-side simplicity.

Both share the trait at [ballista/core/src/execution_plans/shuffle_writer_trait.rs](../ballista/core/src/execution_plans/shuffle_writer_trait.rs).

The output of a shuffle write task is a vector of `ShuffleWritePartition` records ([ballista/core/proto/ballista.proto:500](../ballista/core/proto/ballista.proto#L500)): per output partition, the row count, byte count, file path, and any error. These flow back to the scheduler as part of the `SuccessfulTask` payload, become `PartitionLocation`s in the cluster state, and are eventually wired into downstream `ShuffleReaderExec`s.

A common operational mistake: leaving shuffle files indefinitely. The `--job-data-clean-up-interval-seconds` and `--job-data-ttl-seconds` flags ([ballista/executor/src/config.rs](../ballista/executor/src/config.rs)) control on-executor cleanup. Combined with the scheduler's `--finished-job-data-clean-up-interval-seconds`, finished jobs eventually get their shuffle files deleted. But the roadmap explicitly flags this as incomplete (see [ROADMAP.md](../ROADMAP.md)) — for long-running production clusters, monitor disk usage on executors.

---

## 4.6 Shuffle reads (Arrow Flight)

When a downstream stage's task starts, its plan contains a `ShuffleReaderExec` ([ballista/core/src/execution_plans/shuffle_reader.rs](../ballista/core/src/execution_plans/shuffle_reader.rs)) populated with `PartitionLocation`s. To produce its assigned output partition, the reader must fetch the corresponding input partition from every upstream task. Those input partitions live as files on the upstream executors — which may or may not be this executor.

Two cases:

1. **Local read.** If the upstream partition is on the same executor (and `BALLISTA_SHUFFLE_READER_FORCE_REMOTE_READ` is not set), the reader just opens the file directly. Faster, no network.
2. **Remote read.** Otherwise, the reader connects to the upstream executor's **Arrow Flight** service on port `50051` ([ballista/executor/src/flight_service.rs](../ballista/executor/src/flight_service.rs)) and issues a `DoGet` with a `FetchPartition` action ([ballista/core/proto/ballista.proto:249](../ballista/core/proto/ballista.proto#L249)) describing the file. The Flight server streams the Arrow IPC file's `RecordBatch`es back. The reader concatenates them into its input stream.

A few interesting points:

- **Concurrency is bounded.** `BALLISTA_SHUFFLE_READER_MAX_REQUESTS` controls how many concurrent Flight fetches a single reader will issue. Without this, a wide shuffle (many input partitions) could swamp the network.
- **Connection pooling.** The executor maintains a `client_pool` ([ballista/executor/src/client_pool.rs](../ballista/executor/src/client_pool.rs)) so repeat fetches to the same peer reuse TCP connections.
- **Optional flight proxy.** The scheduler can advertise an Arrow Flight SQL endpoint (`--advertise-flight-sql-endpoint` in [ballista/scheduler/src/config.rs:49](../ballista/scheduler/src/config.rs#L49)) for clients that want to fetch results through a proxy rather than directly from executors. Useful if executors are not reachable from the client network.

Arrow Flight is what makes Ballista's data plane fast. It is essentially gRPC streams of Arrow IPC bytes, with zero-copy on the deserialization side. Memory copies are avoided where possible; the reader hands `RecordBatch`es into the downstream plan as they arrive.

---

## 4.7 Heartbeats and registration

Once registered, the executor periodically calls `HeartBeatFromExecutor` ([ballista/core/proto/ballista.proto:831](../ballista/core/proto/ballista.proto#L831)) every `--executor-heartbeat-interval-seconds` (default 60s). The heartbeat carries:

- The executor's status (`ACTIVE`, `TERMINATING`, etc.).
- Current load metrics (running tasks, available slots, optionally CPU and memory).

The scheduler updates the executor's last-seen timestamp. If two or three heartbeats are missed in a row (the doc string on the timeout flag is explicit about this) the scheduler marks the executor dead.

When the executor receives a `StopExecutor` request or a SIGTERM, it enters `TERMINATING` status and reports it on the next heartbeat. The scheduler stops giving it new work and gives it `--executor-termination-grace-period` seconds (default 30) to drain running tasks before treating it as dead. This is what makes rolling executor updates possible without losing in-flight work.

---

## 4.8 The pluggable `ExecutionEngine`

[ballista/executor/src/execution_engine.rs](../ballista/executor/src/execution_engine.rs) defines a trait that abstracts the per-task execution mechanism. The default engine uses DataFusion's standard physical execution. You can swap in your own to:

- Run on GPUs.
- Add tracing or auditing per task.
- Bridge to another runtime (a custom vectorized engine, a Wasm host, etc.).
- Inject security policies between task receipt and execution.

The override is configured via `ExecutorProcessConfig::override_execution_engine` ([ballista/executor/src/config.rs:210](../ballista/executor/src/config.rs#L210)). File 7 walks through how this fits the broader extension story.

This is one of the most powerful hooks for Eur Data. If you ever want to add tenant-level resource accounting or to inject your own optimization pass between scheduler and executor, this is one obvious place to do it.

---

## 4.9 Standalone executor

For completeness: [ballista/executor/src/standalone.rs](../ballista/executor/src/standalone.rs) provides `new_standalone_executor` and `new_standalone_executor_from_state`, which spin up an in-process executor wired to a local scheduler. This is what `SessionContext::standalone()` calls. The implementation is short and worth reading once; it shows you the smallest end-to-end glue needed to build a fully functional Ballista cluster in code.

---

## 4.10 Metrics and observability

[ballista/executor/src/metrics/](../ballista/executor/src/metrics/) defines an `ExecutorMetricCollectionPolicy` controlled by the `-m / --metrics` flag. Metrics include per-task counters, memory pool usage, CPU time, and operator-level metrics that are forwarded to the scheduler.

For the scheduler-side aggregation: per-job metrics flow into the scheduler with task status updates and are queryable via `GetJobMetrics`. That is what the TUI uses to render per-operator stats.

If you have Prometheus on the scheduler (`prometheus-metrics` feature), make sure executor logs are also being scraped (or shipped to a central collector). A lot of operational diagnosis happens by correlating scheduler timeline with executor logs.

---

## 4.11 What an executor does *not* do

- **No catalog of its own.** The executor does not know about tables; it just executes the plan it was given. Catalog resolution happens on the client and scheduler.
- **No optimizer.** The plan arrives fully optimized. The executor is purely a runtime.
- **No persistent state.** Shuffle files on disk are the only state, and they are tied to job IDs that the scheduler owns.
- **No cross-tenant isolation.** Tasks share the executor's memory pool and CPU pool. If you need hard isolation between tenants, you run separate executor processes per tenant.

---

## 4.12 Where to next

You now know both the control plane (file 3) and the data plane (this file). Next is the part that ties them together: how Ballista decides where stage boundaries go and what a shuffle actually represents at the plan level.

- Next: [5-planning_stages_and_shuffles.md](5-planning_stages_and_shuffles.md).
