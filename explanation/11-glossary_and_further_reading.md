# 11. Glossary and further reading

**What you will know after reading this:** definitions of every Ballista-specific term used in this folder, and pointers to the upstream specs and in-repo official documentation for going deeper on each topic.

Keep this file open while reading the others.

---

## 11.1 Glossary

### AQE (Adaptive Query Execution)

Re-optimization of a query plan at runtime, using statistics from already-completed upstream stages to make better decisions about downstream stages. Borrowed from Spark. In Ballista, lives in [ballista/scheduler/src/state/aqe/](../ballista/scheduler/src/state/aqe/). Active area of development; partial today.

### Arrow Flight

A gRPC-based protocol for moving Arrow data efficiently between processes. Wire format is Arrow IPC, which is the same as Arrow's in-memory layout, so deserialization is near-zero-cost. Ballista uses it for executor-to-executor shuffle reads and for client result fetches. Server implementation: [ballista/executor/src/flight_service.rs](../ballista/executor/src/flight_service.rs).

### Arrow IPC

The on-disk and over-the-wire serialization format for Arrow `RecordBatch`es. Used inside Arrow Flight streams and as the format for shuffle files on disk.

### Ballista cluster

The combination of one scheduler process and N executor processes, sharing a `BallistaCluster` state backend. Today, one cluster has one scheduler; multi-scheduler is on the roadmap.

### `BallistaCluster`

The trait abstracting the cluster's state backend ([ballista/scheduler/src/cluster/mod.rs](../ballista/scheduler/src/cluster/mod.rs)). Only implementation today is `InMemoryCluster`. Designed to allow distributed backends (etcd, Postgres) but none are shipped.

### `BallistaCodec`

The wrapper around `LogicalExtensionCodec` and `PhysicalExtensionCodec` that Ballista uses to serialize plans over the wire ([ballista/core/src/serde/mod.rs](../ballista/core/src/serde/mod.rs)). Generic over plan types `T: AsLogicalPlan` and `U: AsExecutionPlan`.

### `BallistaQueryPlanner`

The client-side hook that intercepts DataFusion physical planning and wraps the logical plan in a `DistributedQueryExec` ([ballista/core/src/planner.rs:41](../ballista/core/src/planner.rs#L41)). Installed automatically when you call `SessionContext::standalone()` or `SessionContext::remote(...)`.

### Bias (task distribution)

A `TaskDistributionPolicy` that packs tasks tightly onto the first available executor. Default. Trades worse load balance for better cache locality.

### Client

The application using Ballista. Holds a `SessionContext` whose query planner is `BallistaQueryPlanner`. Talks to the scheduler over gRPC; talks to executors over Arrow Flight for result fetches.

### Codec (logical / physical)

A pair of traits (`LogicalExtensionCodec`, `PhysicalExtensionCodec` from DataFusion's `datafusion_proto` crate) that serialize and deserialize plan nodes. Required for any custom plan node that crosses a process boundary in Ballista.

### Config producer

`Arc<dyn Fn() -> SessionConfig + Send + Sync>`. The function called on an executor (and on the scheduler) to build a fresh `SessionConfig` with your defaults. Allows shipping `KeyValuePair` deltas over the wire while keeping defaults local.

### `DistributedQueryExec`

The client-side `ExecutionPlan` node that wraps an entire logical plan, submits it to the scheduler, and streams results back. Constructed by `BallistaQueryPlanner`. Source: [ballista/core/src/execution_plans/distributed_query.rs](../ballista/core/src/execution_plans/distributed_query.rs).

### `DistributedExplainAnalyzeExec`

The distributed-aware version of `EXPLAIN ANALYZE`. Runs the query and collects per-stage and per-operator metrics from all participating executors. Source: [ballista/core/src/execution_plans/distributed_explain_analyze.rs](../ballista/core/src/execution_plans/distributed_explain_analyze.rs).

### `EndpointOverrideFn`

`Arc<dyn Fn(tonic::transport::Endpoint) -> Result<Endpoint, _>>`. Runs against every gRPC client endpoint before connection. Hook for TLS, timeouts, custom HTTP/2 settings. Source: [ballista/core/src/extension.rs:55](../ballista/core/src/extension.rs#L55).

### `ExecutionEngine`

A trait abstracting how an executor runs a task ([ballista/executor/src/execution_engine.rs](../ballista/executor/src/execution_engine.rs)). Default uses DataFusion's standard physical execution. Override to add resource accounting, GPU routing, custom runtimes.

### `ExecutionGraph`

The scheduler's representation of a submitted query: a DAG of `ExecutionStage`s with dependencies. Source: [ballista/scheduler/src/state/execution_graph.rs](../ballista/scheduler/src/state/execution_graph.rs).

### `ExecutionStage` / Stage

A contiguous subtree of the physical plan that can be executed without crossing a shuffle boundary. The unit of scheduling. Has a state machine: `UnResolved → Resolved → Running → Successful / Failed`. Source: [ballista/scheduler/src/state/execution_stage.rs](../ballista/scheduler/src/state/execution_stage.rs).

### Executor

A long-lived process that runs tasks for the scheduler. Registers on startup, heartbeats periodically, hosts a gRPC service (for task control) and an Arrow Flight service (for shuffle reads). Source: [ballista/executor/](../ballista/executor/).

### Executor slot

A unit of concurrent task capacity on an executor. The total slot count for an executor is `--concurrent-tasks`. The scheduler tracks available slots per executor and uses them for task placement decisions.

### Flight

See **Arrow Flight**.

### Flight proxy

An optional scheduler-side Arrow Flight server that fronts the executors' Flight services. Used when clients cannot reach executors directly. Enabled with `--advertise-flight-sql-endpoint`. Source: [ballista/scheduler/src/flight_proxy_service.rs](../ballista/scheduler/src/flight_proxy_service.rs).

### Function registry

DataFusion's `FunctionRegistry` holds scalar UDFs, aggregate UDAFs, and window UDFs. Ballista wraps it with defaults at [ballista/core/src/extension.rs:66](../ballista/core/src/extension.rs#L66). Override per-executor via `ExecutorProcessConfig::override_function_registry`.

### Job

A submitted query from the client's perspective. Has a `job_id` returned by `ExecuteQuery`. Internally becomes one `ExecutionGraph`.

### `KeyValuePair`

The atomic unit of session config transport. A list of these flows in every `TaskDefinition`. Source: [ballista/core/proto/ballista.proto:222](../ballista/core/proto/ballista.proto#L222).

### `LogicalExtensionCodec`

DataFusion trait for serializing custom logical plan nodes. Ballista's default implementation: `BallistaLogicalExtensionCodec` at [ballista/core/src/serde/mod.rs](../ballista/core/src/serde/mod.rs).

### Namespace

A scheduler config option (`--namespace`) intended to identify the cluster within a shared state backend. Only meaningful for backends that support cohabitation, which the default in-memory backend does not.

### Partition

A horizontal slice of a `RecordBatch` stream. Operators expose a partition count and produce a `SendableRecordBatchStream` per partition. Shuffles redistribute data across partitions according to a partitioning expression.

### `PartitionLocation`

The "address" of a shuffle output partition: which executor, which file, the partition statistics. Reported by tasks in their `SuccessfulTask` payload. Used by the scheduler to wire `ShuffleReaderExec`s in downstream stages. Source: [ballista/core/proto/ballista.proto:263](../ballista/core/proto/ballista.proto#L263).

### `PhysicalExtensionCodec`

DataFusion trait for serializing custom physical plan nodes. Ballista's default: `BallistaPhysicalExtensionCodec`, which knows about the four shuffle nodes.

### Pull-staged scheduling

A `TaskSchedulingPolicy` in which executors poll the scheduler for work via `PollWork`. Default in many configurations. Executor implementation: [ballista/executor/src/execution_loop.rs](../ballista/executor/src/execution_loop.rs).

### Push-staged scheduling

A `TaskSchedulingPolicy` in which the scheduler pushes tasks to executors via `LaunchTask`. Lower latency, requires scheduler-to-executor reachability. Executor handler: [ballista/executor/src/executor_server.rs](../ballista/executor/src/executor_server.rs).

### REST API

An optional HTTP interface exposed by the scheduler when compiled with `--features rest-api`. Routes in [ballista/scheduler/src/api/](../ballista/scheduler/src/api/). Used by the TUI and by external tooling.

### Round-robin (task distribution)

A `TaskDistributionPolicy` that spreads tasks evenly across executors, one per executor at a time. Better load balance than `Bias`, worse cache locality.

### Runtime producer

`Arc<dyn Fn(&SessionConfig) -> Result<Arc<RuntimeEnv>>>`. The function called on an executor to build a `RuntimeEnv` for a task. Where you register custom `ObjectStore`s, memory pools, disk managers.

### Scheduler

The single (today) coordination process for a Ballista cluster. Plans, schedules, tracks, returns results. Source: [ballista/scheduler/](../ballista/scheduler/).

### Session

A logical conversation with the scheduler. Has a `session_id` and an associated `SessionState`. Tables registered against a session persist across multiple queries within it. Managed by `SessionManager` ([ballista/scheduler/src/state/session_manager.rs](../ballista/scheduler/src/state/session_manager.rs)).

### Session builder

`Arc<dyn Fn(SessionConfig) -> Result<SessionState> + Send + Sync>`. The scheduler-side function that builds a `SessionState` for each session. Hook for per-tenant catalogs, custom statistics providers, session-scoped UDFs. Override via `SchedulerConfig::with_override_session_builder`.

### `SessionConfigExt`

DataFusion `SessionConfig` extension trait that adds Ballista-specific methods (`new_with_ballista`, `with_ballista_job_name`, `set_logical_codec`, etc.). Source: [ballista/core/src/extension.rs:119](../ballista/core/src/extension.rs#L119).

### `SessionStateExt`

DataFusion `SessionState` extension trait that adds Ballista setup methods (`new_ballista_state`, `upgrade_for_ballista`). Source: [ballista/core/src/extension.rs:101](../ballista/core/src/extension.rs#L101).

### Shuffle

The redistribution of data between stages, typically by hashing or sorting on a partitioning key. The mechanism by which a `GROUP BY`, a hash join, or a window function gets all rows with the same key onto the same executor.

### Shuffle file

An Arrow IPC file written by `ShuffleWriterExec` to an executor's `--work-dir`. One file per output partition per task.

### `ShuffleReaderExec`

Physical plan node that reads shuffle output partitions from upstream executors (via Arrow Flight or local file). Resolved from `UnresolvedShuffleExec` by the scheduler before stage dispatch. Source: [ballista/core/src/execution_plans/shuffle_reader.rs](../ballista/core/src/execution_plans/shuffle_reader.rs).

### `ShuffleWriterExec`

Physical plan node that writes its input to disk, partitioned by a partitioning expression. The "freeze point" at a stage boundary. Source: [ballista/core/src/execution_plans/shuffle_writer.rs](../ballista/core/src/execution_plans/shuffle_writer.rs). Sort-based variant: `SortShuffleWriterExec` in [sort_shuffle/](../ballista/core/src/execution_plans/sort_shuffle/).

### `SortShuffleWriterExec`

A sorted variant of `ShuffleWriterExec`. Used when downstream operators (some join strategies) need sorted input. Trades CPU at write time for read-side simplicity.

### Standalone mode

A deployment mode in which scheduler and executor run in the client process. Activated by `SessionContext::standalone()`. Useful for development, tests, and small workloads.

### Stage

See `ExecutionStage`.

### Stage state machine

The lifecycle a stage moves through: `UnResolved → Resolved → Running → Successful / Failed`. Backed by the enum at [ballista/scheduler/src/state/execution_stage.rs:62](../ballista/scheduler/src/state/execution_stage.rs#L62).

### Substrait

A cross-language standard for representing query plans. Optional alternative to DataFusion's native protobuf plan format. Controlled by the `substrait` feature on the scheduler. Example: [examples/examples/standalone-substrait.rs](../examples/examples/standalone-substrait.rs).

### Task

The smallest unit of distributed work: one stage's plan executed against one output partition. A stage with N output partitions is dispatched as N tasks.

### `TableProvider`

DataFusion trait for tables (CSV, Parquet, JSON, Iceberg, custom). Registered with `SessionContext::register_table`. In Ballista, the resulting scan plan must be serializable through the configured codec.

### Task slot

See **Executor slot**.

### `TaskDefinition`

The protobuf message sent from scheduler to executor describing a single task. Contains the physical plan, partition IDs, session config delta, task ID.

### `TaskDistributionPolicy`

The scheduler's strategy for choosing executors when dispatching tasks: `Bias`, `RoundRobin`, or `Custom(Arc<dyn DistributionPolicy>)`. Source: [ballista/scheduler/src/config.rs:462](../ballista/scheduler/src/config.rs#L462).

### `TaskSchedulingPolicy`

Pull-staged or push-staged. Determines whether executors poll for work or the scheduler pushes. Must match on both sides of the cluster.

### TUI

Terminal user interface for inspecting a cluster, shipped with `ballista-cli`. Source: [ballista-cli/src/tui/](../ballista-cli/src/tui/). Talks to the scheduler's REST API.

### `UnresolvedShuffleExec`

Placeholder physical plan node used during distributed planning. Represents "read from the output of stage N" before the scheduler knows where stage N's output will land. Replaced with a `ShuffleReaderExec` once the upstream stage completes. Source: [ballista/core/src/execution_plans/unresolved_shuffle.rs](../ballista/core/src/execution_plans/unresolved_shuffle.rs).

### Work directory

The local filesystem path on each executor where shuffle files are written. Set with `--work-dir`. Should be a fast local disk; cleanup is controlled by `--job-data-ttl-seconds` and related flags.

---

## 11.2 In-repo official documentation

Read these for the canonical source-of-truth view that complements the editorial framing in this folder.

### Architecture

- [docs/source/contributors-guide/architecture.md](../docs/source/contributors-guide/architecture.md) — official architecture overview. Has the design-principles framing (Arrow-native, language-agnostic, extensible).
- [docs/source/contributors-guide/ballista_architecture.excalidraw.svg](../docs/source/contributors-guide/ballista_architecture.excalidraw.svg) — the canonical diagram. Open it; the box-and-arrow picture is worth several pages of prose.
- [docs/source/contributors-guide/code-organization.md](../docs/source/contributors-guide/code-organization.md) — crate layout.
- [docs/source/contributors-guide/development.md](../docs/source/contributors-guide/development.md) — contributor setup.
- [docs/developer/architecture.md](../docs/developer/architecture.md) — older developer-doc architecture notes; check for any details not in the user-facing version.

### User guide

- [docs/source/user-guide/introduction.md](../docs/source/user-guide/introduction.md) — short intro.
- [docs/source/user-guide/scheduler.md](../docs/source/user-guide/scheduler.md) — scheduler concepts.
- [docs/source/user-guide/cli.md](../docs/source/user-guide/cli.md) — CLI usage in detail.
- [docs/source/user-guide/configs.md](../docs/source/user-guide/configs.md) — configuration option reference.
- [docs/source/user-guide/extending-components.md](../docs/source/user-guide/extending-components.md) — extension reference. The official version of file 7.
- [docs/source/user-guide/extensions-example.md](../docs/source/user-guide/extensions-example.md) — full worked S3 integration example. The canonical reference for "how do I wire a custom object store all the way through."
- [docs/source/user-guide/tuning-guide.md](../docs/source/user-guide/tuning-guide.md) — performance tuning.
- [docs/source/user-guide/metrics.md](../docs/source/user-guide/metrics.md) — metrics overview.
- [docs/source/user-guide/faq.md](../docs/source/user-guide/faq.md) — frequently asked questions.
- [docs/source/user-guide/rust.md](../docs/source/user-guide/rust.md) — Rust API tour.
- [docs/source/user-guide/spark-compatible-functions.md](../docs/source/user-guide/spark-compatible-functions.md) — function compatibility reference.

### Deployment

- [docs/source/user-guide/deployment/quick-start.md](../docs/source/user-guide/deployment/quick-start.md) — fastest path to a working cluster.
- [docs/source/user-guide/deployment/docker.md](../docs/source/user-guide/deployment/docker.md) — container basics.
- [docs/source/user-guide/deployment/docker-compose.md](../docs/source/user-guide/deployment/docker-compose.md) — multi-container.
- [docs/source/user-guide/deployment/kubernetes.md](../docs/source/user-guide/deployment/kubernetes.md) — Kubernetes shape.
- [docs/source/user-guide/deployment/cargo-install.md](../docs/source/user-guide/deployment/cargo-install.md) — install from source.

### Python

- [docs/source/user-guide/python/](../docs/source/user-guide/python/) — Python bindings; not in scope for this folder but useful if Eur Data adds Python notebook support.

### Top-level

- [README.md](../README.md) — high-level project overview, feature matrix.
- [ROADMAP.md](../ROADMAP.md) — what is being worked on. **Read this every few months** when planning Eur Data; the gaps that block you today may close.
- [CHANGELOG.md](../CHANGELOG.md) — version history.
- [CONTRIBUTING.md](../CONTRIBUTING.md) — how to contribute upstream. You will want to do this for the gaps that matter to you.

---

## 11.3 Upstream specs and projects

- **DataFusion** — https://datafusion.apache.org/ — the engine Ballista wraps. Read the [DataFusion Documentation](https://datafusion.apache.org/library-user-guide/index.html) for `SessionContext`, `TableProvider`, `ExecutionPlan`, optimizer rules.
- **Apache Arrow** — https://arrow.apache.org/ — the in-memory format. Read the [Format Specification](https://arrow.apache.org/docs/format/Columnar.html).
- **Apache Arrow Flight** — https://arrow.apache.org/docs/format/Flight.html — the protocol Ballista uses for data transfer.
- **Apache Arrow Flight SQL** — https://arrow.apache.org/docs/format/FlightSql.html — the JDBC-compatible variant. Relevant when you build customer-facing connectivity.
- **Substrait** — https://substrait.io/ — the optional alternative plan format.
- **Apache Iceberg** — https://iceberg.apache.org/ — your table format. The [spec](https://iceberg.apache.org/spec/) is essential reading.
- **iceberg-rust** — https://github.com/apache/iceberg-rust — official Rust implementation of Iceberg.
- **datafusion-iceberg** — https://github.com/datafusion-contrib/datafusion-iceberg — community DataFusion-Iceberg adapter.
- **KEDA** — https://keda.sh/ — Kubernetes Event-driven Autoscaler, supported by Ballista for executor autoscaling.

---

## 11.4 Suggested reading order for someone coming after you

For a colleague joining Eur Data who needs to get productive on Ballista:

1. Read [README.md](../README.md) and look at the architecture diagram ([docs/source/contributors-guide/ballista_architecture.excalidraw.svg](../docs/source/contributors-guide/ballista_architecture.excalidraw.svg)). 10 minutes.
2. Read [1-from_datafusion_to_ballista.md](1-from_datafusion_to_ballista.md). 20 minutes.
3. Read [2-cluster_topology_and_lifecycle.md](2-cluster_topology_and_lifecycle.md). 30 minutes.
4. Read [5-planning_stages_and_shuffles.md](5-planning_stages_and_shuffles.md) — this is the one that makes the system *click*. 40 minutes.
5. Skim [3-the_scheduler.md](3-the_scheduler.md) and [4-the_executor.md](4-the_executor.md) for the parts they need.
6. Read [7-extension_points.md](7-extension_points.md) when they have to extend something.
7. Read [10-applying_ballista_to_eur_data.md](10-applying_ballista_to_eur_data.md) for product context.
8. Refer to [6-wire_protocol_and_serialization.md](6-wire_protocol_and_serialization.md), [8-using_ballista_as_a_developer.md](8-using_ballista_as_a_developer.md), [9-deployment_and_operations.md](9-deployment_and_operations.md), and this glossary as needed.

Total time to working understanding: about half a day of focused reading.

---

## 11.5 End

That is the folder. The starting question was "how does Ballista work, how is it different from standalone DataFusion, and how can I use it." Every file from 1 to 10 was an attempt to answer one slice of that.

The next thing to do is build something. Start with step 1 of file 10's build sequence and work down.
