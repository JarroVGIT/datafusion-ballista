# 6. Wire protocol and serialization

**What you will know after reading this:** which `.proto` files define Ballista's protocol, which gRPC services and methods exist, how plans and configs travel between processes, what the codec extension points are for, and how Arrow Flight fits as the data plane companion to gRPC.

This file is essential reading before file 7 (extension points), because every interesting extension to Ballista is ultimately constrained by what can survive the wire.

---

## 6.1 Why this matters

Single-node DataFusion has no wire protocol because everything is in-process. Ballista has three distinct over-the-wire payloads:

- **Logical plans.** Client to scheduler. Must serialize through DataFusion's logical-plan protobuf format plus any extensions for custom logical nodes.
- **Physical plans.** Scheduler to executor. Must serialize through DataFusion's physical-plan protobuf format plus Ballista's shuffle nodes plus any extensions for custom physical nodes.
- **Data (RecordBatches).** Executor to executor (shuffle) and executor to client (results). Uses Arrow IPC over Arrow Flight, not protobuf.

Two different worlds: **gRPC + protobuf for control**, **Arrow Flight for data**. The split matters because protobuf is great for structured but small messages (a plan is at most a few KB to MB) while Arrow Flight is built for streaming large columnar batches with near-zero deserialization cost.

---

## 6.2 The proto files

[ballista/core/proto/](../ballista/core/proto/):

- **`ballista.proto`** — Ballista-specific messages: shuffle plan nodes, execution graph, stages, tasks, executor metadata, gRPC services. Some 800+ lines.
- **`datafusion.proto`** — Vendored DataFusion logical and physical plan messages. The proto definitions for every standard DataFusion plan node (TableScan, Projection, Filter, Aggregate, HashJoin, etc.) live here.
- **`datafusion_common.proto`** — Shared types (schemas, data types, error codes) used by both of the above.

[ballista/scheduler/proto/](../ballista/scheduler/proto/):

- **`keda.proto`** — KEDA external scaler API. Only used when the scheduler is compiled with `--features keda-scaler` and you want Kubernetes to scale executors based on scheduler-reported load.

The generated Rust code lives at [ballista/core/src/serde/generated/](../ballista/core/src/serde/generated/) and is exposed through [ballista/core/src/serde/mod.rs](../ballista/core/src/serde/mod.rs).

---

## 6.3 The two gRPC services

Both defined at the bottom of [ballista/core/proto/ballista.proto](../ballista/core/proto/ballista.proto).

### `SchedulerGrpc`

Lives at [ballista/core/proto/ballista.proto:823](../ballista/core/proto/ballista.proto#L823). Implemented by [ballista/scheduler/src/scheduler_server/grpc.rs](../ballista/scheduler/src/scheduler_server/grpc.rs).

Methods, grouped by who calls them:

| Caller | Method | Purpose |
|---|---|---|
| Executor | `PollWork` | Pull-mode work fetch (pull-staged policy) |
| Executor | `RegisterExecutor` | Initial registration |
| Executor | `HeartBeatFromExecutor` | Periodic liveness ping |
| Executor | `UpdateTaskStatus` | Report task progress / completion / failure |
| Executor | `ExecutorStopped` | Notify shutdown |
| Client | `CreateUpdateSession` | Create or update session config |
| Client | `RemoveSession` | Tear down a session |
| Client | `ExecuteQuery` | Submit a logical plan, get a job_id |
| Client | `ExecuteQueryPush` | Submit and stream status updates back |
| Client | `GetJobStatus` | Poll job status |
| Client | `GetJobMetrics` | Per-operator metrics for a job |
| Client | `CancelJob` | Cancel an in-flight job |
| Client | `CleanJobData` | Explicit cleanup of shuffle files |

The asymmetry is informative: the scheduler is talked *at* by executors (status updates, registration) and *queried* by clients (submission, status). It does not initiate calls to either party in this direction.

### `ExecutorGrpc`

Lives at [ballista/core/proto/ballista.proto:855](../ballista/core/proto/ballista.proto#L855). Implemented by [ballista/executor/src/executor_server.rs](../ballista/executor/src/executor_server.rs).

| Caller | Method | Purpose |
|---|---|---|
| Scheduler | `LaunchTask` | Dispatch a single task (push-staged policy) |
| Scheduler | `LaunchMultiTask` | Dispatch a batch of tasks |
| Scheduler | `CancelTasks` | Cancel running tasks |
| Scheduler | `RemoveJobData` | Delete shuffle files for a finished job |
| Scheduler | `StopExecutor` | Graceful shutdown trigger |

The scheduler is the only client of `ExecutorGrpc`. Clients and other executors never talk to it. (Executors do not even talk to each other on this service — peer-to-peer chatter happens over Arrow Flight only.)

---

## 6.4 Key message types

A handful of message types in [ballista/core/proto/ballista.proto](../ballista/core/proto/ballista.proto) are worth knowing by name, because you will see them in logs and error messages.

- **`BallistaLogicalPlanNode`** (line 33) — Ballista's wrapper for logical plan extensions. Currently used for `LogicalPlanCacheNode` (line 39), a marker for cached plans.
- **`BallistaPhysicalPlanNode`** (line 47) — Wrapper for Ballista's three physical plan extensions: `ShuffleWriterExecNode`, `SortShuffleWriterExecNode`, `UnresolvedShuffleExecNode`, `ShuffleReaderExecNode`. These are the protobuf encodings of the operators from file 5.
- **`ExecutionGraph`** (line 120) and **`ExecutionGraphStage`** (line 141) — The on-the-wire encoding of the scheduler's stage DAG. Used in some scheduler-internal contexts and in the REST API.
- **`TaskDefinition`** — What the scheduler sends to an executor to launch a task. Includes the physical plan (protobuf bytes), session config key-value pairs, the partition IDs this task is responsible for, and a task ID.
- **`TaskStatus`** (line 513) with variants `RunningTask`, `SuccessfulTask`, `FailedTask` — What executors send back. `SuccessfulTask` includes `ShuffleWritePartition` records describing where the output landed.
- **`PartitionLocation`** (line 263) — The "address" of a shuffle output: which executor has it, which file, the partition statistics. The scheduler collects these from successful tasks and threads them into downstream `ShuffleReaderExec`s.
- **`KeyValuePair`** (line 222) — The atomic unit of config transport. Session config is sent as a list of these. The receiving side reconstitutes typed values via the configured `ConfigProducer`.

If you ever need to debug an over-the-wire issue, start by getting a hex dump or `tonic`-level log of the failing RPC and matching it against these message definitions.

---

## 6.5 Codecs: the extension surface

This is where the protocol becomes pluggable.

DataFusion ships with two trait abstractions that Ballista builds on:

- **`LogicalExtensionCodec`** (from `datafusion_proto::logical_plan`) — Serialize and deserialize custom `UserDefinedLogicalNode` instances.
- **`PhysicalExtensionCodec`** (from `datafusion_proto::physical_plan`) — Serialize and deserialize custom `ExecutionPlan` implementations.

Ballista's defaults are:

- **`BallistaLogicalExtensionCodec`** ([ballista/core/src/serde/mod.rs](../ballista/core/src/serde/mod.rs)) — knows how to serialize Ballista's `LogicalPlanCacheNode` and delegates everything else to a chain of fallback codecs.
- **`BallistaPhysicalExtensionCodec`** — knows how to serialize the three shuffle nodes (`ShuffleWriterExec`, `ShuffleReaderExec`, `UnresolvedShuffleExec` + sort variant), and delegates everything else.

These are wrapped in `BallistaCodec<T, U>` ([ballista/core/src/serde/mod.rs](../ballista/core/src/serde/mod.rs)) — the type parameters are the logical and physical plan types respectively. The two natural choices are:

- `BallistaCodec<LogicalPlanNode, PhysicalPlanNode>` (default) — uses DataFusion's proto representations.
- `BallistaCodec<SubstraitPlanNode, SubstraitPhysicalPlanNode>` — uses Substrait. Optional, controlled by the `substrait` feature on the scheduler.

When you need to ship a custom plan node over the wire (because you have written a custom `TableProvider` for Iceberg, or a custom `UserDefinedLogicalNode` for some Eur-Data-specific operation), you:

1. Implement `LogicalExtensionCodec` (or `PhysicalExtensionCodec`) for your node.
2. Wrap your codec to chain to `BallistaLogicalExtensionCodec` (or `BallistaPhysicalExtensionCodec`) as a fallback.
3. Register the wrapped codec on **both** the client side (via `SessionConfigExt::set_logical_codec`) and the scheduler/executor side (via `SchedulerConfig::override_logical_codec` and `ExecutorProcessConfig::override_logical_codec`).

If you forget any of the three registration points, you get a runtime deserialization error the first time the relevant plan node crosses a process boundary. File 7 walks through the wiring concretely.

---

## 6.6 Substrait as an alternative

Substrait is a cross-language standard for representing query plans. The Ballista scheduler can use Substrait as its plan format instead of DataFusion's protobuf format, by parameterizing the codec types and enabling the `substrait` feature.

Why you might care:

- **Polyglot clients.** A Python or Java client that knows Substrait but not Ballista's DataFusion-specific protos can submit plans.
- **Cross-engine queries.** If you later want to share plans with another Substrait-aware engine.
- **Stability.** Substrait is a standardization effort with versioning; DataFusion's proto is whatever the current DataFusion version emits.

Why you might not:

- The DataFusion Substrait integration is not feature-complete relative to its native proto support; some plan nodes do not round-trip.
- For Eur Data this is probably a "phase 2" concern at best — your initial clients will be Rust, and the native codec is the path of least resistance.

The worked example is [examples/examples/standalone-substrait.rs](../examples/examples/standalone-substrait.rs).

---

## 6.7 Arrow Flight for data

gRPC + protobuf is great for plans, statuses, and registration messages — small, structured things. It is a bad fit for moving columnar data because every batch would require encoding into protobuf (slow) and decoding into Arrow (allocations and copies).

Arrow Flight ([Arrow project docs](https://arrow.apache.org/docs/format/Flight.html)) solves this. It is built on gRPC streams but the wire format inside each stream is Arrow IPC, which is identical to Arrow's in-memory format. On the receiving end, a `RecordBatch` is reconstructed without column-by-column deserialization.

Ballista uses Flight in two places:

1. **Executor-to-executor shuffle reads.** Each executor runs a Flight server on its `--bind-port` (default 50051), implemented at [ballista/executor/src/flight_service.rs](../ballista/executor/src/flight_service.rs). Downstream `ShuffleReaderExec` instances open Flight streams against the upstream executors and stream `RecordBatch`es. The fetch is parameterized by a `FetchPartition` action ([ballista/core/proto/ballista.proto:249](../ballista/core/proto/ballista.proto#L249)) that names the job, stage, partition, and file.

2. **Client result fetch.** When a job completes, the client opens Flight streams to the executors holding the final output partitions and pulls `RecordBatch`es directly. Alternatively, if `--advertise-flight-sql-endpoint` ([ballista/scheduler/src/config.rs:49](../ballista/scheduler/src/config.rs#L49)) is set, the scheduler runs a Flight proxy (via [ballista/scheduler/src/flight_proxy_service.rs](../ballista/scheduler/src/flight_proxy_service.rs)) that fronts the executors — useful when the client cannot reach executors directly (firewalls, NAT).

A useful mental model: **gRPC is the cluster's control bus, Arrow Flight is its data bus.** The two are co-located on different ports for a reason. If you ever build a network or security policy around Ballista, you want to think of these as distinct channels with very different traffic patterns (small bursty messages vs sustained high-throughput streaming).

---

## 6.8 Session config transport

This is subtle and worth a section to itself, because it bites people.

`SessionConfig` in DataFusion is rich: it has options, extensions (arbitrary type-erased `Any` values), a runtime env, a function registry. None of that is automatically serializable.

Ballista takes the pragmatic route: it serializes session config as a list of `KeyValuePair` ([ballista/core/proto/ballista.proto:222](../ballista/core/proto/ballista.proto#L222)) records. Each pair is a `(String, String)`. The receiving side reconstitutes a `SessionConfig` from this list using the configured `ConfigProducer` function ([ballista/core/src/extension.rs](../ballista/core/src/extension.rs)).

The implications:

- Anything you put in `SessionConfig` extensions must be reconstructible from string keys and values, or else stored out-of-band (e.g. in a secret manager that the producer reads from).
- The `ConfigProducer` must be available on both the client and the executor. Typically you bundle it into a shared crate.
- Function registries are *not* part of the config transport. They are configured via the runtime producer on the executor side, and must be registered identically on every executor.
- The default config producer is [`ballista_core::utils::default_config_producer`](../ballista/core/src/utils.rs). When you go beyond defaults, you pass `with_override_config_producer` on `SchedulerConfig` and `ExecutorProcessConfig`.

This is the mechanism through which, for example, S3 credentials and Iceberg catalog URLs are propagated from the client to the executors. File 7 walks through the full pattern.

---

## 6.9 gRPC message size limits

Defaults: 16 MiB on both ends, controlled by `--grpc-server-max-decoding-message-size` and `--grpc-server-max-encoding-message-size` on both scheduler and executor.

Cases where you hit the limit:

- Very wide SELECT lists (thousands of columns).
- Plans with many large UDF definitions inlined.
- Bulk session config (rare, but possible).
- Very large `ExecutionGraph`s reported by `GetJobStatus`.

If you hit it, increase both sides symmetrically. There is also a per-message client-side limit set on the Ballista client gRPC channel: `BALLISTA_CLIENT_GRPC_MAX_MESSAGE_SIZE` ([ballista/core/src/config.rs](../ballista/core/src/config.rs)).

---

## 6.10 mTLS

For wire-level security between scheduler, executors, and clients, Ballista supports mTLS. The setup is exactly what you would expect from `tonic`: each side gets a cert, a key, and a list of trusted CAs. The wiring is exposed through `SchedulerConfig::with_use_tls`, `EndpointOverrideFn` for clients, and equivalent hooks on executors.

The end-to-end example is [examples/examples/mtls-cluster.rs](../examples/examples/mtls-cluster.rs). For Eur Data you almost certainly want mTLS between scheduler and executors at minimum; for the client-scheduler hop you may end up replacing the raw gRPC service with an authenticated gateway (file 10).

---

## 6.11 What the wire protocol does *not* give you

- **No authentication.** mTLS proves identity but not authorization. There is no concept of "user X may run query Y."
- **No multi-tenancy.** All sessions share the cluster's resources.
- **No request-level encryption envelope.** mTLS encrypts the connection; the payload is not encrypted at the application layer.
- **No backwards-compatibility guarantees across versions.** Mixing Ballista versions between scheduler and executor is not supported. Upgrade all components together.

For Eur Data, you address these above the protocol (gateway, separate clusters per tenant, version pinning in deployment automation).

---

## 6.12 Where to next

You now understand what is on the wire. The natural next question is: how do I plug *my own* types into all this?

- Next: [7-extension_points.md](7-extension_points.md).
