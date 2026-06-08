# 7. Extension points

**What you will know after reading this:** every documented place where you can plug your own code into Ballista — what each hook is for, where it lives in the code, which process you have to register it in, and a matrix of "if you want to do X, override Y."

This is the prerequisite reading before file 10, where we tie these hooks to building Eur Data on top of Iceberg.

The official reference is [docs/source/user-guide/extending-components.md](../docs/source/user-guide/extending-components.md) and the worked S3 example is at [docs/source/user-guide/extensions-example.md](../docs/source/user-guide/extensions-example.md). This file complements them by explaining *why* and *when*; do not duplicate-read both.

---

## 7.1 The mental model

Ballista is built on a single design principle for extensibility: **the engine itself is generic; behavior is injected through producer functions and codecs**. This is what allows downstream projects to use Ballista as a foundation without forking it.

There are six hook families. They show up across the client, scheduler, and executor configs, with slightly different names but the same shape.

| Hook | What it does | Where you register it |
|---|---|---|
| **Logical codec** | Serialize custom `UserDefinedLogicalNode` | Client, scheduler, executor |
| **Physical codec** | Serialize custom `ExecutionPlan` | Client, scheduler, executor |
| **Config producer** | Build a `SessionConfig` from key-value bag | Client, scheduler, executor |
| **Runtime producer** | Build a `RuntimeEnv` from a `SessionConfig` | Executor (primarily) |
| **Function registry** | Register custom UDFs / UDAFs / window functions | Scheduler, executor |
| **Session builder** | Build a `SessionState` per session | Scheduler |
| **Execution engine** | Replace the per-task runtime | Executor |

There is also a smaller set of hooks for transport-level concerns (the `EndpointOverrideFn` for gRPC client setup) and for cluster-level concerns (the `BallistaCluster` trait and `DistributionPolicy`). These do not fit the "producer" pattern but they are extension points all the same.

---

## 7.2 The entry-point traits: `SessionStateExt` and `SessionConfigExt`

[ballista/core/src/extension.rs](../ballista/core/src/extension.rs) defines two extension traits that DataFusion `SessionState` and `SessionConfig` are blanket-implemented for:

- **`SessionStateExt`** ([ballista/core/src/extension.rs:101](../ballista/core/src/extension.rs#L101)) — methods to set up a `SessionState` for Ballista usage. `new_ballista_state(scheduler_url)` builds one from scratch; `upgrade_for_ballista(scheduler_url)` adapts an existing one. Both install the `BallistaQueryPlanner` and Ballista's default codecs.
- **`SessionConfigExt`** ([ballista/core/src/extension.rs:119](../ballista/core/src/extension.rs#L119)) — methods to set Ballista-specific options on a `SessionConfig`: `new_with_ballista()`, `with_ballista_job_name()`, `with_ballista_standalone_parallelism()`, `set_logical_codec()`, `set_physical_codec()`, etc.

These are what you call from client code. The pattern looks like:

```rust
use ballista::prelude::*;
use datafusion::prelude::*;
use datafusion::execution::SessionStateBuilder;

let config = SessionConfig::new_with_ballista()
    .with_target_partitions(8)
    .with_ballista_job_name("my job");

let state = SessionStateBuilder::new()
    .with_config(config)
    .with_default_features()
    .build();

let ctx = SessionContext::remote_with_state("df://scheduler:50050", state).await?;
```

Anything you want the client side to know about (custom codec, custom session config options) goes through these traits.

---

## 7.3 Logical and physical codecs

Already introduced in file 6. This is the most consequential hook for Eur Data because it is the gate that all custom plan nodes must pass through to reach the executors.

**When you need a custom codec:**

- You have a custom `UserDefinedLogicalNode` (perhaps an Iceberg-specific "scan snapshot" node) that the standard DataFusion proto cannot encode.
- You have a custom `TableProvider` that produces a custom `ExecutionPlan` (an Iceberg `IcebergTableExec` of some sort).
- You want to replace the entire serialization format with Substrait.

**How you wire it:**

1. Implement the trait (`LogicalExtensionCodec` from `datafusion_proto::logical_plan` or `PhysicalExtensionCodec` from `datafusion_proto::physical_plan`).
2. Chain it: your codec handles your nodes and delegates everything else to `BallistaLogicalExtensionCodec::default()` (or `BallistaPhysicalExtensionCodec::default()`).
3. Register it three places:
   - **Client:** `SessionConfig::set_logical_codec(Arc::new(my_codec))` and the physical equivalent before building the `SessionContext`.
   - **Scheduler:** `SchedulerConfig::with_override_logical_codec(Arc::new(my_codec))` / `with_override_physical_codec(...)`.
   - **Executor:** `ExecutorProcessConfig::override_logical_codec = Some(Arc::new(my_codec))` / `override_physical_codec = ...`.

Forget any of the three and you get a deserialization error the first time the affected plan node crosses a process boundary. The error often manifests as "unknown extension node" or similar; look for it in scheduler logs first.

**A subtlety:** the codec is shared across all sessions in the scheduler/executor. There is no per-session codec selection. If you have multiple plugin systems, your codec needs to dispatch internally based on something like a node-type tag.

---

## 7.4 Config and runtime producers

This is the second most important hook because it controls what runtime resources (object stores, function registries, memory pools) the executor actually has access to.

**`ConfigProducer`** is `Arc<dyn Fn() -> SessionConfig + Send + Sync>`. It builds a fresh `SessionConfig` with whatever defaults and extensions your application needs. Ballista's default is `default_config_producer` ([ballista/core/src/utils.rs](../ballista/core/src/utils.rs)).

**`RuntimeProducer`** is `Arc<dyn Fn(&SessionConfig) -> Result<Arc<RuntimeEnv>>>`. Given a `SessionConfig`, it builds a `RuntimeEnv`. This is where you register custom `ObjectStore` implementations, custom memory pools, custom disk managers.

**The flow on the executor** when a task arrives:

1. Deserialize task → physical plan + list of `KeyValuePair` config settings + session ID.
2. Call `ConfigProducer()` to get a fresh `SessionConfig` with your defaults.
3. Apply each `KeyValuePair` to the config (set the option).
4. Call `RuntimeProducer(&config)` to get a `RuntimeEnv`.
5. Build a `TaskContext` from the config and runtime env.
6. Execute the plan with that context.

This means the `KeyValuePair`s shipped in the task are essentially "deltas" on top of your default config. Anything that is constant for your cluster (the S3 endpoint URL, the Iceberg catalog URI, the path to a model file) lives in the producer functions and never travels over the wire. Anything that varies per query (target partitions, broadcast threshold, custom session option `eurdata.tenant_id`) goes in the `KeyValuePair` bag.

**Practical pattern for credentials:** the producer reads from environment variables or a secret manager, not from the wire. Never put credentials in a `KeyValuePair` that travels over gRPC.

The worked S3 example at [docs/source/user-guide/extensions-example.md](../docs/source/user-guide/extensions-example.md) shows this pattern in full. Read that file once for the concrete code; the conceptual takeaway is here.

---

## 7.5 Function registry

For UDFs, UDAFs, and window functions.

DataFusion has a `FunctionRegistry` trait; Ballista wraps it with `ballista_scalar_functions()`, `ballista_aggregate_functions()`, `ballista_window_functions()` in [ballista/core/src/extension.rs:66](../ballista/core/src/extension.rs#L66). These produce the default set, optionally augmented with Spark-compatible functions when the `spark-compat` feature is enabled.

To register your own functions:

- On the **client**, register them on the `SessionState` you build before passing it to `SessionContext::remote_with_state(...)`.
- On the **executor**, register them via `ExecutorProcessConfig::override_function_registry` (see [ballista/executor/src/config.rs:211](../ballista/executor/src/config.rs#L211)).
- On the **scheduler**, register them via the session builder (next section) so that planning has access to them.

Why three places: planning uses the registry to resolve function names; execution uses the registry to actually run the function. Both happen on the scheduler (planning) and executor (execution); the client uses the registry to validate before submitting.

If you forget the executor registration, the scheduler will accept the plan and then the executor will fail with an "unknown function" error at task launch.

A note on UDF languages: today, UDFs must be Rust. Python UDFs are on the roadmap ([ROADMAP.md](../ROADMAP.md)) but not shipped. If your roadmap needs JavaScript/Python/Wasm UDFs, plan for either contributing upstream or running a sidecar runtime out of an `ExecutionEngine` override.

---

## 7.6 Session builder (scheduler only)

`SessionBuilder` is `Arc<dyn Fn(SessionConfig) -> Result<SessionState> + Send + Sync>`. It is the scheduler's hook for building a per-session `SessionState` when a client creates or updates a session via `CreateUpdateSession`.

This is where you do anything that is per-session but not per-task:

- Register a tenant-specific catalog.
- Configure statistics providers.
- Pre-load metadata.
- Inject session-scoped UDFs or table sources.

Register via `SchedulerConfig::with_override_session_builder(...)` ([ballista/scheduler/src/config.rs:403](../ballista/scheduler/src/config.rs#L403)). The exported type alias is `SessionBuilder` from [ballista/scheduler/src/lib.rs:49](../ballista/scheduler/src/lib.rs#L49).

For Eur Data, this is one of the natural places to implement per-tenant metadata isolation: the session ID maps to a tenant; the session builder loads only that tenant's catalog into the `SessionState`. Then any query in that session can only see that tenant's tables.

(That gives you logical isolation, not security isolation. Security still requires authentication on the gRPC, which is a layer you add separately. See file 10.)

---

## 7.7 Object store registry

DataFusion's `RuntimeEnv` includes an `ObjectStoreRegistry` that maps URL schemes (`s3://`, `gs://`, `file://`, etc.) to `ObjectStore` implementations. Ballista has a thin wrapper at [ballista/core/src/object_store.rs](../ballista/core/src/object_store.rs) (compiled with the `build-binary` feature) that configures S3 from environment variables for the binary distribution.

If you want to plug in a custom object store (or just configure the standard `object_store` crate's stores in your own way), do it in the runtime producer. Concretely:

```rust
let runtime_producer = Arc::new(|config: &SessionConfig| -> Result<Arc<RuntimeEnv>> {
    let mut builder = RuntimeEnvBuilder::new();
    let registry = MyObjectStoreRegistry::new(/* read endpoints from config or env */);
    builder = builder.with_object_store_registry(Arc::new(registry));
    Ok(Arc::new(builder.build()?))
});
```

Register this on the executor side. For Iceberg, the object store hook is one piece; the other is catalog access, which goes through the table provider chain, which goes through the session builder.

---

## 7.8 Execution engine (executor only)

`ExecutionEngine` ([ballista/executor/src/execution_engine.rs](../ballista/executor/src/execution_engine.rs)) is the trait that abstracts "how does this executor actually run a task." The default uses DataFusion's standard physical execution.

Reasons to replace it:

- **Acceleration.** Route certain plans to a GPU runtime; fall back to CPU for the rest.
- **Per-task auditing.** Wrap execution with tracing, metrics, billing instrumentation.
- **Per-task isolation.** Run each task in a separate process or container.
- **Alternative engines.** Bridge to Velox, Arrow Acero, your own Rust engine.

Register via `ExecutorProcessConfig::override_execution_engine`. The engine is a single instance per executor process; per-task routing decisions are made inside the engine.

For Eur Data, this is the right place to attach per-tenant resource accounting (CPU seconds, memory peak, bytes scanned) and to enforce per-tenant quotas before the task runs.

---

## 7.9 Cluster backend (`BallistaCluster`)

[ballista/scheduler/src/cluster/mod.rs](../ballista/scheduler/src/cluster/mod.rs) defines the `BallistaCluster` trait. This is what the scheduler talks to for cluster-state persistence: executor registry, session storage, job state.

The only implementation shipped today is `InMemoryCluster` at [ballista/scheduler/src/cluster/memory.rs](../ballista/scheduler/src/cluster/memory.rs). For a Snowflake-class product you will need persistence and probably eventually multi-scheduler. Both require a different backend.

Implementing a custom backend is doable but non-trivial. The trait surface covers:

- Listing and updating executor metadata.
- Storing and retrieving session state.
- Storing and retrieving job execution graphs.
- Listening for events (e.g. "a new scheduler instance joined").

The roadmap entry "Support for multi-scheduler deployments" ([ROADMAP.md](../ROADMAP.md)) signals that this is being thought about upstream; for Eur Data, you may want to wait, contribute, or build your own. File 10 discusses this trade-off.

---

## 7.10 Task distribution policy

`TaskDistributionPolicy::Custom(Arc<dyn DistributionPolicy>)` ([ballista/scheduler/src/config.rs:471](../ballista/scheduler/src/config.rs#L471)) lets you write your own task-to-executor assignment. The trait is `DistributionPolicy` in [ballista/scheduler/src/cluster/mod.rs](../ballista/scheduler/src/cluster/mod.rs).

Use cases:

- **Affinity scheduling.** Place tasks on executors that have warm shuffle data or cached files from a previous query.
- **Multi-tenant fairness.** Reserve slots for tenant A while still using surplus capacity for tenant B.
- **Hardware-aware placement.** Route GPU-tagged tasks to GPU executors.
- **Locality.** Place scan tasks on executors closest to the object store region.

The default (`Bias`) packs tasks tightly; `RoundRobin` spreads them. For a multi-tenant production deployment, you almost certainly outgrow both of these.

---

## 7.11 gRPC endpoint override (`EndpointOverrideFn`)

`EndpointOverrideFn` ([ballista/core/src/extension.rs:55](../ballista/core/src/extension.rs#L55)) is `Arc<dyn Fn(Endpoint) -> Result<Endpoint, ...>>`. It runs against every gRPC client `Endpoint` before connection. Use it for:

- TLS configuration (client certificates, root CAs).
- Connection timeouts and keep-alive tuning.
- TCP buffer sizes.
- Custom HTTP/2 settings.

Register via `SchedulerConfig::with_override_create_grpc_client_endpoint(...)` and the equivalent on `ExecutorProcessConfig`. The client side configures it via the `SessionConfigExt` API.

---

## 7.12 What hooks at which layer? A quick matrix

| Goal | Hook | Process |
|---|---|---|
| Register an Iceberg `TableProvider` | Session builder (scheduler) + runtime producer (executor for object store) + logical codec (all three) | All three |
| Add a custom UDF | Function registry override | Scheduler + executor |
| Configure S3 / GCS / Azure credentials | Runtime producer (object store registry) | Executor (and client for local reads) |
| Add a custom logical plan node | Logical codec | All three |
| Add a custom physical operator | Physical codec + possibly execution engine | All three; engine on executor only |
| Use Substrait as plan format | Parameterize `BallistaCodec<T, U>` types + enable `substrait` feature | Scheduler + executor + client |
| Inject mTLS | Endpoint override | All three |
| Per-tenant catalog isolation | Session builder | Scheduler |
| Per-tenant resource quotas | Execution engine override | Executor |
| GPU acceleration for some operators | Execution engine override | Executor |
| Custom scheduler state backend (e.g. for HA) | Implement `BallistaCluster` trait | Scheduler |
| Affinity-aware task placement | Custom `DistributionPolicy` | Scheduler |
| Persist job history past TTL | Listen on scheduler events; ship to your own store | External, but events come from scheduler |
| Per-query session config | `SessionConfigExt::with_ballista_*` methods + config producer to set defaults | Client (per query) and executor (defaults) |

---

## 7.13 What is *not* extensible

To save you time looking:

- **The protobuf message schemas themselves.** You cannot add fields to the existing message types without forking. Extension is through codec wrappers, not schema evolution.
- **The stage state machine.** No hook to inject a new state.
- **The event loop's event types.** Adding a new event type requires modifying the scheduler.
- **The wire protocol for shuffles.** Arrow Flight, period.
- **The client-scheduler RPC contract.** Adding a new RPC method requires modifying both ends.

For everything else there is a hook. For these things, you fork or you contribute.

---

## 7.14 A pragmatic recommendation for Eur Data

If you take only one thing from this file: **establish a shared "platform" crate early.** That crate contains:

- Your codec implementations (Iceberg nodes, any custom operators).
- Your config producer (with defaults for your catalog endpoint, object store, etc.).
- Your runtime producer (object store registry, custom function registry).
- Your session builder (per-tenant catalog loading).
- Optionally, your execution engine wrapper (resource accounting).

Both the scheduler binary, the executor binary, and the client SDK link against this crate. You ship a single platform jar (well, crate) that is the source of truth for what Ballista knows how to handle in your environment.

This avoids the most common Ballista extension bug: "I registered the codec on the client, forgot the executor, and now task launch fails." By centralizing in one crate and wiring it into all three components consistently, that whole class of bug goes away.

File 10 picks this up concretely.

---

## 7.15 Where to next

You now know what is pluggable. The next file shifts gears and shows what *using* Ballista looks like from the application developer's seat.

- Next: [8-using_ballista_as_a_developer.md](8-using_ballista_as_a_developer.md).
