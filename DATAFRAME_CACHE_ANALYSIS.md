# `DataFrame.cache()` in Ballista: Analysis and Implementation Design

> Status: design analysis / RFC-style writeup.
> Scope: why `DataFrame.cache()` matters for a distributed engine, what Ballista does today, and three concrete ways to finish it.
> Source of truth: this repository at the current checkout, and the `datafusion` 53.1.0 dependency checked out at `/Users/jarrovgit/Development/datafusion` (`branch-53`, tag `53.1.0`).

---

## 1. Executive summary

`DataFrame.cache()` lets a user materialize the result of an expensive subplan once and reuse it across many subsequent queries. In single-node DataFusion this is trivial: run the plan, hold the `RecordBatch`es in a `MemTable`, and serve them from process memory. In Ballista the same operation is fundamentally harder, because the `SessionContext` the user holds is a **thin client**: nothing executes locally, every query is shipped to a scheduler and fanned out to executors, and an in-memory table on the client is invisible to the cluster.

The most surprising finding of this analysis is that **Ballista already contains the logical half of a caching feature, but it is unfinished and disabled by default**:

- A `BallistaCacheFactory` is registered on every Ballista session, so DataFusion's `cache()` takes the lazy `CacheFactory` branch and *never* runs the eager `MemTable` path.
- By default (`ballista.cache.noop = true`) the factory is a **silent no-op** — `cache()` returns the plan unchanged and nothing is cached.
- When enabled (`ballista.cache.noop = false`), the factory wraps the plan in a `BallistaCacheNode`, which then **fails at physical planning** because no `ExtensionPlanner` knows how to turn it into an `ExecutionPlan`. A test asserts exactly this failure.

So "`cache()` is not supported" really means: the logical node, its protobuf, its logical codec, and the config flag all exist; what is missing is the **physical planner plus the distributed materialization / storage / serving layer**.

This document:

1. Explains how `cache()` works in DataFusion and why it is useful in a cluster (sections 2-3).
2. Traces precisely what Ballista does today, with file/line citations (section 4).
3. Answers the two questions posed directly — *would the MemTable be available on all executors?* and *would we serialize it and ship it through the physical codec from client to scheduler to executors?* (section 5).
4. Designs three implementation options end to end, including the serialization path for each (section 6).
5. Gives an adversarial review, a recommendation (executor-resident cache reusing the shuffle machinery), a size-adaptive hybrid, and a phased plan (sections 7-9).

---

## 2. What `DataFrame.cache()` does in DataFusion

The method is small and is the contract Ballista has to satisfy. From `datafusion/core/src/dataframe/mod.rs:2387-2402`:

```rust
pub async fn cache(self) -> Result<DataFrame> {
    if let Some(cache_factory) = self.session_state.cache_factory() {
        let new_plan = cache_factory.create(self.plan, self.session_state.as_ref())?;
        Ok(Self::new(*self.session_state, new_plan))
    } else {
        let context = SessionContext::new_with_state((*self.session_state).clone());
        let plan = self.clone().create_physical_plan().await?;
        let schema = plan.schema();
        let task_ctx = Arc::new(self.task_ctx());
        let partitions = collect_partitioned(plan, task_ctx).await?;   // eager execution
        let mem_table = MemTable::try_new(schema, partitions)?;
        context.read_table(Arc::new(mem_table))                        // register in-memory table
    }
}
```

There are two branches:

- **No `CacheFactory` registered (the default in plain DataFusion):** eager. It physically plans the query, drives it to completion with `collect_partitioned` on the *local* task context, builds a `MemTable`, and registers it. The cached data lives in the process that called `cache()`.
- **A `CacheFactory` is registered:** lazy. It calls `cache_factory.create(plan, state)` and returns whatever new logical plan that produces. No execution happens at call time.

The `CacheFactory` trait (`datafusion/core/src/execution/session_state.rs:2139-2146`) is intentionally a hook for engines like Ballista, and its doc comment says as much:

```rust
/// A [`CacheFactory`] can be registered via [`SessionState`]
/// ... Additionally, a custom [`ExtensionPlanner`]/[`QueryPlanner`]
/// may need to be implemented to handle such plans.
pub trait CacheFactory: Debug + Send + Sync {
    fn create(
        &self,
        plan: LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion_common::Result<LogicalPlan>;
}
```

Two properties matter for the rest of this document:

1. `create` is **synchronous** (`fn create`, not `async fn`). It cannot itself run a distributed job and await results. The intended pattern is: `create` returns a custom logical node, and a separate **`ExtensionPlanner`** materializes lazily at physical-planning time.
2. The reference implementation of that pattern is shipped as an example: `datafusion-examples/examples/dataframe/cache_factory.rs`. It defines a `CacheNode` (a `UserDefinedLogicalNode`), a `CacheNodePlanner` (`ExtensionPlanner`) that on first execution calls `collect_partitioned(physical_inputs[0], ctx)` and stores the batches in a `CacheManager`, returning a `MemorySourceConfig`-backed `DataSourceExec` on every execution thereafter, and a `CacheNodeQueryPlanner` that installs the planner. **This is exactly the shape Ballista must adapt for distribution.**

---

## 3. Why caching is useful in a distributed engine

Caching pays off whenever the same expensive subplan is executed more than once and that subplan dominates runtime. In a cluster the win is larger than single-node because the avoided work includes source I/O *and* network shuffles. Concrete cases:

- **Iterative ML / analytics loops.** Feature engineering over a multi-TB Parquet table, then 10-50 training iterations that each reference the transformed features. Without caching, the full scan-and-transform reruns every iteration. With caching, iteration 1 materializes the transformed dataset across the cluster and every later iteration reads it back.
- **Interactive notebooks / repeated subplans.** Cells that share a filtered, joined fact table. In single-node DataFusion the eager `MemTable` holds the result in-process. In Ballista, without a working cache, each cell re-submits the whole subplan as a fresh distributed job.
- **Broadcast-join build side.** A large dimension table broadcast to all executors. Caching its `scan + filter` lets the scheduler reuse the materialized build side instead of re-reading and re-distributing the dimension for every join.
- **Fan-out pipelines.** One `GROUP BY` result feeding several downstream branches. Without caching the grouped stage runs once per branch; with caching all branches read the same materialized partitions.

The crucial point: **a node-local `MemTable` does not generalize to a cluster.** The cached batches would live in the client process, but execution happens on executors. As section 5 shows, you cannot even ship a `MemTable`-backed scan to the cluster through Ballista's current codecs. A real distributed cache must keep the materialized data *where the executors can reach it* and let the scheduler splice a cheap "read the cache" plan into later queries.

---

## 4. How Ballista handles `cache()` today

### 4.1 The pieces that already exist

Ballista turns a normal `SessionContext` into a cluster client by rebuilding its `SessionState`. Both entry points register a cache factory:

- [`new_ballista_state()` — extension.rs:287-309](ballista/core/src/extension.rs#L287-L309) and [`upgrade_for_ballista()` — extension.rs:311-355](ballista/core/src/extension.rs#L311-L355) call `.with_cache_factory(Some(Arc::new(BallistaCacheFactory::new())))` ([extension.rs:300](ballista/core/src/extension.rs#L300), [extension.rs:324](ballista/core/src/extension.rs#L324)).

Because a factory is present, **DataFusion's eager `MemTable` branch is unreachable in Ballista** — `cache()` always calls `BallistaCacheFactory::create`.

The factory ([extension.rs:903-921](ballista/core/src/extension.rs#L903-L921)):

```rust
impl CacheFactory for BallistaCacheFactory {
    fn create(&self, plan: LogicalPlan, session_state: &SessionState)
        -> datafusion::error::Result<LogicalPlan>
    {
        if session_state.config().ballista_config().cache_noop() {
            Ok(plan)                                  // default: no-op
        } else {
            Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(BallistaCacheNode::new(
                    Uuid::new_v4().to_string(),
                    session_state.session_id().to_string(),
                    plan,
                )),
            }))
        }
    }
}
```

The gate is `ballista.cache.noop`, which **defaults to `true`** ([config.rs:116-119](ballista/core/src/config.rs#L116-L119), accessor [config.rs:374-377](ballista/core/src/config.rs#L374-L377)).

`BallistaCacheNode` ([extension.rs:923-991](ballista/core/src/extension.rs#L923-L991)) is a `UserDefinedLogicalNodeCore` carrying `cache_id` (a fresh UUIDv4), `session_id`, the wrapped `input` plan, and an empty `exprs`. Its schema delegates to `self.input.schema()`.

Serialization of the node already works. The logical codec ([serde/mod.rs:233-289](ballista/core/src/serde/mod.rs#L233-L289)) encodes/decodes it via the proto message `LogicalPlanCacheNode { cache_id, session_id }` ([ballista.proto:39-42](ballista/core/proto/ballista.proto#L39-L42)). Note what crosses the wire: **only `cache_id` and `session_id`**. The wrapped subplan rides along as the node's ordinary child input and is reconstructed on decode ([serde/mod.rs:249-260](ballista/core/src/serde/mod.rs#L249-L260)).

### 4.2 The piece that is missing

There is **no `ExtensionPlanner` for `BallistaCacheNode` anywhere** (no `impl ExtensionPlanner` / `plan_extension` in `ballista/scheduler/src/`), and no physical exec node to materialize or serve the cache. The behaviour is documented by a test whose name is aspirational, [`should_support_on_cache_collect` — context_unsupported.rs:107-134](ballista/client/tests/context_unsupported.rs#L107-L134):

```rust
ctx.sql("SET ballista.cache.noop = false").await?.show().await?;
let cached_df = ctx.sql("SELECT 1").await?.cache().await?;
let result = cached_df.collect().await;
assert!(result.is_err());
let err_msg = result.unwrap_err().to_string();
assert!(err_msg.contains(
    "No installed planner was able to convert the custom node to an execution plan: BallistaCacheNode"
));
```

### 4.3 End-to-end trace

**Default (`ballista.cache.noop = true`):**

```
df.cache()
  -> DataFrame::cache() sees Some(cache_factory)            dataframe/mod.rs:2388
  -> BallistaCacheFactory::create, noop == true             extension.rs:909
  -> returns the input plan unchanged                       extension.rs:910
  => cache() is a silent no-op; later collect() re-runs the full query.
```

**Enabled (`ballista.cache.noop = false`):**

```
df.cache()
  -> BallistaCacheFactory::create wraps plan in BallistaCacheNode     extension.rs:912-918
cached_df.collect()
  -> BallistaQueryPlanner::create_physical_plan: not local,
     wraps whole plan in DistributedQueryExec                        planner.rs:152-162
  -> DistributedQueryExec::execute encodes the logical plan;
     BallistaCacheNode -> LogicalPlanCacheNode {cache_id, session_id} serde/mod.rs:264-289
  -> bytes sent to scheduler via ExecuteQuery gRPC                   distributed_query.rs
  -> scheduler decodes (BallistaCacheNode round-trips)              serde/mod.rs:234-261
  -> scheduler calls state().create_physical_plan(plan)             scheduler/state/mod.rs:449
  -> DefaultPhysicalPlanner finds no ExtensionPlanner for the node
  => Error: "No installed planner was able to convert the custom
     node to an execution plan: BallistaCacheNode"
```

So the failure is on the **scheduler**, during physical planning, after the logical plan has already been shipped successfully. The job never reaches executors.

### 4.4 Inventory: exists vs. missing

| Component | Status | Location |
|---|---|---|
| `CacheFactory` registration | exists | [extension.rs:300](ballista/core/src/extension.rs#L300), [:324](ballista/core/src/extension.rs#L324) |
| `ballista.cache.noop` config gate (default `true`) | exists | [config.rs:116-119](ballista/core/src/config.rs#L116-L119) |
| `BallistaCacheFactory` | exists | [extension.rs:894-921](ballista/core/src/extension.rs#L894-L921) |
| `BallistaCacheNode` logical node | exists | [extension.rs:923-991](ballista/core/src/extension.rs#L923-L991) |
| `LogicalPlanCacheNode` proto + logical codec | exists | [ballista.proto:39-42](ballista/core/proto/ballista.proto#L39-L42), [serde/mod.rs:233-289](ballista/core/src/serde/mod.rs#L233-L289) |
| `ExtensionPlanner` for the node | **missing** | — |
| Physical exec node to materialize / serve cache | **missing** | — |
| Physical codec entry for that node | **missing** | [serde/mod.rs:538-698](ballista/core/src/serde/mod.rs#L538-L698) (else-branch errors) |
| Cross-job cache registry on the scheduler | **missing** | — |
| Cache file pinning / cleanup exclusion | **missing** | [executor_process.rs:676-714](ballista/executor/src/executor_process.rs#L676-L714) |
| Eviction / lifecycle | **missing** | — |

---

## 5. The core distributed problem (answering the two questions directly)

The user asked two precise questions. Here are the architecture-level answers; per-option nuances are in section 6.

### 5.1 "Would the MemTable be available on all executors?"

**No — not if you keep the data in a `MemTable`.** A `MemTable` is an in-process collection of Arrow arrays. It lives in whichever process built it (the client, or, if you materialized server-side, one scheduler/executor process). Three independent facts make a client-side `MemTable` invisible to the cluster:

1. **The client is a thin facade.** Every non-trivial query is intercepted by `BallistaQueryPlanner` and wrapped in `DistributedQueryExec` ([planner.rs:104-165](ballista/core/src/planner.rs#L104-L165)), then shipped to the scheduler. Nothing in the client's memory is automatically visible to executors.
2. **A `MemTable` scan cannot be serialized to ship it.** When a `TableScan` wraps a non-listing provider such as `MemTable`, datafusion-proto routes encoding to `try_encode_table_provider`. Ballista's implementation ([serde/mod.rs:302-310](ballista/core/src/serde/mod.rs#L302-L310)) delegates to `DefaultLogicalExtensionCodec::try_encode_table_provider`, which returns `not_impl_err!`. So a logical plan that scans a `MemTable` **cannot even be sent to the scheduler**, let alone to executors.
3. **Executor disks are private.** Each executor has its own local `work_dir` ([executor.rs](ballista/executor/src/executor.rs)); there is no shared in-memory or local-disk namespace across executors. Data on executor A is reachable from executor B only via Arrow Flight (the shuffle read path) or a shared object store.

The practical conclusion: a working distributed cache should **not** try to broadcast a `MemTable`. It should keep materialized partitions where executors already know how to fetch them (the shuffle/`PartitionLocation` mechanism) or in shared object storage, and pass only *metadata* around.

### 5.2 "Would we serialize it and send it in the physical codec from client to scheduler to executors?"

**For the recommended design, no — the cached data never travels through the physical codec at all.** This is the key insight, and it falls out of how Ballista already moves intermediate results.

Ballista has two codecs:

- **Logical codec** (`BallistaLogicalExtensionCodec`): client to scheduler. Encodes the logical plan, including `BallistaCacheNode` (just `cache_id` + `session_id`).
- **Physical codec** (`BallistaPhysicalExtensionCodec`, [serde/mod.rs:363-699](ballista/core/src/serde/mod.rs#L363-L699)): scheduler to executors. Encodes the per-stage `ExecutionPlan`. It supports exactly `ShuffleWriterExec`, `SortShuffleWriterExec`, `ShuffleReaderExec`, and `UnresolvedShuffleExec`; anything else hits the else-branch and errors with `"unsupported plan type"` ([serde/mod.rs:694-698](ballista/core/src/serde/mod.rs#L694-L698)).

Critically, **neither codec ever serializes `RecordBatch` payloads.** When a stage finishes, its output partitions are written to local Arrow IPC files on the executor, and the only thing that crosses the wire afterward is a `PartitionLocation` — a small struct addressing *where* a partition lives ([serde/scheduler/mod.rs:83-111](ballista/core/src/serde/scheduler/mod.rs#L83-L111)):

```rust
pub struct PartitionLocation {
    pub map_partition_id: usize,
    pub partition_id: PartitionId,         // job_id, stage_id, partition_id
    pub executor_meta: ExecutorMetadata,   // host, port, grpc_port
    pub partition_stats: PartitionStats,
    pub file_id: Option<u64>,
    pub is_sort_shuffle: bool,
}
```

`ShuffleReaderExec` consumes a list of these and reads the data on demand: local partitions straight from Arrow IPC files, remote partitions via Arrow Flight ([shuffle_reader.rs:619-635](ballista/core/src/execution_plans/shuffle_reader.rs#L619-L635)). The bytes stay on executor disks; only `PartitionLocation` metadata is encoded in the physical plan.

That is the model the recommended design reuses: a cached subplan is materialized as a *pinned* shuffle-style stage; subsequent queries get a `ShuffleReaderExec` over the cached `PartitionLocation`s. **The answer to the question is therefore: only `PartitionLocation` metadata travels through the physical codec; the cached `RecordBatch`es never do.** (Option A, by contrast, *does* try to push IPC bytes through the wire and runs straight into the gRPC message-size ceiling — see section 6.1.)

### 5.3 One more constraint: shuffle files are garbage-collected

Whatever a cache stores on executor disks must survive the normal cleanup that destroys ordinary shuffle output. Cleanup happens two ways, both keyed by `job_id`:

- The scheduler issues `remove_job_dir` to executors a short time after a job completes (`finished_job_data_clean_up_interval_seconds`, default 300s — [scheduler/config.rs:271](ballista/scheduler/src/config.rs#L271)).
- Each executor runs a TTL sweep, `clean_shuffle_data_loop`, deleting any `work_dir` subdirectory older than the data TTL (default 7 days — [executor_process.rs:676-714](ballista/executor/src/executor_process.rs#L676-L714)).

A cache must therefore live under a path that these sweeps skip (e.g. a `_cache_`-prefixed directory), and its `PartitionLocation`s must remain valid across jobs. This "pinning" requirement is central to Option B.

---

## 6. Implementation options

All three options keep the existing `BallistaCacheFactory` / `BallistaCacheNode` / logical codec untouched and add the missing `ExtensionPlanner` plus a materialization/serving mechanism. They differ in *where the cached data lives* and *what crosses the wire*.

### 6.1 Option A — Client-collected `MemTable`, broadcast via the codec

**Idea.** Mirror DataFusion's eager default, but cluster-aware. An `ExtensionPlanner` on the **client** drives the cached subplan as a real distributed job (`collect_partitioned` over a `DistributedQueryExec`), stores the resulting `Vec<Vec<RecordBatch>>` in a client-side `BallistaCacheManager`, and serves later reads from RAM. To make the cache usable inside larger queries that still go to the cluster, a custom `CachedTableProvider` is serialized as **Arrow IPC bytes embedded in the logical plan** via a real `try_encode_table_provider` / `try_decode_table_provider`.

**Data flow.**

```
cache() (noop=false): wrap in BallistaCacheNode (cache_id minted)
collect():
  client ExtensionPlanner.plan_extension:
    miss -> collect_partitioned(DistributedQueryExec) -> run real cluster job,
            stream results back, store batches in client CacheManager
    hit  -> MemorySourceConfig DataSourceExec from RAM   (served locally, no cluster)
larger query using the cache:
    rewrite BallistaCacheNode -> TableScan(CachedTableProvider)
    logical codec encodes batches as Arrow IPC bytes in the plan proto
    scheduler decodes -> rebuilds an in-memory table -> plans the rest
```

**Serialization (answering the two questions for A).** The cached bytes travel through the **logical** codec (inside `try_encode_table_provider`), client to scheduler, embedded in `ExecuteQueryParams.query`. They do **not** go through the `BallistaPhysicalExtensionCodec`. Is the MemTable "available on all executors"? Only by re-transmitting the entire payload on every query and having each executor decode its own copy — and even that breaks, because once the scheduler turns the decoded table into a `MemorySourceConfig` `DataSourceExec`, that node cannot be encoded for executors (it falls into the physical codec's error branch). So Option A is only correct for **pure cache-read queries that can be served on a single process**, sized **under the gRPC message limit** (`BALLISTA_CLIENT_GRPC_MAX_MESSAGE_SIZE`, default on the order of 16 MiB — [config.rs:132-135](ballista/core/src/config.rs#L132-L135)).

**Physical planner.** The `ExtensionPlanner` is installed **client-side** (not on the scheduler), because the cache is served from client RAM. A separate optimizer/rewrite rule must replace `BallistaCacheNode` with the `CachedTableProvider` scan before submission, and the `LocalRun` visitor ([planner.rs:172-200](ballista/core/src/planner.rs#L172-L200)) — which today only treats `information_schema` scans as local — must be taught to treat the cached provider as local too.

**Lifecycle / fault tolerance.** Cache lives in client RAM, dies with the client process, no scheduler/executor state. Materialization inherits normal job fault tolerance. No eviction by default; the `DashMap` grows unbounded.

**Code sketch (abridged).**

```rust
// client-side ExtensionPlanner
async fn plan_extension(&self, _p, node, _li, physical_inputs, state)
    -> Result<Option<Arc<dyn ExecutionPlan>>>
{
    let Some(cn) = node.as_any().downcast_ref::<BallistaCacheNode>() else { return Ok(None) };
    if let Some(batches) = self.cache_manager.get(cn.cache_id()) {
        return Ok(Some(MemorySourceConfig::try_new_exec(&batches, physical_inputs[0].schema(), None)?));
    }
    let ctx = Arc::new(state.task_ctx());
    let batches = collect_partitioned(physical_inputs[0].clone(), ctx).await?; // runs the cluster job
    self.cache_manager.insert(cn.cache_id().into(), batches.clone());
    Ok(Some(MemorySourceConfig::try_new_exec(&batches, physical_inputs[0].schema(), None)?))
}
// + try_encode_table_provider/try_decode_table_provider doing Arrow IPC for CachedTableProvider
```

**Verdict: reject as the default.** Hard blockers: the gRPC ceiling makes any real dataset fail; mixed `cache JOIN distributed_source` queries cannot be encoded for executors; `CacheFactory::create` is synchronous so eager materialization cannot live there; the `LocalRun` change and the optimizer/planner phase-ordering are invasive. Useful only as a narrow "small broadcast" special case — which the hybrid (section 8) folds in deliberately.

---

### 6.2 Option B — Executor-resident cache reusing the shuffle / `PartitionLocation` machinery (recommended)

**Idea.** Finish the existing scaffolding by adding the one missing planner and a thin materialization/serving layer built entirely from machinery Ballista already has. On the first execution the scheduler materializes the cached subplan as a **pinned stage** whose output partitions stay resident on executors as Arrow IPC files (exactly like shuffle output, but not garbage-collected). The scheduler records `cache_id -> Vec<Vec<PartitionLocation>>` in a cross-job **cache registry**. Any later query referencing the same `cache_id` is planned so the cached subtree becomes a `ShuffleReaderExec` over those `PartitionLocation`s — only metadata crosses the wire, data is fetched on demand via Arrow Flight.

**What is reused unchanged.** `BallistaCacheFactory`, `BallistaCacheNode`, the logical codec, the config gate, the Arrow IPC write path (`ShuffleWriterExec`), `ShuffleReaderExec` and its full physical-codec support ([serde/mod.rs:441-499](ballista/core/src/serde/mod.rs#L441-L499), [serde/mod.rs:625-667](ballista/core/src/serde/mod.rs#L625-L667)), `PartitionLocation` and its proto round-trip, and the local/remote read split ([shuffle_reader.rs:619-716](ballista/core/src/execution_plans/shuffle_reader.rs#L619-L716)).

**What is new/changed.**

| Component | New/Modified | Target |
|---|---|---|
| `BallistaCacheWriterExec` (writes `work_dir/_cache_{cache_id}/{stage_id}/{partition}/data.arrow`, implements the `ShuffleWriter` trait) | new | `ballista/core/src/execution_plans/cache_writer.rs` |
| `BallistaCacheWriterExecNode` proto + oneof arm | new | [ballista.proto](ballista/core/proto/ballista.proto) |
| physical codec encode/decode arm | new | [serde/mod.rs:538](ballista/core/src/serde/mod.rs#L538), [serde/mod.rs:364](ballista/core/src/serde/mod.rs#L364) |
| `CacheRegistry: Arc<DashMap<String, Vec<Vec<PartitionLocation>>>>` | new | `ballista/scheduler/src/state/mod.rs` |
| `BallistaCacheExtensionPlanner` + `BallistaCacheQueryPlanner` | new | `ballista/scheduler/src/cache_planner.rs` |
| install planner via `override_session_builder` | modified | [scheduler/config.rs](ballista/scheduler/src/config.rs), [cluster/mod.rs](ballista/scheduler/src/cluster/mod.rs) |
| recognize `BallistaCacheWriterExec` as a stage boundary | modified | [scheduler/planner.rs](ballista/scheduler/src/planner.rs) |
| registry population hook on stage completion | modified | [execution_graph.rs:424-468](ballista/scheduler/src/state/execution_graph.rs#L424-L468) |
| cleanup exclusion for `_cache_` dirs | modified | [executor_process.rs:676-714](ballista/executor/src/executor_process.rs#L676-L714) |

**Data flow.**

```
FIRST EXECUTION (miss):
  cache() -> BallistaCacheNode (cache_id)
  collect() -> DistributedQueryExec -> scheduler decodes node
  scheduler create_physical_plan -> BallistaCacheExtensionPlanner.plan_extension:
      registry.get(cache_id) == None
      -> return BallistaCacheWriterExec wrapping physical_inputs[0]
  DistributedPlanner sees BallistaCacheWriterExec as a stage boundary,
      emits a cache-write stage
  executors run BallistaCacheWriterExec: child.execute(p) -> Arrow IPC ->
      work_dir/_cache_{cache_id}/{stage}/{p}/data.arrow
  on stage completion: registry.insert(cache_id, partition_locations)
  downstream stage resolves UnresolvedShuffle -> ShuffleReaderExec -> results to client

SUBSEQUENT QUERY (hit):
  same cache_id reaches the scheduler
  plan_extension: registry.get(cache_id) == Some(locations)
      -> return ShuffleReaderExec over those locations  (input subplan NOT re-run)
  executors read cache files locally or via Arrow Flight; only metadata crossed the wire
```

**Serialization (answering the two questions for B).**
*Would the MemTable be available on all executors?* There is **no `MemTable`**. Cached data lives as Arrow IPC files on the executors that wrote them; any other executor reads them remotely via the existing `ShuffleReaderExec` Arrow Flight path. *Would we serialize the data and send it through the physical codec?* **No.** Only `PartitionLocation` metadata is encoded (inside `ShuffleReaderExecNode`); the Arrow IPC bytes stay on disk and are fetched on demand. The logical codec is unchanged; the physical codec gains only a tiny `BallistaCacheWriterExecNode` (`job_id`, `stage_id`, `cache_id`) — the child is not embedded, mirroring `ShuffleWriterExec`.

**Physical planner.** The `ExtensionPlanner` is installed **on the scheduler** via the session-builder hook and holds the registry by `Arc`:

```rust
async fn plan_extension(&self, _p, node, _li, physical_inputs, _state)
    -> Result<Option<Arc<dyn ExecutionPlan>>>
{
    let Some(cn) = node.as_any().downcast_ref::<BallistaCacheNode>() else { return Ok(None) };
    if let Some(locations) = self.registry.get(cn.cache_id()) {
        let reader = ShuffleReaderExec::try_new(
            0, locations.clone(),
            physical_inputs[0].schema(),
            physical_inputs[0].output_partitioning().clone())?;
        Ok(Some(Arc::new(reader)))                          // HIT: serve, do not recompute
    } else {
        Ok(Some(Arc::new(BallistaCacheWriterExec::new(      // MISS: materialize as a stage
            physical_inputs[0].clone(), cn.cache_id().into(), 0))))
    }
}
```

This is the DataFusion `CacheNodePlanner` pattern from `cache_factory.rs`, but with the in-process `collect_partitioned + MemorySourceConfig` replaced by the distributed `BallistaCacheWriterExec + ShuffleReaderExec` pair. Materialization runs on executors, not on the scheduler — avoiding the "scheduler runs the plan single-node" failure mode.

**Lifecycle / pinning / eviction.** Cache files live under `_cache_`-prefixed directories so neither `remove_job_dir` nor the TTL sweep touches them (the skip must be added *before* the TTL check to avoid a delete race). The registry is in-scheduler-memory; on scheduler restart it is empty and queries re-materialize (orphaned `_cache_` dirs remain on disk until an eviction pass reclaims them). Eviction starts with session-scoped cleanup (the node already carries `session_id`) and can grow to TTL/LRU/`DROP CACHE`.

**Fault tolerance.** Materialization is a normal stage with normal retries; the registry is only populated after all partitions succeed, so partial writes never produce a hit. If an executor holding cached partitions dies, the registry entries pointing at it must be invalidated so the next query re-materializes — analogous to `reset_stages_on_lost_executor`, but extended across jobs (a new hook in `ExecutorManager`).

**Verdict: recommended, with caveats.** It is the only option that is both architecturally consistent with Ballista and free of external infrastructure. Caveats are implementation risks, not architectural ones (see section 7).

---

### 6.3 Option C — Object-store materialization + `ListingTable`

**Idea.** On the first execution, write the cached subplan's output to a **shared object store** (S3/GCS/HDFS or a shared filesystem) as Arrow IPC (or Parquet) files under a `cache_id` path, record the URL in a registry, and rewrite later references to a plain `ListingTable` scan over that path. This is the closest analogue to Spark's `persist(DISK)` / checkpoint.

**Serialization (answering the two questions for C).** No `MemTable`, no `RecordBatch` bytes on the wire. The **read** path is a standard `ListingTable` `TableScan`, fully serializable by the existing DataFusion codec ([datafusion/proto/src/logical_plan/mod.rs:1040-1153](/Users/jarrovgit/Development/datafusion/datafusion/proto/src/logical_plan/mod.rs)); the executors each open the object store independently. The only new physical-codec entry is the write node (metadata: `cache_id`, schema, partition count, URL prefix). What crosses the wire is a **URL**, not data.

**Data flow.**

```
miss: ExtensionPlanner returns BallistaCacheWriterExec(url_prefix)
      executors write {url_prefix}/{cache_id}/part-{p}.arrow to the shared store
      on completion: registry.insert(cache_id, CacheEntry{url_prefix, schema, ...})
hit:  ExtensionPlanner builds ListingTable over {url_prefix}/{cache_id}/ and plans a scan
      every executor reads its partition file directly from the shared store
```

**Pros.** Survives executor death and (with a startup reconciliation scan) scheduler restart; readable by external tools; gets Parquet column/predicate pushdown if Parquet is used; no single-executor read bottleneck.

**Cons / blockers.** Requires a shared object store configured identically on every node — the default Ballista deployment has none (`CustomObjectStoreRegistry` is opt-in, [object_store.rs](ballista/core/src/object_store.rs)). The "first `collect()` must both write the cache *and* return data" requirement forces a tee/dual-root plan that the single-root `ShuffleWriter` assumption in the distributed planner does not natively support. Object-store write latency dominates first materialization. Mismatched URLs across nodes fail silently at execute time.

**Verdict: conditional.** The right choice for cloud deployments where object storage is already the data layer and durability matters; strictly inferior to Option B for the default, infrastructure-free deployment.

---

## 7. Comparison and adversarial review

| Dimension | A: client MemTable broadcast | B: executor-resident shuffle cache | C: object-store + ListingTable |
|---|---|---|---|
| Where cached data lives | client RAM | executor local disk (Arrow IPC) | shared object store |
| What crosses the wire | full IPC bytes, every query (logical codec) | `PartitionLocation` metadata only (physical codec) | a URL only |
| Available to all executors? | only by re-transmitting; breaks for mixed queries | yes, via Arrow Flight on demand | yes, all read the store |
| New external dependency | none | none | shared object store (required) |
| Size ceiling | gRPC msg limit (~16 MiB) | cluster disk | object-store capacity |
| Survives executor death | n/a (data on client) | no (re-materialize on miss) | yes |
| Survives scheduler restart | yes (client holds it) | no (registry lost) | with reconciliation scan |
| Reuses existing machinery | logical codec only | shuffle write + read + codec + PartitionLocation | object store registry + ListingTable codec |
| Implementation size | medium (but architecturally blocked) | medium-large (8-12 dev-days) | large (dual-write + infra) |
| Verdict | reject as default | recommended | cloud-only |

### Critical risks surfaced by review (verified against source)

- **`CacheFactory::create` is synchronous** (`datafusion/core/src/execution/session_state.rs:2139-2146`). Any design that tries to materialize *inside* `create` (a temptation in Option A) is architecturally infeasible without changing upstream DataFusion. Defer materialization to the async `ExtensionPlanner::plan_extension`. Option B does this correctly.
- **The distributed planner dispatches on concrete types, not the `ShuffleWriter` trait.** `DefaultDistributedPlanner::plan_query_stages_internal` ([scheduler/planner.rs](ballista/scheduler/src/planner.rs)) recognizes stage boundaries via explicit `downcast_ref` to `HashJoinExec` (line ~142), `CoalescePartitionsExec` (line ~194), `SortPreservingMergeExec` (line ~216), and `RepartitionExec` (line ~232). It has **no generic "implements `ShuffleWriter`" arm.** A new `BallistaCacheWriterExec` will be wrapped in an outer terminal `ShuffleWriterExec` (a broken double-writer nesting) unless an explicit downcast arm is added. **This is the single most important correctness step for Option B (Phase 3 below).**
- **gRPC message ceiling (Option A).** `BALLISTA_CLIENT_GRPC_MAX_MESSAGE_SIZE` defaults to ~16 MiB ([config.rs:132-135](ballista/core/src/config.rs#L132-L135)); embedding cache bytes per query also amplifies network cost as O(size x queries x executors).
- **Cache-id freshness.** `Uuid::new_v4()` is minted on every `cache()` call ([extension.rs:914](ballista/core/src/extension.rs#L914)). Two `cache()` calls on identical subplans get different ids and never dedupe. This is by-design (each call is a snapshot) but must be documented; otherwise users in iterative loops will report "I cached but the second run is still slow."
- **Cleanup race (Option B).** The `_cache_` skip predicate must run *before* `satisfy_dir_ttl` in `clean_shuffle_data_loop` ([executor_process.rs:676-714](ballista/executor/src/executor_process.rs#L676-L714)), not after.
- **`job_id` overloading (Option B).** Using `job_id = "_cache_{cache_id}"` as a path sentinel via `create_shuffle_path` overloads the field and may confuse tooling that parses job ids. A cleaner long-term fix is an optional `raw_path: Option<String>` on `PartitionLocation` (a proto change).
- **No persistence.** `ClusterStorage::Memory` is the only backend ([cluster/mod.rs:53-55](ballista/scheduler/src/cluster/mod.rs#L53-L55)); any scheduler-side cache registry is lost on restart. Acceptable for an MVP (re-materialize on miss), but production needs a reconciliation/persistence story.
- **Cache staleness (all options).** None of these detect that the underlying source changed. A cached Parquet result stays "valid" even after the files are overwritten. Document this and consider a TTL or an explicit invalidation API.
- **Partition-count drift.** If `datafusion.execution.target_partitions` changes between the materializing query and a later read, the cached `PartitionLocation` count no longer matches the new query's expectations; the planner should validate or re-materialize.

---

## 8. Recommendation and a size-adaptive hybrid

**Recommendation: Option B** as the default distributed cache. It reuses Ballista's proven Arrow IPC write path, `PartitionLocation` addressing, the `ShuffleReaderExec` local/remote read logic, and the existing physical codec for reads. It introduces no external dependency, keeps `RecordBatch` data off the wire, and finishes the scaffolding the project already started. Its risks are implementation complexity (the distributed-planner stage-boundary arm, the registry-population hook, the cleanup exclusion), all solvable inside the codebase.

**Hybrid (size-adaptive), enabled later.** Because `BallistaCacheFactory` is a pluggable `CacheFactory`, a policy can route by estimated result size without changing the logical node:

- In `BallistaCacheExtensionPlanner` (scheduler), inspect `physical_inputs[0].statistics()`.
- If `total_byte_size <= BALLISTA_CACHE_BROADCAST_THRESHOLD_BYTES` (new config, default 0 = disabled): use a small-broadcast path (collect on a resolved upstream and serve a `MemorySourceConfig` from scheduler memory — Option A's good case, bounded by the threshold).
- Otherwise: the Option B executor-resident path.
- If statistics are unknown, default to Option B (never risk OOMing the scheduler).

Object-store materialization (Option C) becomes a third, deployment-selected backend for cloud setups that need durability across restarts — same `BallistaCacheNode`, different `ExtensionPlanner` branch chosen by config.

---

## 9. Phased implementation plan

- **Phase 0 - MVP: make `noop=false` not error (1-2 days).** Add a minimal scheduler-side `BallistaCacheExtensionPlanner` that downcasts `BallistaCacheNode` and returns `Ok(Some(physical_inputs[0].clone()))` (pass-through, no caching). Install it via `override_session_builder`. This yields `noop=true` semantics without the planner crash. Update [`should_support_on_cache_collect`](ballista/client/tests/context_unsupported.rs#L107-L134) to assert success + correct rows. No proto/executor changes.
- **Phase 1 - `BallistaCacheWriterExec` + proto + codec (3-4 days).** Model on `ShuffleWriterExec`; write to `work_dir/_cache_{cache_id}/...` with the same IPC + LZ4_FRAME path. Add `BallistaCacheWriterExecNode` to the proto and a oneof arm; implement encode/decode in `BallistaPhysicalExtensionCodec`; regenerate bindings; add codec round-trip tests.
- **Phase 2 - `CacheRegistry` + population hook (2-3 days).** Add `Arc<DashMap<String, Vec<Vec<PartitionLocation>>>>` to `SchedulerState`; thread it into the session builder; populate it after a cache-write stage completes ([execution_graph.rs:424-468](ballista/scheduler/src/state/execution_graph.rs#L424-L468)); extend the planner's hit branch to return a `ShuffleReaderExec` over the stored locations.
- **Phase 3 - distributed-planner stage boundary (2 days, critical).** Add a `downcast_ref::<BallistaCacheWriterExec>()` arm in `plan_query_stages_internal` analogous to the `CoalescePartitionsExec` arm, so the writer becomes a real stage boundary and its child subtree decomposes correctly (including when the cached subplan itself contains shuffles).
- **Phase 4 - cleanup exclusion (0.5 day).** Skip `_cache_`-prefixed directories in `clean_shuffle_data_loop` *before* the TTL check, and confirm `remove_job_dir` never matches them.
- **Phase 5 - integration tests (2 days).** End-to-end: cache a real table, verify the first `collect()` returns correct rows and the second does **not** re-run the source (observe via metrics / a source-scan counter); test executor-death -> miss -> re-materialize.
- **Phase 6 - eviction & lifecycle (1-2 days).** Session-scoped eviction on `remove_session` using the node's `session_id`; `BALLISTA_CACHE_MAX_ENTRIES`; optional TTL/`DROP CACHE`.
- **Phase 7 - size-adaptive hybrid (optional).** Add the small-broadcast path gated by `BALLISTA_CACHE_BROADCAST_THRESHOLD_BYTES` (default 0).

---

## 10. Key file reference map

| Concern | File |
|---|---|
| DataFusion `cache()` + `CacheFactory` | `datafusion/core/src/dataframe/mod.rs:2387`, `.../session_state.rs:2139` |
| DataFusion lazy-cache example (the pattern to adapt) | `datafusion-examples/examples/dataframe/cache_factory.rs` |
| Ballista session build + cache factory wiring | [extension.rs:287-355](ballista/core/src/extension.rs#L287-L355) |
| `BallistaCacheFactory` / `BallistaCacheNode` | [extension.rs:894-991](ballista/core/src/extension.rs#L894-L991) |
| `ballista.cache.noop` config | [config.rs:116-119](ballista/core/src/config.rs#L116-L119), [:374-377](ballista/core/src/config.rs#L374-L377) |
| Logical codec for the cache node | [serde/mod.rs:233-310](ballista/core/src/serde/mod.rs#L233-L310) |
| Physical codec (shuffle nodes; else-branch errors) | [serde/mod.rs:363-699](ballista/core/src/serde/mod.rs#L363-L699) |
| Cache node proto | [ballista.proto:32-42](ballista/core/proto/ballista.proto#L32-L42) |
| Query interception (client) | [planner.rs:104-200](ballista/core/src/planner.rs#L104-L200) |
| Distributed query dispatch | [distributed_query.rs](ballista/core/src/execution_plans/distributed_query.rs) |
| `PartitionLocation` | [serde/scheduler/mod.rs:83-111](ballista/core/src/serde/scheduler/mod.rs#L83-L111) |
| Shuffle write / read (the reuse target) | [shuffle_writer.rs](ballista/core/src/execution_plans/shuffle_writer.rs), [shuffle_reader.rs:619-716](ballista/core/src/execution_plans/shuffle_reader.rs#L619-L716) |
| Scheduler distributed planner (stage boundaries) | [scheduler/planner.rs](ballista/scheduler/src/planner.rs) |
| Scheduler state / cleanup | [scheduler/state/mod.rs:449](ballista/scheduler/src/state/mod.rs#L449), [execution_graph.rs:424-468](ballista/scheduler/src/state/execution_graph.rs#L424-L468) |
| Executor cleanup loop (pinning target) | [executor_process.rs:676-714](ballista/executor/src/executor_process.rs#L676-L714) |
| The failing test that documents the gap | [context_unsupported.rs:107-134](ballista/client/tests/context_unsupported.rs#L107-L134) |

---

## 11. Glossary

- **`CacheFactory`** - DataFusion hook (sync `create`) that rewrites a `cache()` plan into a custom logical plan; the lazy alternative to the eager `MemTable`.
- **`BallistaCacheNode`** - Ballista's `UserDefinedLogicalNode` wrapper carrying `cache_id` + `session_id`; already serializable, not yet physically plannable.
- **`ExtensionPlanner`** - DataFusion trait (`async plan_extension`) that converts a custom logical node into an `ExecutionPlan`; the missing piece for Ballista's cache node.
- **`PartitionLocation`** - addressing record (`job/stage/partition`, executor host:port, file id) for a materialized partition; the metadata that lets `ShuffleReaderExec` fetch data without moving it through the codec.
- **`ShuffleWriterExec` / `ShuffleReaderExec`** - Ballista's mechanism for persisting a stage's output to executor-local Arrow IPC files and reading it back locally or via Arrow Flight; the substrate Option B reuses for an executor-resident cache.
- **Logical vs physical codec** - logical (`BallistaLogicalExtensionCodec`) encodes plans client->scheduler; physical (`BallistaPhysicalExtensionCodec`) encodes per-stage plans scheduler->executors. Neither moves `RecordBatch` data; only Option A would push bytes through the logical codec.
