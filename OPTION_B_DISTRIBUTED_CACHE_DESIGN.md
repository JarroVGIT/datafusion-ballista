# Option B: Executor-Resident `DataFrame.cache()` — Detailed Design

Companion to [DATAFRAME_CACHE_ANALYSIS.md](DATAFRAME_CACHE_ANALYSIS.md). That document compares three strategies and recommends Option B. This document specifies Option B in enough detail to (a) split it into incremental PRs that each stand on their own and do not regress Ballista, and (b) explain executor-failure handling and cache reconciliation. It favors call chains, data-structure ownership, and component roles over code.

---

## 1. Goal and invariants

Finish the existing cache scaffolding so that, with `ballista.cache.noop = false`, `DataFrame.cache()` materializes a subplan once as a **pinned, executor-resident dataset** and serves it to later queries without recomputation — reusing Ballista's shuffle write/read machinery so no `RecordBatch` ever crosses the codec boundary.

Invariants every PR must preserve:

- **I1 — Default is unchanged.** `ballista.cache.noop` stays `true` by default ([config.rs:116-119](ballista/core/src/config.rs#L116-L119)); a stock cluster behaves exactly as today until a user opts in.
- **I2 — Dormant-until-wired.** New execution nodes / hooks are unreachable until the planner is taught to emit them. Adding them changes no existing path.
- **I3 — Cache files are addressable by the existing serve path.** A cached partition is fetched by the same `do_get` / `ShuffleReaderExec` code that fetches shuffle partitions.
- **I4 — A cache miss is always safe.** If the registry has no (or a stale) entry, the query re-materializes from source. Correctness never depends on the cache being present.

I4 is the backbone of fault tolerance: every failure mode degrades to "miss → recompute."

---

## 2. Component roles

| Component | Owns | Role in caching |
|---|---|---|
| **Client** | the `BallistaCacheNode` inside its DataFrame's `LogicalPlan` (carries `cache_id`, `session_id`) | Mints the `cache_id` (factory, [extension.rs:903-921](ballista/core/src/extension.rs#L903-L921)); submits the plan; fetches results. Holds no cache data. |
| **Scheduler** | the **CacheRegistry** (new) and the per-session cache-aware `QueryPlanner` | The brain. Decides hit vs miss, drives materialization as a pinned stage, records *where* each cached partition lives, reconciles on executor loss, governs eviction. Holds metadata only. |
| **Executor** | the cached Arrow IPC files under `work_dir/_cache_<cache_id>/...` | The muscle. Materializes (writes IPC), serves (Arrow Flight), deletes on command. Stateless about cache *validity* — it just holds and serves bytes. |

The cache_id is the single durable identity. It is minted on the client, rides through the logical codec ([serde/mod.rs:233-289](ballista/core/src/serde/mod.rs#L233-L289)), and keys everything on the scheduler.

---

## 3. Data structures and where they live

### 3.1 New: `CacheRegistry` (scheduler, in-memory)

Owned by `SchedulerState` ([state/mod.rs:111-122](ballista/scheduler/src/state/mod.rs#L111-L122)) as `cache_registry: Arc<CacheRegistry>`, cloned into each session's planner and into the task-completion path.

```
CacheRegistry {
    entries:     DashMap<String /*cache_id*/, CacheEntry>,
    by_executor: DashMap<String /*executor_id*/, HashSet<String /*cache_id*/>>,  // reverse index for invalidation
}
CacheEntry {
    locations:       Vec<Vec<PartitionLocation>>,  // [output_partition][replica/source] — same shape ShuffleReaderExec wants
    schema:          SchemaRef,
    session_id:      String,
    partition_count: usize,
    created_at:      u64,
}
```

Why `Vec<Vec<PartitionLocation>>`: that is exactly the shape `StageOutput::partition_locations` flattens into ([execution_stage.rs ~1061]) and that `ShuffleReaderExec::try_new` consumes, so a hit can hand it straight to a reader.

Why the `by_executor` reverse index: executor-loss reconciliation (section 6) must find affected entries in O(1) without scanning every cache.

The registry is **cross-job**. It deliberately lives outside any `ExecutionGraph`, because the job that materialized a cache is gone from `active_job_cache` long before later queries read it.

### 3.2 New: `BallistaCacheWriterExec` (core execution plan)

A sibling of `ShuffleWriterExec` that writes its input to the pinned cache directory instead of the job directory. It **implements the `ShuffleWriter` trait** ([shuffle_writer_trait.rs:31-49](ballista/core/src/execution_plans/shuffle_writer_trait.rs)) so the distributed planner and stage builder treat it uniformly.

```
BallistaCacheWriterExec {
    cache_id: String,
    stage_id: usize,
    input:    Arc<dyn ExecutionPlan>,
    ...metrics, properties...
}
impl ShuffleWriter:
    job_id()                     -> "_cache_<cache_id>"   // sentinel; drives the file path (see 5.3)
    stage_id()                   -> self.stage_id
    shuffle_output_partitioning()-> None                  // identity; one output file per input partition
```

Serialized by a new `BallistaCacheWriterExecNode { cache_id, stage_id }` oneof arm in [ballista.proto](ballista/core/proto/ballista.proto) and a matching encode/decode arm in `BallistaPhysicalExtensionCodec` ([serde/mod.rs:538-698](ballista/core/src/serde/mod.rs#L538-L698)). The child input is **not** embedded in the proto (same as `ShuffleWriterExec`); it is reattached from `inputs[0]` on decode.

### 3.3 Unchanged but reused

- `BallistaCacheFactory`, `BallistaCacheNode`, the logical codec, `ballista.cache.noop` — all already present.
- `PartitionLocation` ([serde/scheduler/mod.rs:83-111](ballista/core/src/serde/scheduler/mod.rs#L83-L111)) — addresses a partition by `PartitionId(job_id, stage_id, partition_id)` + `executor_meta` + `file_id`; `path(work_dir)` reconstructs the file path via `create_shuffle_path`.
- `ShuffleReaderExec`, `BallistaFlightService::do_get`, `partition_to_location`, the stage state machine — all reused verbatim.

---

## 4. Call chain: first materialization (cache miss)

Pre-state: client called `df.cache()` with `noop=false`, so the DataFrame's plan root is `BallistaCacheNode(subplan)` ([extension.rs:912-918](ballista/core/src/extension.rs#L912-L918)).

```
CLIENT
  DistributedQueryExec::execute                         distributed_query.rs:214
    -> serialize LogicalPlan (BallistaCacheNode round-trips via logical codec)
    -> ExecuteQuery gRPC to scheduler

SCHEDULER (ingest + plan)
  SchedulerGrpc::execute_query -> SchedulerServer::submit_job   grpc.rs:454 / mod.rs:221
    -> event JobQueued -> SchedulerState::submit_job            query_stage_scheduler.rs / state/mod.rs:378
    -> session_ctx.state().create_physical_plan(plan)           state/mod.rs:449
         -> BallistaCacheExtensionPlanner::plan_extension        (NEW, registered via session builder)
              registry.get(cache_id) == None  ->  MISS
              returns BallistaCacheWriterExec( physical(subplan) )
    -> TaskManager::submit_job -> StaticExecutionGraph::new      task_manager.rs:277 / execution_graph.rs:301
         -> DistributedPlanner::plan_query_stages_internal       planner.rs:126
              NEW downcast arm: sees BallistaCacheWriterExec ->
                emits it as a pinned stage boundary,
                replaces it upward with UnresolvedShuffleExec
         -> ExecutionStageBuilder builds stages map
    -> graph stored in TaskManager.active_job_cache (job_id -> JobInfoCache)   task_manager.rs:131

SCHEDULER (schedule) -> EXECUTOR (materialize)
  revive_offers -> LaunchMultiTask to chosen executors        state/mod.rs:190
  ExecutorServer::launch_multi_task -> run_task               executor_server.rs:837 / 359
    -> create_query_stage_exec                                 execution_engine.rs:106
         NEW downcast arm for BallistaCacheWriterExec
    -> plan.execute(partition): writes Arrow IPC to
         work_dir/_cache_<cache_id>/<stage>/<partition>/data.arrow   (NEW create_cache_path; see 5.3)
    -> reports ShuffleWritePartition list via UpdateTaskStatus

SCHEDULER (record outputs)
  SchedulerState::update_task_statuses                        state/mod.rs:363
    -> ExecutionGraph::update_task_status (SuccessfulTask)     execution_graph.rs:~896
         -> partition_to_location builds PartitionLocation[]   execution_graph.rs:1726
              (executor_meta from executor_manager; job_id stamped to "_cache_<cache_id>")
         -> NEW hook: if stage.plan is BallistaCacheWriterExec
              registry.insert(cache_id, locations as Vec<Vec<_>>)
              by_executor index updated
         -> update_stage_output_links routes the SAME locations  execution_graph.rs:424
              to the parent stage's inputs (or output_locations if final)
              => the current query proceeds and returns data normally
```

Net: the cache stage behaves like an ordinary shuffle-write stage for *this* query (its output feeds the parent / the client), and additionally its locations are copied into the registry for *future* queries. No special "return the data" path is needed; the existing final-stage / `ShuffleReaderExec` path already returns it.

For a bare `cache().collect()` (the cache node is the root), `plan_query_stages` still appends the usual terminal gather stage, so there is one extra shuffle hop on the first call only. Eliminating it (let a root cache stage be terminal) is an optional later optimization, not required for correctness.

---

## 5. Call chain: cache hit, and the addressing decision

### 5.1 Hit

```
SCHEDULER
  create_physical_plan -> BallistaCacheExtensionPlanner::plan_extension
     registry.get(cache_id) == Some(entry)  ->  HIT
     return ShuffleReaderExec::try_new(entry.locations, entry.schema, partitioning)
     // the subplan under BallistaCacheNode is discarded; nothing recomputes
  plan_query_stages -> the reader is a leaf; downstream stages consume it
EXECUTOR
  ShuffleReaderExec reads cache files: local (IPC file) or remote (Arrow Flight do_get)
     -> do_get reconstructs the path from (job_id="_cache_<cache_id>", stage_id, partition_id, file_id)
```

Only `PartitionLocation` metadata crossed the wire. The cached bytes stayed on executor disks.

### 5.2 Tolerating update ordering

`update_task_status` iterates stages in non-deterministic `HashMap` order, so a consuming stage can be processed in the same batch as the cache-write stage. The hit lookup therefore happens at **`resolve_stage` time**, not at submit time, so a registry entry written microseconds earlier is visible. Within a single query the cache stage and its consumer live in the same `ExecutionGraph` and are linked by `output_links`, so the normal resolution path already orders them; cross-query reads always see a fully-populated entry because the registry is only written after the stage is `Successful`.

### 5.3 Path addressing (design decision)

A cached partition must be (a) written, (b) recorded as a `PartitionLocation`, and (c) reconstructable by `do_get` — without colliding with shuffle data or being garbage-collected with the job. `ShuffleWritePartition` carries no path field; paths are always rebuilt from `(work_dir, job_id, stage_id, partition_id, file_id)` via `create_shuffle_path` ([execution_plans/mod.rs:66-99](ballista/core/src/execution_plans/mod.rs#L66-L99)).

**Recommended (minimal): sentinel `job_id`.** The cache writer's `job_id()` returns `"_cache_<cache_id>"`. `create_shuffle_path` then yields `work_dir/_cache_<cache_id>/<stage>/<partition>/data.arrow`. The completion hook stamps the same sentinel into the cache `PartitionLocation`s. Result: `create_shuffle_path`, `do_get`, and `ShuffleReaderExec` work **unchanged**; the only cleanup change is excluding `_cache_*` top-level dirs (section 7). Trade-off: the `job_id` field now sometimes holds a non-job sentinel, which any tooling that parses `job_id` must tolerate.

**Alternative (cleaner, more invasive):** add an explicit `cache_id: Option<String>` to `PartitionLocation` (+ proto), a `create_cache_path(work_dir, cache_id, partition)`, and a dedicated `Action::FetchCachePartition` arm in `do_get`. No `job_id` overloading, but it touches the proto, the Flight action enum, and the reader. 

Recommendation: ship the sentinel form first (smallest, fully reuses the serve path); migrate to explicit addressing only if `job_id` overloading proves problematic in metrics/UI.

---

## 6. Executor failure and cache reconciliation

### 6.1 Why the existing path does not cover caches

Executor loss today flows:

```
expire_dead_executors (heartbeat timeout)        scheduler_server/mod.rs:280
  -> remove_executor -> event ExecutorLost        scheduler_server/mod.rs:341
  -> TaskManager::executor_lost                    task_manager.rs:591
       iterates active_job_cache ONLY (live jobs)
       -> ExecutionGraph::reset_stages_on_lost_executor   execution_graph.rs:1211
            reset_stages_internal filters each StageOutput's locations
            by executor_meta.id, sets StageOutput.complete=false,
            rolls affected stages back to UnResolved (re-run)   execution_graph.rs:498
```

This only repairs **stages of currently-active jobs**. A cache materialized by a job that already finished is not in `active_job_cache`, so its registry entry is never touched. A later hit would hand a `ShuffleReaderExec` `PartitionLocation`s pointing at a dead executor and fail at Flight-fetch time.

### 6.2 The added hook

Add cache invalidation alongside the existing `ExecutorLost` handling (in the event handler at [query_stage_scheduler.rs:314-333](ballista/scheduler/src/scheduler_server/query_stage_scheduler.rs#L314-L333), or right after `task_manager.executor_lost`):

```
on ExecutorLost(executor_id):
  for cache_id in registry.by_executor.remove(executor_id):
      registry.entries.remove(cache_id)        // drop the whole entry (simplest, correct)
```

Dropping the entry turns the next reference into a **miss → re-materialize** (invariant I4). This is the same recompute-on-loss guarantee Ballista already gives shuffle data, lifted to the cross-job registry.

Refinement (optional): instead of dropping the whole entry, remove only the dead executor's `PartitionLocation`s and mark the entry partial; a hit on a partial entry re-materializes. Whole-entry drop is recommended first for simplicity.

### 6.3 Scheduler restart

The registry is in-memory (consistent with `ClusterStorage::Memory` being the only backend). On restart it is empty, so every cache reference is a miss and re-materializes — correct, just cold. The Arrow IPC files from before the restart are now **orphans** (no entry points at them) and, because cleanup excludes `_cache_*`, they would never be reclaimed. Therefore on scheduler startup, broadcast a one-time **purge of all `_cache_*` directories** to every executor (reuse the cache-delete RPC from section 7). Simple, bounded, and safe given I4.

### 6.4 Stale executor metadata

`PartitionLocation.executor_meta` is captured at task-completion time from `executor_manager.get_executor_metadata` ([state/mod.rs:369](ballista/scheduler/src/state/mod.rs#L369)), not from the task status. If an executor restarts on a new host/port, cached locations are stale. This is identical to existing shuffle behavior; with the section 6.2 hook, the executor-loss event invalidates the entry first, so the stale-address window is closed by reconciliation rather than special-cased.

---

## 7. Lifecycle, pinning, and eviction

Two cleanup paths would otherwise destroy cache files; both are job-keyed:

| Path | Trigger | What it deletes | Cache treatment |
|---|---|---|---|
| `clean_up_successful_job` -> `clean_up_job_data_delayed` -> `remove_job_dir` | job completes | `work_dir/<job_id>` | Cache lives under `work_dir/_cache_<cache_id>`, a different top-level dir, so the materializing job's cleanup never matches it. No change needed. |
| `clean_shuffle_data_loop` (TTL sweep) | every interval | any `work_dir` child older than `job_data_ttl` | **Would delete `_cache_*` by age.** Add a name-prefix skip for `_cache_` **before** the TTL check ([executor_process.rs:676-714](ballista/executor/src/executor_process.rs#L676-L714)). |

Pinning is therefore: cache files live in their own `_cache_` namespace, excluded from the TTL sweep. They are removed **only** by explicit instruction.

Explicit eviction uses a new `RemoveCacheDataParams { cache_id }` gRPC + executor handler (sibling of `remove_job_data`, [executor_server.rs:921-932](ballista/executor/src/executor_server.rs#L921-L932)) that deletes `work_dir/_cache_<cache_id>`. The scheduler invokes it when an entry leaves the registry:

- **Session end** — on `remove_session`, evict entries whose `CacheEntry.session_id` matches (the node already carries `session_id`).
- **Capacity / TTL** — optional `BALLISTA_CACHE_MAX_ENTRIES` / cache TTL with an LRU or age policy.
- **Executor loss** — section 6.2 (the executor is gone, so the delete RPC targets surviving replicas only; the dead executor's files are reclaimed by its own startup or operator cleanup).
- **Scheduler restart** — section 6.3 purge.

---

## 8. Incremental PR plan

Each PR is independently mergeable and non-regressing. I1 (default `noop=true`) and I2 (dormant-until-wired) mean nothing changes for existing users until a user sets `noop=false`, and even then only the steps that are merged take effect.

| PR | Title | Changes (call chains / structs) | Why it does not regress | Test |
|---|---|---|---|---|
| **1** | Planner injection + pass-through | Install a cache-aware `QueryPlanner` on the scheduler via `override_session_builder` ([config.rs:243](ballista/scheduler/src/config.rs#L243), wired in [cluster/mod.rs:107-126](ballista/scheduler/src/cluster/mod.rs#L107-L126) / [memory.rs:428-441](ballista/scheduler/src/cluster/memory.rs#L428-L441)). `BallistaCacheExtensionPlanner` initially just unwraps `BallistaCacheNode` to its input (no caching). Add the empty `Arc<CacheRegistry>` field to `SchedulerState` so later PRs share the handle. | Default `noop=true` path untouched. The only change is that the previously-erroring `noop=false` plan now succeeds with pass-through semantics. | Flip [`should_support_on_cache_collect`](ballista/client/tests/context_unsupported.rs#L107-L134) to assert success + correct rows. |
| **2** | Cache writer node (dormant) | Add `BallistaCacheWriterExec` (impl `ShuffleWriter` + `ExecutionPlan`, `with_new_children` preserving `cache_id`); proto `BallistaCacheWriterExecNode`; physical codec encode/decode arm; `create_cache_path`; executor `create_query_stage_exec` arm ([execution_engine.rs:106](ballista/executor/src/execution_engine.rs#L106)); `get_stage_partitions` arm ([execution_stage.rs:1006]). | Nothing emits this node yet (planner is still pass-through), so all new arms are unreachable. | Codec round-trip unit test; direct executor write-then-read of one partition. |
| **3** | Materialize on miss (write path live) | DistributedPlanner downcast arm so `BallistaCacheWriterExec` becomes a pinned stage boundary ([planner.rs:126-263](ballista/scheduler/src/planner.rs#L126-L263)); planner miss branch returns the writer; completion hook in `update_task_status` ([execution_graph.rs:~896-943](ballista/scheduler/src/state/execution_graph.rs)) populates the registry (+ `by_executor`). | Gated by `noop=false` (opt-in). With it on, queries still return correct results; they just always materialize (no hit path yet). | Integration: `noop=false`, cache + collect, assert correct rows and that `_cache_` files exist. |
| **4** | Serve on hit (read path live) | Planner hit branch returns a `ShuffleReaderExec` over `entry.locations`; subplan discarded. | Additive; a miss still re-materializes (I4). | Integration: second collect of the same cached DataFrame returns identical rows **without** re-scanning source (assert via source-scan metric / counter). |
| **5** | Pinning + eviction | `_cache_` exclusion in `clean_shuffle_data_loop`; `RemoveCacheDataParams` gRPC + executor handler; registry-driven eviction on session end / capacity. | Touches only `_cache_` dirs and a new RPC. | Test that a cache survives a TTL interval and that session end deletes it. |
| **6** | Failure reconciliation + restart purge | `ExecutorLost` hook invalidates registry entries via `by_executor`; startup broadcast purges `_cache_*`. | Without it a post-loss hit would fail; with it that hit becomes a miss. Pure robustness gain. | Integration: kill an executor holding cache partitions, assert next query re-materializes and succeeds. |
| **7** (opt.) | Size-adaptive + default | Optional small-result broadcast path; optionally revisit the `noop` default (likely keep opt-in); docs. | Opt-in. | — |

PR ordering rationale: 1 removes the hard failure and lands the injection seam; 2 adds dormant execution capability; 3 turns on writing; 4 turns on reading; 5 makes caches durable for their intended lifetime; 6 makes them safe under failure. A cluster is correct (if not yet useful) after any prefix of this list.

---

## 9. Key integration points

| Concern | Location | Action |
|---|---|---|
| Register the ExtensionPlanner | session builder via [config.rs:243](ballista/scheduler/src/config.rs#L243), [memory.rs:428-441](ballista/scheduler/src/cluster/memory.rs#L428-L441) | install cache-aware `QueryPlanner` carrying `Arc<CacheRegistry>` |
| Hit/miss decision | `plan_extension` (new) reached from [state/mod.rs:449](ballista/scheduler/src/state/mod.rs#L449) | miss → `BallistaCacheWriterExec`; hit → `ShuffleReaderExec` |
| Stage boundary recognition | [planner.rs:126-263](ballista/scheduler/src/planner.rs#L126-L263) (arms at 142/194/214/231) | add `BallistaCacheWriterExec` else-if before the generic child loop |
| Task count for the writer stage | `get_stage_partitions` [execution_stage.rs:1006] | add arm reading input partition count |
| Executor execution | `create_query_stage_exec` [execution_engine.rs:106-168](ballista/executor/src/execution_engine.rs#L106) | add `BallistaCacheWriterExec` arm |
| File path | `create_shuffle_path` [execution_plans/mod.rs:66-99](ballista/core/src/execution_plans/mod.rs#L66-L99) | unchanged (sentinel `job_id`) or new `create_cache_path` |
| Record locations | completion hook near `partition_to_location` [execution_graph.rs:1726](ballista/scheduler/src/state/execution_graph.rs#L1726) + `update_stage_output_links` [:424](ballista/scheduler/src/state/execution_graph.rs#L424) | insert into `CacheRegistry`; also route to parent/output as usual |
| Registry ownership | `SchedulerState` [state/mod.rs:111-122](ballista/scheduler/src/state/mod.rs#L111-L122) | new `cache_registry` field |
| Pinning | `clean_shuffle_data_loop` [executor_process.rs:676-714](ballista/executor/src/executor_process.rs#L676-L714) | skip `_cache_*` before TTL check |
| Explicit delete | new RPC + handler near `remove_job_data` [executor_server.rs:921-932](ballista/executor/src/executor_server.rs#L921-L932) | delete `work_dir/_cache_<cache_id>` |
| Failure invalidation | `ExecutorLost` handling [query_stage_scheduler.rs:314-333](ballista/scheduler/src/scheduler_server/query_stage_scheduler.rs#L314-L333) | drop registry entries via `by_executor` |

---

## 10. Open design decisions

1. **Addressing:** sentinel `job_id` (minimal, reuses serve path; overloads `job_id`) vs. explicit `cache_id` on `PartitionLocation` + a Flight action (clean; proto + reader churn). Recommend sentinel first. (Section 5.3.)
2. **First-collect extra stage:** accept one extra gather hop when the cache node is the query root, or special-case a terminal cache stage in `plan_query_stages`. Recommend accept-first, optimize later.
3. **Loss granularity:** drop the whole entry vs. partial-entry invalidation on executor loss. Recommend whole-entry first.
4. **Cache-id dedup:** `cache_id` is a fresh UUID per `cache()` call ([extension.rs:914](ballista/core/src/extension.rs#L914)), so identical subplans do not share a cache. Document as intended snapshot semantics; content-addressed dedup is out of scope.
5. **Restart durability:** in-memory registry + startup purge (recommended) vs. a persistent registry backend (larger; deferred until a non-memory `ClusterStorage` exists).
6. **Slot pressure:** cache-write tasks consume executor task slots like any task; long materializations can starve regular tasks. Note for capacity planning; no special handling proposed.
