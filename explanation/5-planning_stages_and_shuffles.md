# 5. Planning, stages, and shuffles

**What you will know after reading this:** how a DataFusion logical plan becomes a Ballista distributed physical plan, why stage boundaries exist exactly where they do, what the three shuffle-related `ExecutionPlan` nodes mean, and how a concrete `GROUP BY` query is broken into a stage DAG.

This is the conceptual centerpiece of the folder. If you only have time to deeply read one file beyond file 1, this is the one.

---

## 5.1 The DataFusion planning pipeline, recap

When you call `ctx.sql("...")` in DataFusion:

1. **Parse** SQL into an AST.
2. **Plan** the AST into a `LogicalPlan` (a tree of logical operators: `Projection`, `Filter`, `Aggregate`, `Join`, `TableScan`, etc.). Names and types are resolved against registered catalogs and `TableProvider`s.
3. **Optimize the logical plan.** Run a fixed list of `OptimizerRule`s (predicate pushdown, constant folding, projection pushdown, decorrelation, etc.). Output: another `LogicalPlan`, hopefully smaller and cheaper.
4. **Physical plan.** A `PhysicalPlanner` (default: `DefaultPhysicalPlanner`) walks the logical plan and emits an `Arc<dyn ExecutionPlan>` tree (`HashAggregateExec`, `FilterExec`, `ParquetExec`, `HashJoinExec`, etc.). This tree has concrete partitioning information attached.
5. **Optimize the physical plan.** Run `PhysicalOptimizerRule`s (aggregate pushdown, sort enforcement, etc.).
6. **Execute.** The execution plan's `execute(partition, context)` method returns a `SendableRecordBatchStream` for each output partition. The runtime pulls from these and you get rows.

In single-node DataFusion, steps 1-6 all happen in the client process. The output stream is consumed locally.

Ballista intercepts at the boundary between steps 4 and 5.

---

## 5.2 Where Ballista hooks in: `BallistaQueryPlanner`

The hook is `BallistaQueryPlanner` ([ballista/core/src/planner.rs:41](../ballista/core/src/planner.rs#L41)), which implements DataFusion's `QueryPlanner` trait. The Ballista session state installs it in place of the default planner.

What it does is almost embarrassingly simple: instead of producing a real physical plan, it produces *exactly one* node — `DistributedQueryExec` ([ballista/core/src/execution_plans/distributed_query.rs](../ballista/core/src/execution_plans/distributed_query.rs)) — wrapping the entire optimized logical plan as a serializable payload.

When DataFusion later asks that node to execute, it does:

1. Open a gRPC connection to the scheduler.
2. Serialize the wrapped logical plan using the configured `LogicalExtensionCodec`.
3. Call `ExecuteQuery`, getting back a `job_id`.
4. Poll the scheduler until the job finishes.
5. For each output partition, open an Arrow Flight stream to the executor holding that partition and read the `RecordBatch`es.

So on the **client side**, no distributed planning happens at all. The client hands off the logical plan and waits. All physical planning, stage breaking, and shuffle insertion happens on the **scheduler**. This is important to internalize: the client only knows logical plans; the scheduler is the sole owner of the distributed physical plan.

---

## 5.3 What the scheduler does on the other side

Scheduler-side, the journey of a submitted logical plan goes:

1. **Deserialize the logical plan** using the configured `LogicalExtensionCodec`.
2. **Run DataFusion's standard physical planner** on it. Output: a normal DataFusion `Arc<dyn ExecutionPlan>` tree, identical to what you would get in single-node DataFusion. No Ballista-specific nodes yet.
3. **Run distributed physical optimizer rules** ([ballista/scheduler/src/physical_optimizer/](../ballista/scheduler/src/physical_optimizer/)). These rules walk the tree and insert `ShuffleWriterExec` / `UnresolvedShuffleExec` nodes wherever a stage boundary is needed. The output is a tree that is *partitioned into stages* by these insertion points.
4. **Construct the `ExecutionGraph`** ([ballista/scheduler/src/state/execution_graph.rs](../ballista/scheduler/src/state/execution_graph.rs)). Each contiguous sub-tree that does not cross a `ShuffleWriterExec` becomes a stage. The `UnresolvedShuffleExec`s in a stage are the "input slots" naming which upstream stages it depends on.

After step 4, the scheduler has a DAG of stages, each containing one DataFusion physical plan, and the stages have explicit data dependencies. From here on it is the scheduler's job (file 3) to schedule tasks, await results, resolve downstream stages, and so on.

---

## 5.4 The three shuffle operators

These are the only Ballista-specific physical plan nodes you really need to understand. They all live in [ballista/core/src/execution_plans/](../ballista/core/src/execution_plans/).

### `ShuffleWriterExec`

[ballista/core/src/execution_plans/shuffle_writer.rs](../ballista/core/src/execution_plans/shuffle_writer.rs).

This is the "freeze point." It takes a single input stream and writes its contents to disk, partitioned by a `Partitioning` (almost always `Hash(exprs, n)`). The output of a `ShuffleWriterExec` task is *not* `RecordBatch`es — it is a set of files plus a list of `ShuffleWritePartition` metadata records ([ballista/core/proto/ballista.proto:500](../ballista/core/proto/ballista.proto#L500)) describing them.

A stage that ends in a `ShuffleWriterExec` produces shuffle output; it does not stream to the client. Almost every non-final stage ends with one of these.

A sort-based variant `SortShuffleWriterExec` ([ballista/core/src/execution_plans/sort_shuffle/](../ballista/core/src/execution_plans/sort_shuffle/)) does the same thing but sorts within each output partition.

### `UnresolvedShuffleExec`

[ballista/core/src/execution_plans/unresolved_shuffle.rs](../ballista/core/src/execution_plans/unresolved_shuffle.rs).

A placeholder. It says, in effect: "this subtree of the plan reads from the output of stage N, but we do not know yet which executors will have those partitions because stage N has not run yet." It carries the upstream stage ID and the shape of the data, but no concrete locations.

`UnresolvedShuffleExec`s are present in every non-leaf stage at the moment it is constructed. They are what makes a stage `UnResolved` in the stage state machine (file 3).

### `ShuffleReaderExec`

[ballista/core/src/execution_plans/shuffle_reader.rs](../ballista/core/src/execution_plans/shuffle_reader.rs).

The runtime counterpart. When all of a stage's upstream stages have completed, the scheduler walks the stage's plan and replaces every `UnresolvedShuffleExec` with a `ShuffleReaderExec` carrying the concrete `PartitionLocation`s collected from the upstream task status updates. The plan, now containing real readers, is what gets dispatched to executors.

At execute time, a `ShuffleReaderExec` opens connections (Arrow Flight to remote executors, or local file reads when the data is on the same executor) and produces a `RecordBatch` stream of the input partition it was assigned.

The rewriting from `UnresolvedShuffleExec` to `ShuffleReaderExec` is the heart of stage resolution. It is also the surface area for adaptive query execution (file 3, section 3.8): in principle, AQE rules can also rewrite the rest of the plan at this moment based on observed statistics.

---

## 5.5 What is a "stage," precisely

A stage is a contiguous subtree of the physical plan that contains no `ShuffleWriterExec` except possibly at its root. Equivalently: a stage is the largest unit of work that can be executed end-to-end without redistributing data across the network.

Each stage has:

- **A physical plan** (one DataFusion `Arc<dyn ExecutionPlan>`).
- **A partition count** — the number of output partitions, which is the number of tasks the stage will be split into.
- **Zero or more input stages**, each represented in the plan by an `UnresolvedShuffleExec` (initially) or `ShuffleReaderExec` (after resolution).
- **A stage_id**, unique within the job.
- **A state**: `UnResolved`, `Resolved`, `Running`, `Successful`, `Failed`.

Stages are the unit of scheduling: the scheduler hands out task definitions one stage at a time, with one task per partition.

A common confusion: a stage is not the same as a task. A stage with 100 output partitions is dispatched as 100 separate tasks, each producing one partition. The tasks within a stage run in parallel; the stages within a job run with the dependencies forced by the DAG.

---

## 5.6 Worked example: `SELECT a, MIN(b) FROM t WHERE a <= b GROUP BY a`

This is the query from the README. Let us walk it.

**Step 1: optimized logical plan.**

After DataFusion's logical optimizer:

```
Aggregate: groupBy=[a], aggr=[MIN(b)]
  Filter: a <= b
    TableScan: t projection=[a, b]
```

**Step 2: DataFusion physical plan (scheduler side, before stage breaking).**

DataFusion's physical planner produces roughly:

```
AggregateExec (mode=Final, group=[a], aggr=[MIN(b)])
  CoalescePartitionsExec
    AggregateExec (mode=Partial, group=[a], aggr=[MIN(b)])
      FilterExec: a <= b
        ParquetExec: t [a, b]
```

DataFusion's two-phase aggregation: a `Partial` aggregate on each input partition produces partial sums grouped by `a`, then a `CoalescePartitionsExec` brings them onto one partition, then a `Final` aggregate finishes the aggregation. This is what you would get in single-node DataFusion.

The problem in a distributed setting: `CoalescePartitionsExec` assumes all the partial aggregate outputs are on the same machine. They are not. We need a shuffle on the grouping key `a` so that all rows with the same `a` end up on the same executor for the final aggregation.

**Step 3: distributed physical optimizer inserts a shuffle.**

The distributed optimizer recognizes the partial/final aggregate pattern and rewrites it to:

```
Stage 2 (final aggregation):
    AggregateExec (mode=Final, group=[a], aggr=[MIN(b)])
      UnresolvedShuffleExec: stage_id=1, partitioning=Hash([a], N)

Stage 1 (partial aggregation + shuffle write):
    ShuffleWriterExec: partitioning=Hash([a], N)
      AggregateExec (mode=Partial, group=[a], aggr=[MIN(b)])
        FilterExec: a <= b
          ParquetExec: t [a, b]
```

Two stages now. Stage 1 is a leaf (no inputs from other stages); stage 2 reads from stage 1.

**Step 4: ExecutionGraph.**

```
ExecutionGraph(job_id=...) {
  stages: {
    1: UnResolvedStage {  // becomes Resolved immediately - has no inputs
         partitions: P_in  // determined by ParquetExec, e.g. number of files
         plan: <Stage 1 plan above>
       },
    2: UnResolvedStage {  // waits for stage 1
         partitions: N  // determined by the shuffle's hash partitioning
         plan: <Stage 2 plan above, with UnresolvedShuffleExec>
       }
  },
  edges: { 1 -> 2 }
}
```

Stage 1 transitions to `Resolved` straight away (no inputs to wait on), then `Running`.

**Step 5: execution.**

- The scheduler picks executors for stage 1's `P_in` tasks. Each task reads its slice of the Parquet input, runs filter + partial aggregate, and writes N shuffle output files. Each task reports a `ShuffleWritePartition` per output partition, with file path and stats.
- When all stage 1 tasks succeed, stage 1 is `Successful`. Its outputs are catalogued as `PartitionLocation`s indexed by (stage_id=1, partition_id=0..N-1, source executor).
- The scheduler resolves stage 2: walk its plan, replace `UnresolvedShuffleExec` with `ShuffleReaderExec` populated with all `PartitionLocation`s for stage 1's outputs.
- Stage 2 transitions to `Resolved`, then `Running`. Each of its N tasks is responsible for one output partition. Each task's `ShuffleReaderExec` connects (over Arrow Flight or local file) to the upstream executors and fetches all stage 1 outputs for *its* partition. Then it runs the final aggregate.
- Each stage 2 task produces one final `RecordBatch` stream. For a query that ends in a stage 2-style "final" output, these get materialized to disk too (so the client can fetch them via Flight without holding executors hostage), or in some configurations are streamed back through the scheduler.
- The client's `DistributedQueryExec` fetches each final partition and yields `RecordBatch`es to the user's `.show()` or DataFrame stream.

**Stage and shuffle count, as a heuristic:**

- A query with no grouping, joins, or sort: typically one stage.
- A query with a single `GROUP BY` or one shuffle-requiring join: two stages, one shuffle.
- A query with chained shuffles (joins on top of grouped data, etc.): three or more stages.

You can see the actual stage graph for any query you run: the `EXPLAIN` output through Ballista, the GraphViz exporter ([ballista/scheduler/src/state/execution_graph_dot.rs](../ballista/scheduler/src/state/execution_graph_dot.rs), enabled by the `graphviz-support` feature), or the REST API's job inspection endpoints all expose it.

---

## 5.7 When does the optimizer insert a shuffle

The rules are in [ballista/scheduler/src/physical_optimizer/](../ballista/scheduler/src/physical_optimizer/). At a conceptual level, a shuffle is inserted whenever the *required input partitioning* of an operator does not match the *output partitioning* of its child. The common cases:

- **Aggregation with grouping keys.** Requires `Hash(group_keys)`. Inserted as in the example above.
- **Hash join.** Requires both sides to be partitioned on the join keys. Two shuffles (one per side), one stage per side.
- **Broadcast join.** No shuffle on the broadcast side; the small side is replicated to every executor. The threshold is set by `BALLISTA_BROADCAST_JOIN_THRESHOLD_BYTES` ([ballista/core/src/config.rs](../ballista/core/src/config.rs)).
- **Sort that crosses partitions.** Requires range partitioning, which today usually falls back to a single-partition coalesce.
- **Window functions over a partition.** Requires `Hash(partition_by)`.

Operators with no partitioning requirement (filter, projection, most scalar transformations) do not introduce shuffles; they run in the same stage as their child.

---

## 5.8 `DistributedQueryExec` and `DistributedExplainAnalyzeExec`

For completeness, two more execution plan nodes in [ballista/core/src/execution_plans/](../ballista/core/src/execution_plans/):

- **`DistributedQueryExec`** ([distributed_query.rs](../ballista/core/src/execution_plans/distributed_query.rs)) — already covered. The client-side wrapper that ships the whole logical plan to the scheduler and streams results back. You never construct this directly; `BallistaQueryPlanner` creates it.

- **`DistributedExplainAnalyzeExec`** ([distributed_explain_analyze.rs](../ballista/core/src/execution_plans/distributed_explain_analyze.rs)) — the distributed-aware variant of `EXPLAIN ANALYZE`. Runs the query, collects per-operator and per-stage metrics from every executor that participated, and produces a human-readable plan with timing and row counts attached. Invaluable for performance debugging; the equivalent of looking at Spark's stage detail UI.

There is also a scheduler-side helper for plan display: [ballista/scheduler/src/state/distributed_explain.rs](../ballista/scheduler/src/state/distributed_explain.rs).

---

## 5.9 Why "stages" instead of just "operators"

A different system could ship every operator as a task: produce one task per operator, schedule each, materialize between every operator. That would be very flexible and very slow — every operator boundary becomes a disk write.

The stage abstraction exists because adjacent operators that share an input partitioning can be fused into one task. The pipeline `ParquetExec → Filter → Partial Aggregate` does not need disk between its operators; everything streams through Arrow's columnar in-memory format. Only the boundaries where partitioning *must* change become materialization points.

So a stage is essentially "the biggest chunk of pipelinable work we can give a single task." Choose stage boundaries well, and you minimize the number of writes-and-reads to disk while still distributing the work.

This is also why custom plan nodes that introduce non-trivial data-distribution requirements need careful thought: if you add an operator that says "I need range-partitioned input on column X," you have just told the optimizer to insert a stage boundary. That has correctness implications (need a shuffle that produces range-partitioned output) and cost implications (extra disk write + network read).

---

## 5.10 Where to next

You now understand the planning pipeline, what a stage is, what a shuffle is, and how a query becomes a DAG. The next file is about the *bytes* that flow across the wire when these things are exchanged between processes.

- Next: [6-wire_protocol_and_serialization.md](6-wire_protocol_and_serialization.md).
