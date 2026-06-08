# 1. From DataFusion to Ballista: the delta

**What you will know after reading this:** what *actually* changes when you take a working single-node DataFusion application and run it across a Ballista cluster, and which four irreducible problems Ballista exists to solve that single-node DataFusion never has to confront.

This whole folder is built on the assumption that you already understand DataFusion as a single-process query engine: `SessionContext`, `LogicalPlan`, `ExecutionPlan`, `RecordBatch`, `TableProvider`. If those are unfamiliar, read the DataFusion guide first; the rest of this folder will be much harder otherwise.

---

## 1.1 The exact code delta

A standard DataFusion application:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new())
        .await?;

    let df = ctx
        .sql("SELECT a, MIN(b) FROM example WHERE a <= b GROUP BY a LIMIT 100")
        .await?;

    df.show().await?;
    Ok(())
}
```

The Ballista version of the same thing:

```rust
use ballista::prelude::*;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::standalone().await?;          // <-- changed

    ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new())
        .await?;

    let df = ctx
        .sql("SELECT a, MIN(b) FROM example WHERE a <= b GROUP BY a LIMIT 100")
        .await?;

    df.show().await?;
    Ok(())
}
```

One line changed. That is intentional. Ballista is presented as a drop-in upgrade so you can adopt it without rewriting your DataFusion code. But that one line hides a lot, and the rest of this folder is about what it hides.

The exact two entry points live in [ballista/client/src/extension.rs](../ballista/client/src/extension.rs):

- `SessionContext::standalone()` ([ballista/client/src/extension.rs:146](../ballista/client/src/extension.rs#L146)) starts a scheduler and an executor in the current process, connects them over gRPC on a local port, and hands you back a `SessionContext` whose query planner submits work to that local cluster.
- `SessionContext::remote(url)` ([ballista/client/src/extension.rs:113](../ballista/client/src/extension.rs#L113)) skips the cluster bring-up and just connects to a scheduler you already have running, e.g. `df://localhost:50050`.

Both go through the same plumbing internally: they install a `BallistaQueryPlanner` ([ballista/core/src/planner.rs:41](../ballista/core/src/planner.rs#L41)) into the `SessionState`. From the user's perspective the `SessionContext` looks identical, but every query you run through it now travels over the wire instead of being executed in your process.

The official README is blunt about the cost of this abstraction:

> There is a gap between DataFusion and Ballista, which may bring incompatibilities. The community is actively working to close the gap.

Keep that quote in mind. Some DataFusion features (notably anything that depends on user-defined logical or physical plan nodes, certain catalogs, certain `TableProvider` implementations) do not yet round-trip cleanly through Ballista's serialization layer. You will hit this when you start integrating Iceberg in file 10.

---

## 1.2 The four irreducible problems Ballista solves

Single-node DataFusion gets to ignore distribution. The instant you say "run this query across N machines," four problems appear that you cannot get rid of, only solve differently. Ballista's entire surface area is a direct response to these four.

### Problem 1: who decides what runs where

In single-node DataFusion, the runtime is the same process as the planner. The `ExecutionPlan` is just walked top-down and executed against the local Tokio runtime. There is nobody to decide *where* a task runs because there is only one place it can run.

In Ballista, you have many executors and one (eventually multiple) scheduler. Something has to:

- Know which executors are alive.
- Decide which task goes to which executor.
- Track which tasks succeeded, which failed, which to retry.
- Decide what to do when an executor disappears mid-query.

That something is the **scheduler**. It is a separate process with its own gRPC service ([ballista/core/proto/ballista.proto:823](../ballista/core/proto/ballista.proto#L823)) and its own internal state machine. File 3 is dedicated to it.

### Problem 2: where do intermediate results live

In single-node DataFusion, intermediate `RecordBatch` streams flow from one operator to the next inside the same process. A `HashAggregateExec` reads its input by polling its child operator. No serialization, no I/O, just a Rust stream.

The moment your `GROUP BY` has to combine data from rows that live on different machines, this stops working. The grouped-by column has to be **redistributed** so that all rows with the same group key land on the same executor. That redistribution is called a **shuffle**, and it forces three things to exist:

- A point in the plan where output is "frozen" and written somewhere durable (disk or object store) instead of streamed.
- A mechanism for the consuming side to fetch the partitions it needs.
- A planner that knows where to insert these freeze points in the plan.

Ballista's answer: `ShuffleWriterExec` ([ballista/core/src/execution_plans/shuffle_writer.rs](../ballista/core/src/execution_plans/shuffle_writer.rs)) writes Arrow IPC files to the executor's work directory, partitioned by the shuffle key. `ShuffleReaderExec` ([ballista/core/src/execution_plans/shuffle_reader.rs](../ballista/core/src/execution_plans/shuffle_reader.rs)) on the consuming side fetches those partitions over **Arrow Flight** ([ballista/executor/src/flight_service.rs](../ballista/executor/src/flight_service.rs)). The planner inserts the boundary. File 5 walks through this end-to-end.

### Problem 3: who knows what the plan is

In single-node DataFusion, the `LogicalPlan` and `ExecutionPlan` are just Rust values in memory. They never leave the process.

In Ballista the client builds a plan, but the **scheduler** has to understand it to break it into stages, and the **executor** has to understand it to run the stages. The plan therefore has to cross at least two process boundaries (client to scheduler, scheduler to executor) and possibly two language boundaries (Python client to Rust scheduler, for example).

This means every node in the plan tree must be **serializable** in a format that all three roles understand. Ballista chooses protobuf as the default ([ballista/core/proto/ballista.proto](../ballista/core/proto/ballista.proto), [ballista/core/proto/datafusion.proto](../ballista/core/proto/datafusion.proto)) and Substrait as an optional alternative. Anything that does not have a protobuf encoding cannot survive the trip.

This is why the **codec** is one of the most important extension points in Ballista. If you add a custom `LogicalPlan` node (which you may well do for Iceberg-specific behavior), you must also implement a `LogicalExtensionCodec` so the scheduler and executors can read it. File 6 is about the wire protocol; file 7 is about how to plug in your own codecs.

### Problem 4: how do you configure the cluster from one place

In single-node DataFusion, `SessionConfig` is just a struct in your process. You set values, the executor reads them.

In Ballista the *client* builds a `SessionConfig`, but the *executor* is the one that actually reads tables, runs operators, and needs to know things like S3 credentials, broadcast-join thresholds, custom function registrations, and catalog endpoints. The config has to be:

- Serialized into protobuf.
- Sent across gRPC alongside the plan.
- Reconstituted on the executor into a `SessionConfig` that produces the same `RuntimeEnv` (object stores, function registry, etc.).

Ballista solves this with the **config producer** and **runtime producer** pattern ([ballista/core/src/extension.rs](../ballista/core/src/extension.rs)): you register a function on both ends that knows how to build a config and runtime from a key-value bag. The bag is what travels over the wire; the functions reconstitute the rich types. File 7 covers this in detail.

---

## 1.3 The shape of a Ballista cluster, in 30 seconds

Three roles. They each get their own deep-dive file later; this is just enough to orient you.

- **Client.** Your application. Holds a `SessionContext` whose query planner is `BallistaQueryPlanner`. Submits a plan to the scheduler over gRPC, then either streams results back from executors or fetches them via Arrow Flight SQL.
- **Scheduler.** One process (today; multi-scheduler is on the roadmap). Listens on gRPC port `50050` by default. Receives plans, breaks them into stages, hands tasks to executors, tracks status, returns results to the client. State lives in memory.
- **Executor.** N processes. Each registers with the scheduler on startup and heartbeats periodically. Listens on gRPC port `50052` for task commands and on Arrow Flight port `50051` for shuffle reads from peer executors. Runs DataFusion physical plans on the partitions it has been assigned.

In **standalone mode** (`SessionContext::standalone()`), all three roles run in your process. The scheduler and executor are spun up on local sockets and torn down with the process. This is the dev-loop and test mode.

In **distributed mode**, the scheduler and executor binaries run separately (typically in containers, see file 9). Your application is purely a client, connecting via `SessionContext::remote("df://host:50050")`.

The standard architecture diagram lives at [docs/source/contributors-guide/ballista_architecture.excalidraw.svg](../docs/source/contributors-guide/ballista_architecture.excalidraw.svg). Open it now; the rest of this folder is easier with that picture in your head.

---

## 1.4 What does *not* change when you adopt Ballista

This is worth saying explicitly because it is the whole reason Ballista is interesting as a foundation for Eur Data:

- **SQL syntax.** Whatever DataFusion accepts, Ballista accepts (within the codec gap).
- **DataFrame API.** Same `DataFrame` methods, same builder pattern.
- **`TableProvider`.** A `TableProvider` implementation that works in DataFusion will work in Ballista *as long as* it can be serialized over the wire. For Parquet, CSV, JSON this is built in. For custom providers (Iceberg) you handle serialization via the codec hooks. See file 7.
- **Function registry.** UDFs, UDAFs, window functions registered through DataFusion's normal mechanisms continue to work, as long as you register them on the executor side too (file 7 again).
- **Optimizer rules.** Ballista uses DataFusion's logical and physical optimizer. Distributed-specific rules are layered on top in [ballista/scheduler/src/physical_optimizer/](../ballista/scheduler/src/physical_optimizer/).
- **Arrow as the in-memory format.** Identical. Ballista's wire format for shuffle data is Arrow IPC, which is the on-disk/over-wire serialization of the same Arrow `RecordBatch` you already know.

So mentally: Ballista is *DataFusion plus a control plane plus a shuffle layer plus a wire format*. Everything you already know about DataFusion still applies; Ballista wraps it.

---

## 1.5 What honestly does break or surprise you

In order of how likely you are to hit each one:

1. **Custom `TableProvider`s do not "just work" remotely.** If you register a custom provider on the client, the executor will not know how to reconstruct it unless you also wire a logical codec that can serialize the table source. This is the single biggest gotcha for Iceberg integration.
2. **Statistics may be wrong or missing.** DataFusion's optimizer leans on table statistics for join ordering and broadcast decisions. If your catalog does not surface accurate stats to the scheduler, you will get worse plans than you would in single-node DataFusion against a hot table.
3. **There is no persistent scheduler state.** Restart the scheduler and you lose job history, currently-running jobs, the lot. This is on the roadmap ([ROADMAP.md](../ROADMAP.md)) but not done.
4. **Shuffle files are not always cleaned up.** Long-running clusters accumulate stale work directories. There is a configurable cleanup interval but the roadmap explicitly flags this as incomplete.
5. **Multi-scheduler is not supported yet.** A single scheduler is a single point of failure for the cluster control plane.
6. **Authentication is mTLS only.** There is no user/role concept built into the gRPC surface. For a Snowflake-class product you will put a gateway in front (file 10).

None of these are blockers for understanding the system. They are blockers for productionizing the system, and file 9 and file 10 come back to them.

---

## 1.6 Where to next

Now that you know the *shape* of the change, file 2 walks through how the three roles boot, find each other, and run a query end-to-end. Read that next; then files 3 to 6 dive into each piece in depth.

- Next: [2-cluster_topology_and_lifecycle.md](2-cluster_topology_and_lifecycle.md).
