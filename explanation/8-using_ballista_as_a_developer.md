# 8. Using Ballista as a developer

**What you will know after reading this:** what writing real application code against Ballista looks like, the API surface you use day-to-day, how to register tables and propagate configuration, and a walkthrough of three of the most useful canonical examples in the repo.

This is the "hands on the keyboard" file. Everything before it was about how Ballista works; this is about what your code looks like when you use it.

---

## 8.1 The two entry points, again

You met them in file 1; this file is where you actually use them.

### `SessionContext::standalone()`

```rust
use ballista::prelude::*;
use datafusion::prelude::*;

let ctx = SessionContext::standalone().await?;
```

Starts an in-process scheduler on a random local port, registers an in-process executor against it, and hands you a `SessionContext` whose query planner points at the local scheduler. Tear-down happens when the process exits.

When to use:

- Unit and integration tests.
- Local development before you have a cluster to point at.
- Single-machine workloads where the distributed semantics are not load-bearing but you want to validate the distributed code path.

When not to use:

- Anything multi-process. The in-process executor cannot be reached from outside the binary.
- Performance benchmarks at small scale — the serialization overhead makes standalone slower than raw DataFusion for trivial queries.

### `SessionContext::remote(url)`

```rust
let ctx = SessionContext::remote("df://scheduler.internal:50050").await?;
```

Connects to an existing scheduler. The URL is parsed in [ballista/client/src/extension.rs:165](../ballista/client/src/extension.rs#L165): the scheme is informational, the host is required, the port defaults to 50050.

When to use:

- Anything in production.
- Anything where the cluster lifecycle is independent of the client's lifecycle.

Both have `_with_state` variants (`standalone_with_state`, `remote_with_state`) that take a pre-built `SessionState`. You use these when you want to control the function registry, codecs, or config explicitly. The non-`_with_state` variants are convenient defaults.

---

## 8.2 Registering tables

A `SessionContext` returned by Ballista is still a normal DataFusion `SessionContext`. Every `register_*` method works:

```rust
ctx.register_csv("orders", "path/to/orders.csv", CsvReadOptions::new()).await?;
ctx.register_parquet("events", "path/to/events.parquet", ParquetReadOptions::default()).await?;
ctx.register_json("logs", "path/to/logs.json", NdJsonReadOptions::default()).await?;
ctx.register_table("custom", Arc::new(MyTableProvider::new(...)))?;
```

But registration happens on the **client side**. The scheduler and executors do not see this registration directly. They receive a logical plan in which the table reference has already been resolved to a `TableScan` against a `TableProvider` of some kind. The scheduler then re-runs physical planning, which calls back into the `TableProvider` to build an actual scan.

**Two implications:**

1. **The `TableProvider` must be serializable** through the logical codec, or the scan must be reducible to a form that is (e.g. a `ListingTable` reading from a fixed list of file URIs, which DataFusion can serialize natively). For built-in providers (CSV, Parquet, JSON over a local or object-store URL) this is automatic. For custom providers (Iceberg) it is not, and you write a codec — see file 7.

2. **Object store credentials must be available on executors.** The client registers the table via a URL, but the executor is the one that opens the files. If your S3 credentials only live on the client, executors will fail with permission errors. Wire credentials through the runtime producer on the executor (file 7).

---

## 8.3 Running queries

Both APIs work as in DataFusion.

**SQL:**

```rust
let df = ctx.sql("SELECT name, COUNT(*) FROM orders GROUP BY name").await?;
df.show().await?;
```

**DataFrame:**

```rust
let df = ctx.table("orders").await?
    .filter(col("status").eq(lit("complete")))?
    .aggregate(vec![col("name")], vec![count(lit(1))])?;
df.collect().await?;
```

Both end up producing a `LogicalPlan` which goes through `BallistaQueryPlanner`, which produces a `DistributedQueryExec`, which submits to the scheduler. From the user's perspective there is no difference.

A point worth highlighting: `df.show()` and `df.collect()` are the points at which work is actually submitted. Building a DataFrame is lazy; it produces a logical plan. Submission happens only when you ask for output.

If you want to inspect the plan before running it: `df.explain(verbose, analyze)` returns a stringified plan, and `df.into_optimized_plan()` returns the optimized logical plan directly. You can use these to validate that the plan looks reasonable before paying the cost of execution.

---

## 8.4 Configuration

There are three layers of configuration, applied in the order: client default → per-session override → per-query override.

**`BallistaConfig`** ([ballista/core/src/config.rs](../ballista/core/src/config.rs)) defines Ballista-specific session options. Keys include:

- `BALLISTA_JOB_NAME` — a human-readable label for the job, shown in the TUI and REST API.
- `BALLISTA_BROADCAST_JOIN_THRESHOLD_BYTES` — switch to broadcast join when one side is smaller than this.
- `BALLISTA_COALESCE_ENABLED`, `BALLISTA_COALESCE_TARGET_PARTITION_BYTES`, `BALLISTA_COALESCE_MERGED_PARTITION_FACTOR`, `BALLISTA_COALESCE_SMALL_PARTITION_FACTOR` — partition coalescing tuning for AQE.
- `BALLISTA_STANDALONE_PARALLELISM` — number of concurrent tasks for the in-process executor in standalone mode.
- `BALLISTA_CLIENT_GRPC_MAX_MESSAGE_SIZE` — client-side gRPC message size limit.
- `BALLISTA_CLIENT_USE_TLS` — enable mTLS for client-scheduler connection.
- `BALLISTA_SHUFFLE_READER_MAX_REQUESTS`, `BALLISTA_SHUFFLE_READER_FORCE_REMOTE_READ`, `BALLISTA_SHUFFLE_READER_REMOTE_PREFER_FLIGHT` — shuffle read tuning.

You set these on the `SessionConfig`:

```rust
let config = SessionConfig::new_with_ballista()
    .with_target_partitions(8)
    .with_ballista_job_name("nightly ETL: aggregate clicks")
    .set_str("datafusion.execution.batch_size", "8192");
```

DataFusion's standard options (`datafusion.execution.batch_size`, `datafusion.optimizer.*`, etc.) also apply and travel through the same mechanism.

**Per-query overrides:** there is no per-query API per se, but you can build a new `SessionContext` from a `SessionState` with different config and use that for one query.

---

## 8.5 Inspecting cluster state

For debugging and operations, you have three windows into a live cluster.

**The REST API**, if the scheduler was built with `--features rest-api`. Routes are in [ballista/scheduler/src/api/](../ballista/scheduler/src/api/). Endpoints include job listings, executor listings, per-job stage details. Hit it with `curl`:

```bash
curl http://scheduler:50050/api/jobs
```

**The TUI**, in [ballista-cli/src/tui/](../ballista-cli/src/tui/). Launch the CLI with `--cluster-url` pointing at your scheduler, and it gives you a `ratatui`-based dashboard of jobs, executors, and metrics. Built on top of the REST API.

**The `GetJobMetrics` RPC** ([ballista/core/proto/ballista.proto:845](../ballista/core/proto/ballista.proto#L845)) for programmatic per-job metrics. The TUI uses this for per-stage tables.

For per-query inspection, `EXPLAIN ANALYZE` over SQL gives you a `DistributedExplainAnalyzeExec` ([ballista/core/src/execution_plans/distributed_explain_analyze.rs](../ballista/core/src/execution_plans/distributed_explain_analyze.rs)) output that includes per-stage timing and row counts.

---

## 8.6 The CLI binary

`ballista-cli` ([ballista-cli/src/main.rs](../ballista-cli/src/main.rs)) is two things in one binary:

- An **interactive SQL shell**, analogous to `datafusion-cli` but talking to a Ballista cluster. Commands are parsed in [ballista-cli/src/command.rs](../ballista-cli/src/command.rs), executed via [ballista-cli/src/exec.rs](../ballista-cli/src/exec.rs).
- A **TUI dashboard** ([ballista-cli/src/tui/](../ballista-cli/src/tui/)) when launched in that mode. The TUI talks to the REST API.

Invocations:

```bash
# Connect to a remote scheduler, drop into SQL shell
ballista-cli --cluster-url http://scheduler:50050

# Standalone mode (starts in-proc cluster, drops you into shell)
ballista-cli --standalone

# Launch the TUI (requires rest-api on the scheduler)
ballista-cli --cluster-url http://scheduler:50050 --tui
```

For Eur Data, the CLI is most useful early-on as a smoke test against new cluster configurations. Eventually your customers interact through your own SDK or web UI, not this binary.

---

## 8.7 Walkthrough: `standalone-sql.rs`

[examples/examples/standalone-sql.rs](../examples/examples/standalone-sql.rs). The simplest end-to-end Ballista program.

```rust
use ballista::datafusion::{
    common::Result,
    execution::{SessionStateBuilder, options::ParquetReadOptions},
    prelude::{SessionConfig, SessionContext},
};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_examples::test_util;

#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(1)
        .with_ballista_standalone_parallelism(2);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::standalone_with_state(state).await?;

    let test_data = test_util::examples_test_data();
    ctx.register_parquet(
        "test",
        &format!("{test_data}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    let df = ctx.sql("select count(1) from test").await?;
    df.show().await?;
    Ok(())
}
```

What happens, in order:

1. Build a `SessionConfig` with Ballista defaults applied. `with_target_partitions(1)` sets DataFusion's parallelism hint. `with_ballista_standalone_parallelism(2)` tells the in-process executor to run two concurrent tasks.
2. Build a `SessionState` from that config plus DataFusion's default features (functions, etc.).
3. `SessionContext::standalone_with_state(state)` spins up an in-process scheduler and executor and returns a context wired to them.
4. Register a Parquet file as a table. This is the same call you would make in single-node DataFusion.
5. Run a SQL query and show results. Under the hood: logical plan, `BallistaQueryPlanner` wraps in `DistributedQueryExec`, gRPC to the in-process scheduler, scheduler plans the query (one stage, no shuffle), dispatches to the in-process executor, executor reads the Parquet file, returns the count, `df.show()` prints `1`-row table.

If you can run this example successfully, your Ballista build is healthy. It is the first thing to try after a fresh checkout.

---

## 8.8 Walkthrough: `remote-sql.rs`

[examples/examples/remote-sql.rs](../examples/examples/remote-sql.rs). Same shape, different connection mode.

```rust
let config = SessionConfig::new_with_ballista()
    .with_target_partitions(4)
    .with_ballista_job_name("Remote SQL Example");

let state = SessionStateBuilder::new()
    .with_config(config)
    .with_default_features()
    .build();

let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

let test_data = test_util::examples_test_data();
ctx.register_csv(
    "test",
    &format!("{test_data}/aggregate_test_100.csv"),
    CsvReadOptions::new(),
)
.await?;

let df = ctx.sql("SELECT c1, MIN(c12), MAX(c12) FROM test WHERE c11 > 0.1 AND c11 < 0.9 GROUP BY c1").await?;
df.show().await?;
```

Differences from the standalone example:

- `SessionContext::remote_with_state(url, state)` instead of `standalone_with_state`. Expects a scheduler to already be running at the URL.
- `with_target_partitions(4)` because we expect a real cluster with multiple executors.
- `with_ballista_job_name` for visibility in the cluster's job listing.

To run it: first bring up a scheduler and at least one executor (see file 9 for how), then run this binary.

The query (`GROUP BY c1`) does produce a shuffle, so this is a real two-stage example. Compare scheduler logs while it runs: you should see two stage entries in the `ExecutionGraph`, the first leaf, the second depending on it.

---

## 8.9 Walkthrough: `custom-executor.rs`

[examples/examples/custom-executor.rs](../examples/examples/custom-executor.rs). Your first taste of running an executor with overridden hooks.

This example shows how to:

- Build an `ExecutorProcessConfig` programmatically rather than from CLI args.
- Configure custom function registry, config producer, runtime producer.
- Launch the executor process from within a Rust program (useful for tests and embedded scenarios).

Read it when you are about to wire your platform crate (file 7's recommendation) into an executor binary. The pattern is what you copy.

The matching `custom-scheduler.rs` does the same for the scheduler side; together they form the template for a fully customized Ballista deployment.

---

## 8.10 Other examples worth scanning

[examples/examples/](../examples/examples/):

- **`remote-dataframe.rs`** — DataFrame API instead of SQL against a remote cluster. Useful when you want to construct queries programmatically.
- **`standalone-broadcast-join.rs`** — Shows how to tune `BALLISTA_BROADCAST_JOIN_THRESHOLD_BYTES` and confirm a broadcast join is selected.
- **`standalone-substrait.rs`** — Uses Substrait plans instead of the default proto codec. Read if you are considering Substrait as your interchange format.
- **`custom-client.rs`** — Lower-level: talks to the scheduler via the raw `BallistaClient` ([ballista/core/src/client.rs](../ballista/core/src/client.rs)) instead of through `SessionContext`. Useful when you are building your own SDK that does not look like DataFusion's `SessionContext`.
- **`mtls-cluster.rs`** — Sets up scheduler, executor, and client with mTLS certificates. Read before deploying anything sensitive.
- **`remote-spark-functions.rs`** — Demonstrates Spark-compatible function usage when the `spark-compat` feature is enabled.

These examples are the most reliable source of "the API right now," because they are run as part of CI. Documentation drifts; examples are caught by the build.

---

## 8.11 Common application-level patterns

A few patterns that come up repeatedly when building real applications on Ballista.

**Pattern: long-lived `SessionContext`.** A `SessionContext` is cheap to keep alive. Reuse one across many queries from the same user/tenant; do not rebuild for every query. The session ID is stable per `SessionContext`, which means the scheduler can keep registered tables for it.

**Pattern: per-tenant `SessionContext`.** When you have multiple tenants, give each one its own `SessionContext` (and thus its own session on the scheduler). This isolates table registrations and per-session config. The session builder on the scheduler (file 7) can then load tenant-specific catalogs based on the session ID.

**Pattern: error envelopes.** Distributed queries fail in more ways than local queries. Wrap `df.collect().await?` calls in your own retry/classification logic that distinguishes between "scheduler unreachable" (retry), "task failed" (probably do not retry), "submission rejected" (definitely do not retry).

**Pattern: explain before collect.** For expensive queries, call `df.explain(...)` and inspect the plan before triggering execution. This is doubly useful in Ballista because the explain output also tells you the stage structure, which is the first thing to look at if a query is unexpectedly slow.

**Pattern: connection pool for many clients.** A single `SessionContext` holds a gRPC channel to the scheduler. If you have many concurrent users, each user can have their own context; gRPC handles connection multiplexing efficiently. Do not try to share a single context across threads as a serialization point.

---

## 8.12 Where to next

You can now write code against Ballista. The next file shifts to the operational concerns: how do you actually get a cluster up and keep it running.

- Next: [9-deployment_and_operations.md](9-deployment_and_operations.md).
