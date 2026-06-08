# 10. Applying Ballista to Eur Data

**What you will know after reading this:** what Ballista gives you for free toward building a Snowflake-class product on Iceberg, exactly where Iceberg plugs into Ballista, what is missing and where that missing piece has to come from, an ordered build sequence for a working MVP, and a risk register you can plan against.

This is the file you came for. It assumes you have read files 1-9. It is also opinionated. Where the codebase is silent, this file offers a recommendation rather than a survey.

---

## 10.1 What Ballista gives you on day one

If you stripped Ballista down to its useful contributions for Eur Data, you get:

- **Distributed SQL execution over Apache Arrow.** A correct, parallel, push-down-aware execution engine that can scale across executors. You do not have to build a query engine.
- **DataFusion's optimizer and SQL coverage.** A mature logical optimizer (predicate pushdown, projection pushdown, decorrelation, broadcast detection, partial-final aggregation rewrites) and a growing physical optimizer. You inherit improvements as DataFusion evolves.
- **A control plane (the scheduler) and a data plane (executors + Arrow Flight).** Two clean process types with well-defined gRPC contracts. You can put autoscaling and per-tenant pools on top.
- **Plug-in surfaces** (file 7) that let you customize behavior without forking the engine: codecs, config and runtime producers, session builders, execution engines, distribution policies.
- **Arrow Flight as a result-fetch protocol** that any client (Rust, Python, JDBC via Flight SQL) can speak.
- **Standalone mode** for local development and CI that exercises the same code paths as distributed mode.

That is meaningful. Recognize that what Snowflake built took thousands of engineer-years; what you are getting from Ballista is the equivalent of "Spark plus DataFusion's modern engine" already done, and the project being open source.

What it gives you is the *query engine*. What you build on top is the *product*.

---

## 10.2 Iceberg integration points, concretely

Iceberg is your table format. It defines: how files are arranged in object storage, how snapshots accumulate, how transactional commits work, how schemas evolve, how partitions are pruned, how time travel resolves to a specific snapshot.

DataFusion can read Iceberg via either of two paths:

- **[`iceberg-rust`](https://github.com/apache/iceberg-rust)** — the Apache Iceberg Rust implementation. Has a DataFusion `TableProvider` integration in progress (check current state at time of integration).
- **[`datafusion-iceberg`](https://github.com/datafusion-contrib/datafusion-iceberg)** — a community-maintained adapter, possibly more feature-complete in DataFusion-specific aspects.

You will pick one. The choice should be guided by which one supports the Iceberg features you need (positional and equality deletes, snapshot pinning, partition spec evolution) and which one is most actively maintained at your time of evaluation. This is a moving target; verify with a small spike.

Whichever you choose, the plug-in points into Ballista are the same. Here is the wiring, hook by hook.

### Catalog and table registration: the session builder

Iceberg uses a *catalog* to map table names to current metadata pointers. Catalog options include:

- Iceberg REST catalog (most flexible, vendor-neutral).
- AWS Glue.
- Project Nessie (Git-style versioning on top of Iceberg).
- Hive Metastore.
- A simple file-system catalog (development only).

The catalog lives outside Ballista. Your code talks to it to resolve table names. The right place to do that resolution is on the **scheduler**, in the session builder override (file 7 section 7.6):

```rust
let session_builder: SessionBuilder = Arc::new(|config: SessionConfig| {
    let session_id = config.options().extensions.get::<EurDataSessionContext>()
        .map(|s| s.tenant_id.clone())
        .unwrap_or_default();

    let catalog = build_iceberg_catalog_for_tenant(&session_id)?;
    let provider = IcebergCatalogProvider::new(catalog);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    state.register_catalog("iceberg", Arc::new(provider));
    Ok(state)
});
```

Register this on `SchedulerConfig::with_override_session_builder`. The result: every session on the scheduler has the tenant's Iceberg catalog registered. SQL queries can reference tables by their fully-qualified name and the scheduler resolves them at planning time.

### Object store access: the runtime producer

Iceberg tables live in object storage. Executors are the ones that actually read Parquet (or Avro/ORC) files. That means executors need:

- The right `ObjectStore` implementation for your storage scheme.
- Credentials to authenticate.
- Optional: regional configuration, S3 path style, etc.

Wire this in the runtime producer on the **executor** ([ballista/executor/src/config.rs:213](../ballista/executor/src/config.rs#L213)):

```rust
let runtime_producer: RuntimeProducer = Arc::new(|config: &SessionConfig| {
    let s3 = AmazonS3Builder::from_env()
        .with_region(config.options().extensions.get::<EurDataRegion>().unwrap().0.clone())
        .build()?;

    let registry = DefaultObjectStoreRegistry::new();
    registry.register_store(&Url::parse("s3://").unwrap(), Arc::new(s3));

    let runtime = RuntimeEnvBuilder::new()
        .with_object_store_registry(Arc::new(registry))
        .build()?;

    Ok(Arc::new(runtime))
});
```

Credentials come from the environment or a sidecar (vault, IRSA on EKS, workload identity on GKE). Do not put credentials in `SessionConfig` extensions if they can be avoided; if they must be there, never let them be serialized into a `KeyValuePair` on the wire.

### Plan serialization: the logical codec (if needed)

This is the part you will only discover by trying.

When Iceberg-rust or datafusion-iceberg builds a `TableProvider`, the table provider produces a physical `ExecutionPlan` at planning time. The scheduler then needs to serialize that plan and send it to executors. **If** the physical scan operator is a standard `FileScanConfig`-based scan over Parquet files (which is the case for most simple Iceberg reads), DataFusion can already serialize it natively. The default Ballista codec handles it.

**If** the Iceberg integration produces a custom `ExecutionPlan` (an `IcebergScanExec` that needs to be aware of equality deletes, position deletes, or metadata pruning at execute time), then you have a custom physical plan node that needs a custom physical codec, registered everywhere (file 7 section 7.3).

A pragmatic test sequence:

1. Build a minimal standalone Ballista program that registers an Iceberg table and runs a query.
2. If it works, great — the table provider reduces to standard DataFusion nodes that round-trip.
3. If it fails with a deserialization error on the executor, you need a custom physical codec. Look at what `ExecutionPlan` type the Iceberg integration produces and write a codec for that type.

This is the single highest-risk integration item. Budget time for it.

### Snapshot pinning, time travel, deletes

These are Iceberg's killer features and the user-visible reasons people pick Iceberg over plain Parquet. Each has implications:

- **Snapshot pinning.** A query specifies "as of snapshot X" or "as of timestamp T." This is a planning-time concern: the catalog resolves the snapshot, the table provider scans the right files. As long as your table provider supports the syntax, no Ballista work is needed.
- **Time travel.** Same as snapshot pinning, expressed differently in SQL.
- **Position deletes and equality deletes.** Iceberg can express "this row has been deleted" without rewriting the data file. The reader must filter out deleted rows. If your table provider handles this transparently (producing a standard `ExecutionPlan` that does the filtering), Ballista's default codec handles it. If the provider exposes deletes as a separate operator type, you need a custom codec.

For Eur Data, snapshot pinning and time travel are competitive necessities. Confirm your chosen Iceberg integration supports them end-to-end with Ballista before committing.

---

## 10.3 What is missing for a Snowflake-class product

Ballista is a query engine. Snowflake is a query engine plus the entire surrounding product. Here is what is missing, organized by where it has to come from.

### From you: gateway, AuthN, AuthZ, multi-tenancy

Ballista's scheduler gRPC is unauthenticated. For a multi-tenant SaaS, you put a gateway in front:

- Terminates client TLS.
- Authenticates the client (API key, OAuth, SSO).
- Identifies the tenant.
- Forwards the request to the scheduler, attaching the tenant ID as a session identifier or in `KeyValuePair` settings.
- Logs the request for billing and audit.

The gateway is your code. It can be a thin gRPC-to-gRPC proxy, or a richer service that also speaks SQL HTTP (à la Snowflake's SQL API) and translates internally.

Multi-tenancy below the gateway: do you give each tenant their own cluster, or share clusters? Trade-offs:

- **One cluster per tenant.** Strong isolation, simple security model, but high baseline cost. Reasonable for enterprise tier.
- **Shared cluster, per-tenant sessions.** Lower cost. Requires careful session builder (per-tenant catalogs), execution engine override (per-tenant resource accounting), and possibly a custom distribution policy (per-tenant fair scheduling). Higher engineering investment.

A hybrid model is common: shared executors for small tenants, dedicated executors for large tenants, all coordinated by the same scheduler (or one scheduler per tier).

### From you: persistent metadata

The scheduler has no persistence. You will need an external store (Postgres is the obvious starter) for:

- **Query log / audit.** Every submission, who submitted it, what it cost, success/failure.
- **Billing events.** Bytes scanned, compute seconds, executor time.
- **User and tenant metadata.** API keys, roles, quotas.
- **Saved queries, workspaces, dashboards** if you build those.

The natural pattern: your gateway records submissions and outcomes; your execution engine override records per-task resource use; a side-channel ships these to your store. Ballista itself never touches this store.

### From you: result caching and materialization

Snowflake's result cache and materialized views are major differentiators. Ballista has neither. Implementation options:

- **Result cache.** Hash the query plan, look it up before submission. Serve cached results when found. Cache invalidation on table writes is the hard part (Iceberg's snapshot IDs give you a clean version key).
- **Materialized views.** Define a query, compute it periodically, write the result as an Iceberg table, rewrite incoming queries to use the materialization when applicable. Substantial engineering.

Both layers sit above Ballista. They do not require modifying it.

### From you: workload management

Concurrency limits per tenant, query priority, queueing, sliding-window quotas — none of this is in Ballista. Implement in the gateway or in a custom `DistributionPolicy` (file 7 section 7.10).

### From you: an SDK and UI

Snowflake users do not write `BallistaClient` calls. They use a web UI, a Python connector, a JDBC driver, a CLI. You build these. The Ballista components that help:

- **Arrow Flight SQL.** Standard JDBC drivers exist for Flight SQL; configure your cluster's Flight proxy (`--advertise-flight-sql-endpoint`) and you have a JDBC story.
- **REST API.** If enabled, your web UI can talk to the scheduler over HTTP for job listing and management.

### From Ballista's roadmap (be patient or contribute)

The following are on [ROADMAP.md](../ROADMAP.md) but not done. You can wait, contribute, or work around:

- **Multi-scheduler.** Removes scheduler as SPOF; enables sharding.
- **Persistent scheduler state.** Job history survives restart.
- **Better shuffle cleanup.** Less manual disk management.
- **Adaptive query execution refinement.** Better mid-query replanning.
- **Auto-scaling support beyond KEDA's basic model.**
- **Python UDFs.** If you need them, this is a roadmap item.

For each, decide: is it MVP blocking, is it post-MVP critical, or is it nice-to-have?

### From Iceberg

Iceberg itself solves transactional commits, schema evolution, partition pruning, snapshot management, time travel storage. These come "for free" with your Iceberg integration; they are not Ballista's concern.

---

## 10.4 A suggested first build sequence

Ordered, each step producing a runnable artifact. Treat this as a checklist for the MVP.

**Step 1: Standalone Ballista + local Parquet.** Run [examples/examples/standalone-sql.rs](../examples/examples/standalone-sql.rs). Confirm your build environment works.

**Step 2: Standalone Ballista + S3-backed Parquet via the official extensions example.** Follow [docs/source/user-guide/extensions-example.md](../docs/source/user-guide/extensions-example.md). This validates that you can wire a custom object store through the runtime producer. Use MinIO or real S3.

**Step 3: Standalone Ballista + Iceberg.** Add an Iceberg `TableProvider` to your session builder. Run a simple `SELECT *`. This is the moment when codec issues surface (or do not).

**Step 4: Distributed Ballista (Docker Compose) + Iceberg.** Move from standalone to two executors and one scheduler. Run a query that produces a shuffle (a `GROUP BY` against an Iceberg table). Verify executor logs show shuffle write/read activity. This validates that your codec, config producer, and runtime producer work across process boundaries.

**Step 5: Build your platform crate.** Centralize all custom codec, producer, builder code in a single crate that scheduler, executor, and client all consume. This is the moment you should stop hand-wiring overrides per-binary.

**Step 6: Kubernetes deployment.** Move to your target deployment platform. Set memory pool sizes explicitly. Verify executor graceful shutdown works. Set up Prometheus scraping on the scheduler.

**Step 7: Add a gateway.** Build the thin AuthN/AuthZ layer in front of the scheduler. Authenticated request → session identification → forward to scheduler. Log everything to your audit store.

**Step 8: Add KEDA-driven executor autoscaling.** Turn on `--features keda-scaler` on the scheduler. Define a `ScaledObject` against the scheduler's KEDA endpoint. Verify scale-out under load and scale-in when idle.

**Step 9: Add per-tenant catalogs in the session builder.** Multi-tenancy at the session level. Test with two tenants seeing disjoint table sets.

**Step 10: Add an execution engine override for resource accounting.** Per-task CPU seconds, memory peak, bytes scanned. Ship to your billing store.

**Step 11: Build the customer-facing SDK and the web UI.** Now you have a product.

Each step is "small enough to fit in a sprint, big enough to demonstrably advance the product."

---

## 10.5 Risk register

What can go wrong, ordered by likelihood × impact.

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Iceberg integration produces custom plan nodes that fail to round-trip through default codec | High | High | Spike step 3 early. Budget for writing custom codecs. Coordinate with iceberg-rust maintainers. |
| Scheduler restart loses in-flight jobs in production | Certain | Medium | Until persistent state lands, design client/gateway to retry. Document the SLO with this in mind. |
| Performance gap to Snowflake on complex workloads | Likely (early) | Medium | Profile with TPC-H benchmarks ([benchmarks/](../benchmarks/)) early. Identify bottleneck stages. Contribute optimizer rules upstream. |
| Shuffle file accumulation causes executor disk pressure | Likely | Medium | Aggressive cleanup intervals. Monitoring on `--work-dir` disk usage. Custom cleanup cron as a backstop. |
| Multi-scheduler not available for scheduler HA at scale | Likely (later) | Medium | Active-passive deployment with health-checked failover. Plan to migrate when upstream lands multi-scheduler. |
| AuthN/AuthZ design lock-in via the gateway | Likely | Medium | Make the gateway thin and replaceable. Do not assume Ballista will grow native AuthN any time soon. |
| Tenant isolation breaks under heavy concurrency | Possible | High | Enforce isolation in execution engine override (CPU/memory accounting) and in distribution policy (per-tenant slots). Test with hostile workloads. |
| DataFusion logical plan format changes in a minor version | Possible | Low-Medium | Pin versions across components. Update all binaries together. Have a release validation suite. |
| AQE incomplete; queries with skewed partitions perform poorly | Possible | Medium | Detect skew early in benchmarks. Use explicit broadcast-join thresholds. Contribute to AQE upstream. |
| Substrait incompatibility if you bet on it as interchange | Possible | High | Validate Substrait round-trip for your full workload before committing. Default to native proto codec; treat Substrait as opt-in. |

None of these are show-stoppers. All of them are knowable now, plannable for now, and addressable with concrete engineering decisions.

---

## 10.6 What Ballista is *not*

Ending with a clear-eyed statement. Ballista is not:

- A complete data warehouse product. It is the engine.
- A storage system. Use Iceberg + object storage.
- A catalog. Use Iceberg REST catalog or similar.
- An identity provider. You add a gateway.
- A query result cache. You add it above.
- A billing platform. You build it.
- A web UI. You build it.
- A streaming engine. (Though DataFusion has streaming work in progress, Ballista's distribution model is batch-oriented.)
- A pre-built solution for HA at the scheduler tier. (Yet.)

Once you internalize these "is not"s, the picture becomes clear: you are using Ballista the way you would use PostgreSQL or Kafka — as a powerful, well-engineered component that you assemble into a product. You do not get Snowflake by installing it; you get Snowflake by building the surrounding product around it.

The bet you are making is that the engine is good enough, extensible enough, and on a trajectory that will keep improving. Based on this repository, that bet looks reasonable.

---

## 10.7 Where to next

You have the full picture. The last file is a reference: vocabulary and pointers to deeper resources.

- Next: [11-glossary_and_further_reading.md](11-glossary_and_further_reading.md).
