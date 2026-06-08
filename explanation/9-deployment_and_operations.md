# 9. Deployment and operations

**What you will know after reading this:** how to take a Ballista cluster from "it works on my laptop" to "it serves real workloads." Binary vs container vs Kubernetes, which Cargo features matter for production, the consolidated configuration surface, observability, and the operational limitations to plan around.

This file is the bridge between understanding Ballista (files 1-8) and applying it to Eur Data (file 10).

---

## 9.1 Standalone binary vs distributed binaries

Three artifacts ship from the repo:

- **`ballista-scheduler`** — built from [ballista/scheduler/src/bin/main.rs](../ballista/scheduler/src/bin/main.rs). Run one per cluster (today).
- **`ballista-executor`** — built from [ballista/executor/src/bin/main.rs](../ballista/executor/src/bin/main.rs). Run N per cluster.
- **`ballista-cli`** — built from [ballista-cli/src/main.rs](../ballista-cli/src/main.rs). Interactive shell and TUI.

A fourth "all-in-one" binary is built via [dev/docker/ballista-standalone.Dockerfile](../dev/docker/ballista-standalone.Dockerfile), bundling scheduler + executor in one container. Convenient for single-VM deployments, not appropriate for anything you want to scale.

The decision matrix:

| Scenario | Use |
|---|---|
| Local development | `SessionContext::standalone()` in your code, no binaries needed |
| Single-VM trial deployment | Standalone container or `ballista-standalone` binary |
| Production, single-rack | Separate scheduler + executor binaries on bare metal or VMs |
| Production, cloud | Separate containers managed by Docker Compose, Nomad, or Kubernetes |
| Production, autoscaling | Kubernetes + KEDA |

---

## 9.2 Cargo features for production

You build Ballista with features. The defaults are conservative; for production you turn things on. The feature catalog is in [README.md](../README.md); here is the editorial view of which ones matter.

For the **scheduler** ([ballista/scheduler/Cargo.toml](../ballista/scheduler/Cargo.toml)):

| Feature | Default | Production guidance |
|---|---|---|
| `build-binary` | Yes | Required for the binary. |
| `substrait` | Yes | Leave on if you might ever need Substrait; otherwise off saves binary size. |
| `prometheus-metrics` | No | **Turn on.** Without metrics, operating a cluster is guesswork. |
| `rest-api` | No | **Turn on** if you want the TUI or any HTTP-based operator tooling. Adds dependencies. |
| `graphviz-support` | No | Useful for debugging stage graphs in non-prod. |
| `keda-scaler` | No | **Turn on** if you run on Kubernetes and want executor autoscaling. |
| `spark-compat` | No | Only if you have queries written against Spark-specific functions. |
| `disable-stage-plan-cache` | No | Diagnostic only. Leave off. |

For the **executor** ([ballista/executor/Cargo.toml](../ballista/executor/Cargo.toml)):

| Feature | Default | Production guidance |
|---|---|---|
| `build-binary` | Yes | Required. |
| `arrow-ipc-optimizations` | Yes | Leave on. Free performance. |
| `mimalloc` | Yes | Leave on. Significantly better allocator behavior under load. |
| `spark-compat` | No | Mirror the scheduler setting. |

For **`ballista-core`**:

| Feature | Default | Production guidance |
|---|---|---|
| `arrow-ipc-optimizations` | Yes | Leave on. |
| `build-binary` | No | Enabled transitively by scheduler/executor binaries. |
| `force_hash_collisions` | No | Testing only. **Never enable in production.** |

A practical baseline for Eur Data:

```bash
cargo build --release -p ballista-scheduler \
  --features build-binary,substrait,prometheus-metrics,rest-api,keda-scaler

cargo build --release -p ballista-executor \
  --features build-binary,arrow-ipc-optimizations,mimalloc
```

---

## 9.3 Container images

Dockerfiles ship at [dev/docker/](../dev/docker/):

- `ballista-scheduler.Dockerfile` — scheduler binary, exposes 50050.
- `ballista-executor.Dockerfile` — executor binary, exposes 50051 (Flight) and 50052 (gRPC).
- `ballista-standalone.Dockerfile` — combined binary.
- `ballista-cli.Dockerfile` — CLI for ad-hoc use.
- `ballista-benchmarks.Dockerfile` — benchmark harness.

Entrypoints in the same directory show the default flag wiring. Cribbing from these is the fastest way to build your own production images that bundle your platform crate (from file 7) alongside the Ballista binary. The pattern is:

1. Use the official Dockerfile as a base.
2. Add a `COPY` for your platform crate or for binary artifacts that include your customizations.
3. Replace the entrypoint with one that points at *your* built binary (typically a thin Rust binary that constructs `ExecutorProcessConfig` programmatically with all your overrides wired in, then delegates to Ballista's process functions).

The official `apache/datafusion-ballista-scheduler:latest` and `apache/datafusion-ballista-executor:latest` images are useful for evaluation but not for production with extensions — you cannot inject custom codecs into them.

---

## 9.4 Docker Compose

[docker-compose.yml](../docker-compose.yml) at the repo root is the minimal correct compose setup. It spins up one scheduler and two executors. Read it once; it is short and shows the right environment variables, port mappings, and dependency order.

Use it as the basis for a development cluster on a single host. For multi-host or production, move to Kubernetes.

---

## 9.5 Kubernetes

The reference is [docs/source/user-guide/deployment/kubernetes.md](../docs/source/user-guide/deployment/kubernetes.md). The shape:

- **Scheduler.** One pod, typically a `Deployment` with `replicas: 1` (because multi-scheduler is not supported yet). A `Service` of type `ClusterIP` or `LoadBalancer` exposes port 50050.
- **Executors.** A `Deployment` (or `StatefulSet` if you want stable node identities for cache affinity) with N replicas. Each pod registers with the scheduler on startup.
- **Headless `Service` for executors.** Optional, useful if peer-to-peer connectivity needs DNS resolution per pod.
- **ConfigMap for shared config.** Codec choices, Iceberg catalog URLs, etc.
- **Secret for credentials.** S3 keys, mTLS certs, catalog auth tokens.
- **HPA or KEDA for autoscaling.** With the `keda-scaler` feature, the scheduler exposes a KEDA external scaler endpoint via [ballista/scheduler/src/scheduler_server/external_scaler.rs](../ballista/scheduler/src/scheduler_server/external_scaler.rs) that reports load, and KEDA scales the executor `Deployment` based on it. Without `keda-scaler` you can still use HPA with CPU/memory metrics, but those are noisier signals than scheduler-reported load.

Operational notes:

- **Executor termination must be graceful.** Set `terminationGracePeriodSeconds` to be at least `--executor-termination-grace-period` plus the longest expected task duration. Otherwise rolling updates will kill in-flight queries.
- **Pod anti-affinity for executors.** You usually want executors spread across nodes for both fault tolerance and shuffle locality.
- **Storage.** Mount a fast local volume for `--work-dir`. For shuffle-heavy workloads on cloud nodes, this should be NVMe.
- **Network policies.** Executors talk peer-to-peer on the Flight port. If you lock down with `NetworkPolicy`, allow executor-to-executor on 50051 explicitly.

---

## 9.6 Configuration surface, consolidated

All the configuration knobs in one place. Cross-reference back to the source files for exhaustive lists.

### Scheduler (`ballista/scheduler/src/config.rs`)

The most consequential flags:

- `--bind-host`, `--bind-port` — gRPC bind address.
- `--external-host` — what executors should use to reach the scheduler. **Set explicitly in containers.**
- `--namespace` — cluster identifier; only relevant when a single state backend serves multiple clusters (not really possible today with the in-memory backend, but the field exists for future use).
- `--scheduler-policy` — `pull-staged` or `push-staged`. Must match executor.
- `--task-distribution` — `bias` or `round-robin`.
- `--event-loop-buffer-size` — backpressure tuning. Default 1000 in CLI, 10000 in library.
- `--executor-timeout-seconds`, `--expire-dead-executor-interval-seconds` — how quickly to declare an executor dead.
- `--task-max-failures`, `--stage-max-failures` — retry budgets.
- `--finished-job-data-clean-up-interval-seconds`, `--finished-job-state-clean-up-interval-seconds` — GC for completed jobs.
- `--grpc-server-max-decoding-message-size`, `--grpc-server-max-encoding-message-size` — gRPC limits. Raise if large plans fail.
- `--disable-rest-api` (when `rest-api` feature is on) — turn off REST without recompiling.
- `--advertise-flight-sql-endpoint` — when set, scheduler runs a Flight proxy in front of executors.

### Executor (`ballista/executor/src/config.rs`)

The most consequential flags:

- `--scheduler-host`, `--scheduler-port` — where to register.
- `--bind-host`, `--bind-port` (Flight), `--bind-grpc-port` (control).
- `--external-host` — what peers and scheduler should use to reach this executor. **Set explicitly in containers.**
- `--concurrent-tasks` — task slot count. Default 0 (= num CPUs).
- `--memory-pool-size` — total memory budget. **Set this in production.** Otherwise OOMs.
- `--work-dir` — shuffle file directory. Set to a fast disk.
- `--task-scheduling-policy` — must match scheduler.
- `--executor-heartbeat-interval-seconds` — default 60s.
- `--job-data-clean-up-interval-seconds`, `--job-data-ttl-seconds` — per-executor cleanup.
- `--scheduler-connect-timeout-seconds` — retry budget for initial registration.
- `--metric-collection-policy` — what executor metrics to ship to the scheduler.

### Session-level (`BallistaConfig` in `ballista/core/src/config.rs`)

Set per `SessionContext`. Includes broadcast thresholds, coalesce tuning, shuffle reader tuning, gRPC message size, mTLS toggle. See file 8 section 8.4 for the catalog.

---

## 9.7 Observability

Three layers, in order of how soon you want them.

### Logs

Both binaries log via `env_logger`/`tracing`. Defaults are `INFO,datafusion=INFO`. For production, ship logs to a central collector (Loki, Elasticsearch, Cloud Logging). Pay attention to:

- Scheduler "executor expired" warnings.
- Executor "task failed" errors and their `FailedReason` payloads.
- Shuffle read failures (`FetchPartitionError`).
- gRPC server warnings about exceeded message size.

Set `--log-dir` if you want file-based rotation; otherwise logs go to stderr.

### Metrics (Prometheus)

With `--features prometheus-metrics` on the scheduler, a Prometheus scrape endpoint is exposed. The implementation is in [ballista/scheduler/src/metrics/prometheus.rs](../ballista/scheduler/src/metrics/prometheus.rs). Standard SRE practice: scrape, dashboard, alert.

Key signals for alerts:

- Active executor count drops below threshold.
- Job failure rate above threshold.
- Event loop processing latency above threshold (signals overload).
- Scheduler memory growth (signals state retention bug or DDoS).

For executors there is no built-in Prometheus exporter today; ship process-level metrics (CPU, memory, disk I/O on `--work-dir`) via your platform's standard mechanism (cAdvisor on Kubernetes, node exporter on VMs).

### REST API and TUI

For interactive debugging. See file 8 sections 8.5 and 8.6. The TUI is your friend during incident response.

### `GetJobMetrics` and `EXPLAIN ANALYZE`

For per-query diagnosis. When a customer says "this query is slow," `EXPLAIN ANALYZE` is the first thing to reach for; it tells you which stage and which operator dominated time.

---

## 9.8 Capacity planning

A short, opinionated list.

**Memory.** The executor's working memory is dominated by hash tables for joins/aggregates and by `RecordBatch` buffers. For a guideline, expect each concurrent task to use up to `memory_pool_size / concurrent_tasks`. Set both flags explicitly; default unbounded is dangerous in production.

**CPU.** One task uses one core under heavy compute. `concurrent_tasks = num_cores` is the natural starting point; lower it if you also need headroom for gRPC and Flight servers.

**Disk.** Shuffle files. Worst case a stage's intermediate output is the size of its input (no aggregation reduction); typical case smaller. Budget at least 2x your peak working-set per executor. NVMe matters.

**Network.** Shuffle reads are the dominant traffic. Hot spots: a "large" stage with many fan-in connections. Plan for inter-executor traffic, not just client-scheduler.

**Scheduler sizing.** The scheduler is event-loop bound. One node sized for moderate CPU and large memory (job state is in memory) works for most clusters. Watch the event-loop latency metric; if it climbs, you are either taking too many submissions or the in-memory state has grown too large.

---

## 9.9 Operational limitations to plan around

These are unavoidable today and shape every architectural decision downstream. The roadmap intends to address most of them ([ROADMAP.md](../ROADMAP.md)); you are not waiting forever, but you are waiting.

1. **Scheduler is a single point of failure.** Multi-scheduler is on the roadmap. Until then, scheduler restart loses all in-flight jobs. Mitigations:
   - Keep the scheduler in a fast-restart configuration (small JVM-equivalent footprint, no heavy initialization).
   - Build your application to handle "scheduler unreachable" as a retryable error.
   - Run scheduler in a managed restart loop (Kubernetes `Deployment` with `replicas: 1` does this).

2. **No persistent cluster state.** Restart loses job history. If you need audit, ship events out.

3. **Shuffle file cleanup is best-effort.** Monitor disk on executors. Have a fallback cleanup script for the work directory.

4. **No authentication.** mTLS provides confidentiality and identity at the transport layer, but no AuthN/AuthZ at the application layer. Put a gateway in front of the scheduler if you expose it to untrusted clients.

5. **No workload isolation between queries.** All queries share the executors' memory pool and CPU. A heavy query can starve others. Workarounds: separate executor pools per priority class, or implement your own admission control in front.

6. **No CBO joint optimization.** Each query is planned in isolation. Multi-query optimization is not a thing.

7. **Limited fault tolerance for long queries.** Task and stage retries are bounded. A query that has run for hours and fails near the end will not gracefully resume from a checkpoint; it will start over (with retried stages cached when possible, but no formal checkpoint protocol).

---

## 9.10 Upgrade and version management

Mixing Ballista versions between scheduler and executors is not supported. The protobuf schemas can change. Upgrade procedure:

1. Drain executors (`StopExecutor` RPC or SIGTERM with adequate grace period). They will finish in-flight tasks.
2. Bring scheduler down.
3. Bring new scheduler up.
4. Bring new executors up. They register against the new scheduler.

For zero-downtime upgrades, you currently have to do blue-green: spin up an entire new cluster, switch your client URL, decommission the old one. Until multi-scheduler lands and the cluster state has a persistent backend, you cannot do meaningful in-place upgrades.

For client SDK upgrades, version skew with the scheduler is sometimes tolerated for minor versions but is not guaranteed. Pin your client SDK version to your scheduler version.

---

## 9.11 Backup and disaster recovery

Today, there is nothing to back up in the scheduler — state is in memory and ephemeral. Catalogs (Iceberg) and underlying object stores are *your* responsibility, separate from Ballista.

If you build out a custom `BallistaCluster` backend that persists state (e.g. to Postgres), you take on the standard DR responsibilities for that store: snapshots, replication, point-in-time recovery.

For the underlying object store and Iceberg metadata, follow standard practices for those systems. Ballista does not add backup requirements.

---

## 9.12 Where to next

You can now run a Ballista cluster. The final substantive file ties everything together for the Eur Data goal.

- Next: [10-applying_ballista_to_eur_data.md](10-applying_ballista_to_eur_data.md).
