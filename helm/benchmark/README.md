# Azolla Benchmark Helm Chart

This chart launches a short-lived Azolla stack (orchestrator, shepherd, and benchmark client) to measure sustained throughput against an existing PostgreSQL instance (for example, Amazon RDS). When the benchmark finishes successfully it cleans up the orchestrator and shepherd workloads, leaving only the Job pod logs for inspection. If the benchmark fails, the resources are left in place for troubleshooting.

## Prerequisites

- An Azolla-compatible PostgreSQL database (schema will be managed by the orchestrator). The chart does **not** provision PostgreSQL; provide an RDS or other managed instance.
- A Kubernetes cluster (tested with EKS) with the ability to pull images from GHCR.
- A Kubernetes Secret containing the PostgreSQL connection string, e.g.

  ```bash
  kubectl create secret generic azolla-db \
    --from-literal=DATABASE_URL="postgres://user:password@host:5432/azolla"
  ```

- The Azolla images published to GHCR (defaults assume `ghcr.io/azolla/*`). Override the repositories or tags as needed.

## Quick Start

Install the chart into a dedicated namespace, pointing at your database secret:

```bash
helm install benchmark ./helm/benchmark \
  --namespace azolla-benchmark --create-namespace \
  --set database.existingSecret=azolla-db \
  --set database.existingSecretKey=DATABASE_URL
```

Useful overrides:

- `orchestrator.image.repository` / `.tag`: location of the orchestrator image.
- `shepherd.image.repository` / `.tag`: location of the shepherd image.
- `benchmarkClient.image.repository` / `.tag`: location of the benchmark client image built from this repo.
- `benchmarkClient.totalRequests`: total tasks to submit (default 10,000).
- `benchmarkClient.threads`: number of worker threads in the benchmark client (default 8).
- `benchmarkClient.cleanupOnSuccess`: set to `false` to keep the orchestrator and shepherd running after a successful benchmark.
- `benchmarkClient.waitForReadySeconds`: how long to wait for the orchestrator deployment to become ready before giving up.

## Chart Behaviour

1. Deploys the orchestrator Deployment and Service, pointing at the supplied PostgreSQL connection string via `AZOLLA__DATABASE__URL`.
2. Deploys the shepherd Deployment, which connects to the orchestrator and uses the bundled `trivial_task` implementation.
3. Runs the benchmark Job using the `azolla-benchmark-client` image. The job waits for the orchestrator to become ready, executes the requested number of `trivial_task` submissions, and logs throughput every reporting interval.
4. On success (and if `cleanupOnSuccess` is `true`), the job removes the orchestrator and shepherd deployments and services. The Job object itself is removed automatically after `benchmarkClient.jobTtlSeconds`.

To re-run the benchmark, install the chart again (for example, `helm upgrade --install`).

## Cleanup

If you want to stop the benchmark early or clean up after a failed run, delete the Helm release:

```bash
helm uninstall benchmark --namespace azolla-benchmark
```

This command removes all benchmark-related Kubernetes resources but leaves your PostgreSQL instance untouched.

## Automated Benchmark Runner

The helper script `scripts/run_benchmark_kind.py` automates benchmark execution in two modes:

- **kind (default)** – provisions a throwaway kind cluster, deploys the chart, streams benchmark logs, and tears everything down (unless you pass `--keep-cluster`/`--keep-namespace`).
- **EKS/production** – reuses the current kube-context (optionally supplied via `--kube-context`), deploys into the specified namespace, and leaves the cluster untouched.

Common behaviour:

- validates prerequisites (`kubectl`, `helm`, and `kind` when running in kind mode)
- creates the namespace (if missing), provisions a unique temporary PostgreSQL database based on the template URL you provide, and seeds the secret with that per-run connection string
- installs the Helm chart using the GHCR images/tags passed on the command line
- waits for the orchestrator deployment and benchmark Job to complete
- streams Job logs, which include interval and final throughput metrics
- optionally cleans up the Helm release, namespace, kind cluster, and drops the temporary database

### kind example

```bash
python3 scripts/run_benchmark_kind.py \
  --mode kind \
  --database-url 'postgres://USER:PASSWORD@HOST:5432/azolla' \
  --orchestrator-image ghcr.io/<org>/azolla-orchestrator:<tag> \
  --shepherd-image ghcr.io/<org>/azolla-shepherd:<tag> \
  --benchmark-image ghcr.io/<org>/azolla-benchmark-client:<tag>
```

### EKS example

```bash
python3 scripts/run_benchmark_kind.py \
  --mode eks \
  --kube-context <your-eks-context> \
  --database-url 'postgres://USER:PASSWORD@HOST:5432/azolla' \
  --namespace azolla-benchmark \
  --orchestrator-image ghcr.io/<org>/azolla-orchestrator:<tag> \
  --shepherd-image ghcr.io/<org>/azolla-shepherd:<tag> \
  --benchmark-image ghcr.io/<org>/azolla-benchmark-client:<tag> \
  --keep-namespace
```

Use a database URL that points at an existing admin database (for example `postgres://user:pass@host:5432/postgres`). The script will create a randomly named database for the benchmark run, inject that URL into the cluster, and drop the database during teardown so multiple runs do not interfere with each other. The script echoes the benchmark logs to stdout so you can capture the sustained throughput. Ensure the referenced GHCR images exist and the supplied database URL is reachable from your cluster.
