#!/usr/bin/env python3
"""Utility script to run the Azolla benchmark chart on kind or EKS."""

from __future__ import annotations

import argparse
import json
import secrets
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlparse, urlunparse


def run(
    cmd: List[str], *, check: bool = True, capture_output: bool = False, text: bool = True
) -> subprocess.CompletedProcess:
    print(f"[run] {' '.join(cmd)}")
    return subprocess.run(cmd, check=check, capture_output=capture_output, text=text)


def ensure_tool(name: str) -> None:
    if shutil.which(name) is None:
        raise SystemExit(f"Required tool '{name}' not found on PATH. Please install it before running this script.")
    print(f"[ok] found {name}")


def kind_cluster_exists(name: str) -> bool:
    result = run(["kind", "get", "clusters"], capture_output=True)
    clusters = result.stdout.strip().splitlines()
    return name in clusters


def ensure_kind_cluster(name: str) -> None:
    if kind_cluster_exists(name):
        print(f"[skip] kind cluster '{name}' already exists")
        return
    run(["kind", "create", "cluster", "--name", name])
    if not kind_cluster_exists(name):
        raise SystemExit(f"Failed to create kind cluster '{name}'")
    print(f"[ok] kind cluster '{name}' is ready")


def get_current_context() -> str:
    result = run(["kubectl", "config", "current-context"], capture_output=True)
    context = result.stdout.strip()
    if not context:
        raise SystemExit("Unable to determine current kubectl context")
    return context


def ensure_kube_context(expected: Optional[str]) -> None:
    if expected is None:
        current = get_current_context()
        print(f"[info] using existing kubectl context '{current}'")
        return

    current = get_current_context()
    if current == expected:
        print(f"[ok] kubectl context '{expected}' is active")
        return

    print(f"[info] switching kubectl context from '{current}' to '{expected}'")
    run(["kubectl", "config", "use-context", expected])
    current_after = get_current_context()
    if current_after != expected:
        raise SystemExit(f"Failed to switch kubectl context to '{expected}'")
    print(f"[ok] kubectl context '{expected}' is active")


def namespace_exists(namespace: str) -> bool:
    result = run(["kubectl", "get", "namespace", namespace], capture_output=True, check=False)
    return result.returncode == 0


def ensure_namespace(namespace: str) -> None:
    if namespace_exists(namespace):
        print(f"[skip] namespace '{namespace}' already exists")
        return
    run(["kubectl", "create", "namespace", namespace])
    if not namespace_exists(namespace):
        raise SystemExit(f"Failed to create namespace '{namespace}'")
    print(f"[ok] namespace '{namespace}' is ready")


def secret_exists(namespace: str, secret: str) -> bool:
    result = run(["kubectl", "get", "secret", secret, "-n", namespace], capture_output=True, check=False)
    return result.returncode == 0


def ensure_secret(namespace: str, secret: str, key: str, value: str) -> None:
    if secret_exists(namespace, secret):
        print(f"[skip] secret '{secret}' already exists in namespace '{namespace}'")
        return
    run([
        "kubectl",
        "-n",
        namespace,
        "create",
        "secret",
        "generic",
        secret,
        f"--from-literal={key}={value}",
    ])
    if not secret_exists(namespace, secret):
        raise SystemExit(f"Failed to create secret '{secret}' in namespace '{namespace}'")
    print(f"[ok] secret '{secret}' created")


def helm_release_exists(release: str, namespace: str) -> bool:
    result = run(["helm", "status", release, "-n", namespace], capture_output=True, check=False)
    return result.returncode == 0


def _split_image(image: str) -> Tuple[str, str]:
    repo, tag = image, "latest"
    after_slash = image.rsplit("/", 1)[-1]
    if ":" in after_slash:
        repo, tag = image.rsplit(":", 1)
    return repo, tag


def install_chart(
    release: str,
    chart_path: Path,
    namespace: str,
    secret_name: str,
    secret_key: str,
    orchestrator_image: str,
    shepherd_image: str,
    benchmark_image: str,
) -> None:
    orchestrator_repo, orchestrator_tag = _split_image(orchestrator_image)
    shepherd_repo, shepherd_tag = _split_image(shepherd_image)
    benchmark_repo, benchmark_tag = _split_image(benchmark_image)
    cmd = [
        "helm",
        "upgrade",
        "--install",
        release,
        str(chart_path),
        "-n",
        namespace,
        "--set",
        f"database.existingSecret={secret_name}",
        "--set",
        f"database.existingSecretKey={secret_key}",
        "--set",
        f"orchestrator.image.repository={orchestrator_repo}",
        "--set",
        f"orchestrator.image.tag={orchestrator_tag}",
        "--set",
        f"shepherd.image.repository={shepherd_repo}",
        "--set",
        f"shepherd.image.tag={shepherd_tag}",
        "--set",
        f"benchmarkClient.image.repository={benchmark_repo}",
        "--set",
        f"benchmarkClient.image.tag={benchmark_tag}",
    ]
    run(cmd)
    if not helm_release_exists(release, namespace):
        raise SystemExit(f"Helm release '{release}' failed to install")
    print(f"[ok] helm release '{release}' installed")


def wait_for_deployment(namespace: str, deployment: str, timeout: str) -> None:
    run([
        "kubectl",
        "wait",
        f"deployment/{deployment}",
        "-n",
        namespace,
        "--for=condition=available",
        f"--timeout={timeout}",
    ])
    print(f"[ok] deployment '{deployment}' is available")


def wait_for_job(namespace: str, job: str, timeout: str) -> None:
    run([
        "kubectl",
        "wait",
        f"job/{job}",
        "-n",
        namespace,
        "--for=condition=complete",
        f"--timeout={timeout}",
    ])
    print(f"[ok] job '{job}' completed")


def get_job_logs(namespace: str, job: str) -> None:
    pods = run([
        "kubectl",
        "-n",
        namespace,
        "get",
        "pods",
        "-l",
        f"job-name={job}",
        "-o",
        "json",
    ], capture_output=True)
    data = json.loads(pods.stdout)
    for item in data.get("items", []):
        name = item["metadata"]["name"]
        print(f"[logs] fetching logs for pod {name}")
        run(["kubectl", "-n", namespace, "logs", name])


def cleanup(
    release: str,
    namespace: str,
    cluster: str,
    delete_cluster: bool,
    delete_namespace: bool,
    mode: str,
) -> None:
    if helm_release_exists(release, namespace):
        run(["helm", "uninstall", release, "-n", namespace])
        print(f"[ok] helm release '{release}' removed")
    if delete_namespace and namespace_exists(namespace):
        run(["kubectl", "delete", "namespace", namespace])
        print(f"[ok] namespace '{namespace}' removed")
    if mode == "kind" and delete_cluster and kind_cluster_exists(cluster):
        run(["kind", "delete", "cluster", "--name", cluster])
        print(f"[ok] kind cluster '{cluster}' removed")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Azolla benchmark chart on a local kind cluster or an existing Kubernetes cluster (e.g., EKS)"
    )
    parser.add_argument(
        "--mode",
        choices=["kind", "eks"],
        default="kind",
        help="Execution mode: create/manage a kind cluster locally or use the current kube-context (EKS)",
    )
    parser.add_argument("--cluster-name", default="azolla-benchmark", help="kind cluster name (kind mode only)")
    parser.add_argument("--namespace", default="azolla-benchmark", help="Kubernetes namespace")
    parser.add_argument("--release", default="benchmark", help="Helm release name")
    parser.add_argument(
        "--database-url",
        required=True,
        help="PostgreSQL connection string pointing at the template database (e.g., postgres://user:pass@host:5432/postgres)",
    )
    parser.add_argument("--chart-path", default="helm/benchmark", help="Path to the benchmark chart")
    parser.add_argument("--wait-timeout", default="300s", help="Timeout for deployment/job waits")
    parser.add_argument("--orchestrator-image", default="ghcr.io/azolla/azolla-orchestrator:latest")
    parser.add_argument("--shepherd-image", default="ghcr.io/azolla/azolla-shepherd:latest")
    parser.add_argument("--benchmark-image", default="ghcr.io/azolla/azolla-benchmark-client:latest")
    parser.add_argument("--kube-context", help="kube-context to use (defaults to kind-<cluster> in kind mode)")
    parser.add_argument("--keep-cluster", action="store_true", help="Keep the kind cluster after completion (kind mode only)")
    parser.add_argument("--keep-namespace", action="store_true", help="Keep the namespace after completion")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "kind":
        ensure_tool("kind")
    ensure_tool("kubectl")
    ensure_tool("helm")
    ensure_tool("psql")

    chart_path = Path(args.chart_path)
    if not chart_path.is_dir():
        raise SystemExit(f"Chart path '{chart_path}' does not exist")

    original_dsn = args.database_url
    parsed = urlparse(original_dsn)
    if not parsed.path or parsed.path == "/":
        raise SystemExit("--database-url must include a database name (path component)")

    base_db_name = parsed.path.lstrip("/")
    admin_dsn = urlunparse(parsed._replace(path=f"/{base_db_name}"))
    temp_db_name: Optional[str] = None
    runtime_dsn: Optional[str] = None

    context_hint: Optional[str]
    if args.mode == "kind":
        context_hint = args.kube_context or f"kind-{args.cluster_name}"
    else:
        context_hint = args.kube_context

    try:
        if args.mode == "kind":
            ensure_kind_cluster(args.cluster_name)

        ensure_kube_context(context_hint)
        ensure_namespace(args.namespace)

        temp_db_name = f"azolla_bench_{secrets.token_hex(8)}"
        runtime_dsn = urlunparse(parsed._replace(path=f"/{temp_db_name}"))

        print(f"[info] creating temporary database '{temp_db_name}' using {admin_dsn}")
        run(["psql", admin_dsn, "-c", f"CREATE DATABASE {temp_db_name};"])

        ensure_secret(args.namespace, "azolla-db", "DATABASE_URL", runtime_dsn)
        install_chart(
            args.release,
            chart_path,
            args.namespace,
            "azolla-db",
            "DATABASE_URL",
            args.orchestrator_image,
            args.shepherd_image,
            args.benchmark_image,
        )
        wait_for_deployment(args.namespace, f"{args.release}-orchestrator", args.wait_timeout)
        wait_for_job(args.namespace, f"{args.release}-run", args.wait_timeout)
        get_job_logs(args.namespace, f"{args.release}-run")
        print("[success] benchmark run completed")
    except subprocess.CalledProcessError as exc:
        print(exc.stdout or "", file=sys.stdout)
        print(exc.stderr or "", file=sys.stderr)
        print(f"[error] command failed: {' '.join(exc.cmd)}", file=sys.stderr)
        raise
    finally:
        cleanup(
            args.release,
            args.namespace,
            args.cluster_name,
            delete_cluster=(args.mode == "kind" and not args.keep_cluster),
            delete_namespace=not args.keep_namespace,
            mode=args.mode,
        )

        if temp_db_name and admin_dsn:
            try:
                print(f"[info] dropping temporary database '{temp_db_name}'")
                run(["psql", admin_dsn, "-c", f"DROP DATABASE IF EXISTS {temp_db_name};"], check=False)
            except Exception as exc:
                print(f"[warn] failed to drop database '{temp_db_name}': {exc}")


if __name__ == "__main__":
    main()
