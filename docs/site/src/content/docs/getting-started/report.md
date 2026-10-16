---
title: One-shot multi-cluster reports
description: Run leeway report across one or more clusters to get a point-in-time JSON report on topology skews and ComputeClass priority ranks.
sidebar:
  order: 3
---

`leeway report` is a stateless, one-shot CLI mode designed for external processes, CI/CD checks, and AI troubleshooting agents. It connects to one or more Kubernetes clusters in parallel, lists the placement-relevant objects, evaluates every subject with dwell timers bypassed (`dwell=0`), and prints a single JSON report.

## Specifying target clusters

`leeway report` resolves clusters from four sources (deduplicated by canonical cluster name):

### 1. Explicit `--clusters` flag

Pass a comma-separated list of `name=https://host:port` pairs, bare API server URLs, or `kube-agents` profile names:

```sh
leeway report \
  --clusters=gke-prod-us-east1=https://10.0.0.1:6443,gke-prod-us-west1=https://10.0.0.2:6443
```

### 2. Cluster list file (`--clusters-from`)

Pass a newline-delimited file (`#` comments and blank lines are ignored):

```sh
leeway report --clusters-from=/etc/leeway/clusters.txt --output=report.json
```

### 3. `kube-agents` profile directory (`--profiles-dir`)

Point `leeway` at a directory of `kube-agents` cluster profiles (`<profiles-dir>/<name>/env` with optional `cluster_identity`):

```sh
leeway report --profiles-dir=~/.kube-agents/profiles
```

When `WATCHER_BROKER_URL` and `WATCHER_BROKER_TOKEN_FILE` are set in the environment, `leeway` automatically routes cluster API connections through the credential-proxy broker.

### 4. Standard `--kubeconfig` / `--context` / `--in-cluster`

When no multi-cluster flags are provided, `leeway report` falls back to your active kubeconfig or in-cluster service account:

```sh
leeway report --kubeconfig=~/.kube/config --context=prod-us-central1
```

## Controlling concurrency and output

- `--concurrency=8` (default): Maximum number of clusters evaluated in parallel.
- `--timeout=60s` (default): Per-cluster snapshot and evaluation timeout. If one cluster is unreachable, its entry in `clusters[]` records `"error": "..."` while the remaining clusters still succeed.
- `--output=<file>` / `-o <file>`: Write the JSON report to disk instead of `stdout`.
- `--compact`: Emit single-line compact JSON instead of indented JSON.
