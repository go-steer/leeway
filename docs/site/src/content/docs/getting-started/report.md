---
title: One-shot multi-cluster reports
description: Run leeway report across one or more clusters to get a point-in-time JSON report on topology skews and ComputeClass priority ranks.
sidebar:
  order: 3
---

`leeway report` is a stateless, one-shot CLI mode designed for external processes, CI/CD checks, and AI troubleshooting agents. It connects to one or more Kubernetes clusters in parallel, lists the placement-relevant objects, evaluates every subject with dwell timers bypassed, and prints a single JSON report.

## Specifying target clusters

`leeway report` resolves clusters from four sources, tried in this order. The first one that is set wins.

### 1. Hermes profiles directory (`--profiles-dir`)

Point `leeway` at a directory of Cluster Agent profiles. A subdirectory counts as a cluster profile when its `config.yaml` carries a complete `cluster_identity` block (`project`, `location`, `cluster`):

```sh
leeway report --profiles-dir=/opt/data/profiles
```

`--clusters` acts as a filter here: name a cluster, a `project/location/cluster` triple, or a profile directory to restrict the run.

### 2. Fleet discovery (`--clusters-from`)

`--clusters-from` names a discovery source, not a file. It accepts:

- `kubeconfig` or `kubeconfig:/path/to/config` — one target per context in the kubeconfig (`--kubeconfig` supplies the path when none is given).
- `profiles:/path/to/profiles` — the same as `--profiles-dir`.
- `<project>` or `<project>/<location>` — every GKE cluster listed under that project (and location) via the GKE API.

```sh
leeway report --clusters-from=kubeconfig --output=report.json
leeway report --clusters-from=my-project/us-central1
```

`--clusters` filters the discovered set in the same way.

### 3. Explicit `--clusters` list

With no discovery source, `--clusters` is a comma-separated list of targets, each one of:

- `project/location/cluster` — a GKE cluster, described through the GKE API.
- `name=https://host:port` — an explicit API server endpoint (`https://` is assumed when the scheme is omitted).
- a kubeconfig context name.

```sh
leeway report \
  --clusters=my-project/us-east1/gke-prod-us-east1,gke-prod-us-west1=https://10.0.0.2:6443
```

### 4. Standard `--kubeconfig` / `--context` / `--in-cluster`

When none of the multi-cluster flags are set, `leeway report` falls back to a single cluster from your kubeconfig (`--kubeconfig`, else `KUBECONFIG`, else `~/.kube/config`), the named `--context`, or the in-cluster service account under `--in-cluster`. `--cluster-name` sets the name recorded in `clusters[].cluster` (default: the context name, else `default`).

```sh
leeway report --kubeconfig=~/.kube/config --context=prod-us-central1
```

When `WATCHER_BROKER_URL` and `WATCHER_BROKER_TOKEN_FILE` are both set in the environment, cluster API connections and GKE API calls are routed through the credential-proxy broker. When only `CREDENTIAL_PROXY_URL` is set (inside the agent-sandbox container), objects are read through the sandbox's `kubectl` instead of a direct client.

## Scoping the evaluation

- `--namespace=<ns>[,<ns>...]`: Restrict workload and pod evaluation to these namespaces (default: all namespaces).
- `--sources=topology-drift,compute-class` (default): Which sources to evaluate.
- `--topology-keys=topology.kubernetes.io/zone,topology.kubernetes.io/region` (default): Node labels treated as topology axes, in precedence order.
- `--topology-domain-unavailable-keys` (default: the same two keys): Axes on which a domain with no usable node raises `leeway.domain_unavailable`. Pass an empty string to disable.
- `--topology-cluster-defaults=<key>=<maxSkew>[:DoNotSchedule|ScheduleAnyway],...` or `none`: Declare the cluster's kube-scheduler default spread constraints instead of letting `leeway` assume them.
- `--topology-tier-c-signals` / `--compute-class-tier-c-signals`: Include Tier C findings in `topology_findings` / `rank_findings` (off by default).
- `--all`: Include every evaluated workload and node-group subject in each cluster's `subjects` array, breaching or not.
- `--min-drift=<fraction>`: Include subjects whose `score.drift` is at least this value in `subjects`.

## Controlling concurrency and output

- `--parallelism=8` (default): Maximum number of clusters evaluated in parallel.
- `--timeout=60s` (default): Per-cluster evaluation timeout. If one cluster is unreachable, its entry in `clusters[]` has `"status": "error"` and an `error` string while the remaining clusters still succeed.
- `--output=<file>` / `-o <file>`: Write the JSON report to disk instead of `stdout`.
- `--pretty=false`: Emit single-line JSON instead of the indented default.
