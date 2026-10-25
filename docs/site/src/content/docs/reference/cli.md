---
title: CLI reference
description: Subcommands and flags for leeway report, leeway watch, and leeway version.
sidebar:
  order: 2
---

`leeway` provides three subcommands:
- `leeway report [flags]` — Run a one-shot placement and priority-rank evaluation across one or more clusters and emit JSON.
- `leeway watch [flags]` — Run the continuous daemon with informers, `/metrics`, `/healthz`, `/readyz`, and optional webhook delivery. (Also the default when invoked with bare flags.)
- `leeway version` — Print the build version.

`leeway report --help` and `leeway watch --help` print the authoritative flag lists for the installed build.

## Cluster discovery flags (shared by `report` and `watch`)

| Flag | Default | Description |
| --- | --- | --- |
| `--clusters` | `""` | Comma-separated cluster targets: `project/location/cluster` GKE triples, `name=https://endpoint` pairs, or kubeconfig context names. Acts as a filter when `--profiles-dir` or `--clusters-from` is set. |
| `--clusters-from` | `""` | Fleet discovery source: `kubeconfig[:/path]`, `profiles:/path/to/profiles`, or `<project>[/<location>]`. |
| `--profiles-dir` | `""` | Hermes profiles directory; each Cluster Agent profile is a subdirectory whose `config.yaml` has a `cluster_identity` block. |
| `--kubeconfig` | `""` | Path to a kubeconfig; empty falls back to `KUBECONFIG`, then `~/.kube/config`. |
| `--context` | `""` | Kubeconfig context to use instead of its current-context. |
| `--in-cluster` | `false` | Use in-cluster service account credentials. |
| `--cluster-name` | `""` | Logical cluster name for single-cluster mode (defaults to the context name, else `default`). |
| `--project` | `""` | Default GCP project for cluster identity when not inferred from discovery. |
| `--location` | `""` | Default GCP location (region or zone) for cluster identity when not inferred from discovery. |
| `--sources` | `topology-drift,compute-class` | Comma-separated sources to run (`topology-drift`, `compute-class`). |

## Scoring flags (shared by `report` and `watch`)

| Flag | Default | Description |
| --- | --- | --- |
| `--topology-keys` | `topology.kubernetes.io/zone,topology.kubernetes.io/region` | Node labels treated as topology axes, in precedence order. |
| `--topology-domain-unavailable-keys` | `topology.kubernetes.io/zone,topology.kubernetes.io/region` | Axes on which a domain with no usable node raises `leeway.domain_unavailable`. Pass an empty string to disable. |
| `--topology-cluster-defaults` | `""` | Cluster kube-scheduler `PodTopologySpread` `defaultConstraints`, as `key=maxSkew[:DoNotSchedule\|ScheduleAnyway]` comma-separated, or `none`. Empty means the system defaults are assumed. |
| `--topology-tier-c-signals` | `false` | Include Tier C topology-drift findings (`report`: in `topology_findings`; `watch`: in the signals counter and sink). |
| `--compute-class-tier-c-signals` | `false` | Include Tier C compute-class findings (`leeway.rank_tier_unused`) (`report`: in `rank_findings`; `watch`: in the signals counter and sink). |

## `leeway report` flags

| Flag | Default | Description |
| --- | --- | --- |
| `--namespace` | `""` | Comma-separated namespaces to scope workload and pod evaluation (empty queries all namespaces). |
| `--all` | `false` | Include all evaluated workload and node-group subjects in each cluster's `subjects` array, even when not breaching. |
| `--min-drift` | `0` | Include evaluated subjects with `score.drift >= min-drift` in each cluster's `subjects` array. |
| `--parallelism` | `8` | Maximum number of clusters evaluated concurrently. |
| `--timeout` | `60s` | Per-cluster evaluation timeout. |
| `--output`, `-o` | `""` (stdout) | Write the JSON report to this file path instead of `stdout`. |
| `--pretty` | `true` | Pretty-print JSON with indentation; `--pretty=false` emits single-line JSON. |

## `leeway watch` flags

| Flag | Default | Description |
| --- | --- | --- |
| `--kube-qps` | `50` | Client-side rate limit (queries per second) for Kubernetes API calls. |
| `--kube-burst` | `100` | Client-side burst limit for Kubernetes API calls. |
| `--metrics-addr` | `:9090` | Listener address for `/metrics`, `/healthz`, and `/readyz`. Empty disables the listener. |
| `--otel-exporter` | `none` | Also push metrics over OTLP: `otlp` or `none`. |
| `--access-recheck` | `2m` | How often to re-run the `SelfSubjectAccessReview` probe over enabled sources; `0` disables. |
| `--topology-dwell` | `10m` | How long a placement breach must persist before the topology-drift source raises a finding. |
| `--topology-per-domain-series` | `false` | Export the per-domain metric breakdown for every tracked subject rather than only the drifting ones. |
| `--compute-class-window` | `1h` | How much recent history a compute class's rank shares are computed over. |
| `--compute-class-dwell` | `10m` | How long a rank verdict must persist before the compute-class source raises a finding. |
| `--compute-class-rank0-floor` | `0` | Raise `leeway.rank_degraded` when less than this fraction of windowed pod-time ran at rank 0 (`0` disables). |
| `--compute-class-last-rank-ceiling` | `0.9` | Raise `leeway.rank_degraded` when more than this fraction of windowed pod-time ran at the least-preferred rank (`0` disables). |
| `--webhook-url` | `""` | Shorthand for `--sink=webhook --sink-url=<url>`. |
| `--sink` | `none` | Incident sink: `webhook`, `core-agent`, or `none` (defaults to `webhook` when `--webhook-url` or `--sink-url` is set). |
| `--sink-url` | `""` | Receiver base URL for `--sink=webhook` or `--sink=core-agent` (no trailing slash). |
| `--sink-token-env` | `""` | Environment variable holding the Bearer token for the sink. |
| `--dry-run` | `false` | Log outbound incident and resolution payloads to stderr instead of POSTing them. |
| `--dedup-window` | `15m` | Rolling deduplication window for emitted incidents. |
| `--recovery-stable-for` | `2m` | How long a cleared episode must stay clear before a `kind=resolved` record is emitted. |
| `--store` | `""` | SQLite file path for persisting dwell timers and learned baselines across restarts (empty keeps them in memory). |
| `--exit-after` | `0` | Shut down cleanly after this duration (`0` runs until `SIGINT`/`SIGTERM`). |
| `--version` | `false` | Print the version and exit. |
