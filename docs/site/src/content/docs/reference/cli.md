---
title: CLI reference
description: Subcommands and flags for leeway report, leeway watch, and leeway version.
sidebar:
  order: 2
---

`leeway` provides three subcommands:
- `leeway report [flags]` — Run a one-shot placement and priority-rank evaluation across one or more clusters and emit JSON.
- `leeway watch [flags]` — Run the continuous daemon with informers, `/metrics`, `/healthz`, `/readyz`, and optional webhook delivery. (Also the default when invoked with bare flags.)
- `leeway version` — Print the build version, git commit, and build timestamp.

## Cluster discovery flags (shared by `report` and `watch`)

| Flag | Default | Description |
| --- | --- | --- |
| `--clusters` | `""` | Comma-separated cluster targets (`name=https://host:port`, `https://host:port`, or profile name). |
| `--clusters-from` | `""` | Path to file with one cluster target per line (`#` comments allowed). |
| `--profiles-dir` | `$KUBE_AGENTS_PROFILES_DIR` | Directory containing `kube-agents` cluster profile subdirectories. |
| `--kubeconfig` | `$KUBECONFIG` | Path to kubeconfig file (fallback when no multi-cluster targets are set). |
| `--context` | `""` | Kubeconfig context override. |
| `--in-cluster` | `false` | Use in-cluster service account credentials. |
| `--sources` | `topology-drift,compute-class` | Comma-separated sources to enable (`topology-drift`, `compute-class`). |

## `leeway report` flags

| Flag | Default | Description |
| --- | --- | --- |
| `--concurrency` | `8` | Maximum number of clusters evaluated concurrently. |
| `--timeout` | `60s` | Per-cluster snapshot and evaluation timeout. |
| `--output`, `-o` | `""` (stdout) | Write JSON report to file path instead of `stdout`. |
| `--compact` | `false` | Emit compact single-line JSON instead of indented JSON. |

## `leeway watch` daemon & sink flags

| Flag | Default | Description |
| --- | --- | --- |
| `--cluster`, `--cluster-name` | `""` | Logical cluster name for single-cluster mode (stamped on `cluster` metric label and webhook payloads). |
| `--listen-addr`, `--metrics-addr` | `:9090` | Listen address for `/metrics`, `/healthz`, and `/readyz`. |
| `--webhook-url` | `""` | Shorthand for `--sink=webhook --sink-url=<url>`. |
| `--sink` | `none` | Incident sink (`none` or `webhook`). |
| `--sink-url` | `""` | Base URL for the webhook sink (`POST <url>/incidents`). |
| `--sink-token-env` | `""` | Environment variable name holding the webhook Bearer token. |
| `--dry-run` | `false` | Log webhook payloads without sending HTTP requests. |
| `--dedup-window` | `10m` | Deduplication window for identical `(UID, Reason)` incidents. |
| `--recovery-window` | `5m` | Stability window before emitting a `resolved` webhook event (`0` disables). |
| `--store` | `""` | Path to SQLite database for persisting dwell state, learned baselines, and dedup bindings (`:memory:` or empty for in-memory). |
| `--otel-exporter` | `none` | OpenTelemetry metric push exporter (`none` or `otlp`). |
| `--exit-after` | `0` | Exit cleanly after this duration (`0` runs until `SIGINT`/`SIGTERM`). |

## Scoring & cardinality flags (shared by `report` and `watch`)

| Flag | Default | Description |
| --- | --- | --- |
| `--topology-keys` | `topology.kubernetes.io/zone,topology.kubernetes.io/region` | Comma-separated topology keys scored by default. |
| `--topology-dwell` | `10m` | Dwell time before a topology breach fires in `watch` mode (always `0` in `report` mode). |
| `--topology-capacity-ratio` | `1.25` | Domain CPU ratio threshold for switching from uniform to capacity-weighted expectation. |
| `--topology-cluster-defaults` | `""` | Assumed `kube-scheduler` default constraints (`key:maxSkew:ScheduleAnyway,...`). |
| `--topology-learn-baselines` | `true` | Learn EWMA baselines for workloads without declared constraints (`watch` mode). |
| `--topology-baseline-half-life` | `12h` | Half-life for EWMA baseline learning. |
| `--topology-baseline-band` | `4` | Width of the learned tolerance band in MADs. |
| `--topology-tier-c-signals` | `false` | Emit Tier C topology findings as signals. |
| `--topology-per-domain-min-drift` | `0.05` | Minimum drift required to export per-domain Prometheus series. |
| `--topology-per-domain-series` | `false` | Export per-domain Prometheus series for all subjects regardless of drift. |
| `--topology-per-domain-namespaces` | `""` | Comma-separated namespace allowlist for per-domain series. |
| `--topology-per-domain-exclude-namespaces` | `""` | Comma-separated namespace denylist for per-domain series. |
| `--topology-per-domain-max-keys` | `4` | Maximum topology keys per subject for per-domain series. |
| `--topology-per-domain-collapse-states` | `true` | Collapse 4 pod states into 2 (`running`, `other`) on per-domain series. |
| `--topology-max-node-groups` | `200` | Maximum node groups before node-group scoring is withheld. |
| `--compute-class-dwell` | `10m` | Dwell time before a ComputeClass rank finding fires in `watch` mode. |
| `--compute-class-window` | `1h` | Rolling window over which ComputeClass rank shares are evaluated. |
| `--compute-class-rank0-floor` | `0` | Minimum `rank 0` share (`(0,1]`, `0` disables). |
| `--compute-class-last-rank-ceiling` | `0.9` | Maximum last-rank (`K-1`) share (`(0,1]`, `0` disables). |
| `--compute-class-tier-c-signals` | `false` | Emit Tier C ComputeClass findings (`leeway.rank_tier_unused`) as signals. |
