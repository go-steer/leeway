---
title: Observability and Grafana dashboard
description: Health probes, Prometheus SLIs, OTLP push export, and the prebuilt Grafana dashboard.
sidebar:
  order: 4
---

## HTTP endpoints (`leeway watch`)

`leeway watch` serves three HTTP endpoints on `--metrics-addr` (default `:9090`; an empty value disables the listener):

- **`/metrics`**: Prometheus scrape endpoint containing all `lookout_leeway_*` series plus `lookout_leeway_standalone_info` and `lookout_leeway_standalone_signals_total`.
- **`/healthz`**: Liveness probe (`200 ok`). Deliberately independent of apiserver connectivity so transient control-plane hiccups do not cause pod restart loops.
- **`/readyz`**: Readiness probe. Returns `503` naming the unsynced source until every informer's initial `LIST` has completed and the initial evaluation pass has populated the gauges.

## Pushing metrics over OTLP

Pass `--otel-exporter=otlp` to enable a periodic OTLP/HTTP metric push reader alongside `/metrics`, configured with standard OpenTelemetry environment variables (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_METRIC_EXPORT_INTERVAL`).

## The four measurement-soundness SLIs

Before trusting any drift graph, verify these four self-checking SLIs (they form the top row of the Grafana dashboard):

| Metric | Expected | Meaning when non-zero |
| --- | --- | --- |
| `lookout_leeway_counter_mismatch_total` | `0` | Incremental pod-counter state disagreed with a full cache rebuild (repaired in place). |
| `lookout_leeway_preference_disagreement` | `0` | Node's `cloud.google.com/compute-class-priority-rank` annotation disagreed with `leeway`'s rule inference. |
| `lookout_leeway_transient_subjects` | Context | Subject-axes currently suppressed or relaxed due to a rollout, scale event, or domain outage. |
| `lookout_leeway_domains_unavailable` | Context | Domains with zero ready nodes per topology key. |

## Grafana dashboard

`deploy/dashboards/leeway.json` is a complete Grafana dashboard covering the measurement SLIs, topology drift, ComputeClass priority ranks, and learned baselines.

Import `deploy/dashboards/leeway.json` directly in Grafana, or apply the sidecar ConfigMap with Kustomize:

```sh
kubectl apply -k deploy/dashboards/
```
