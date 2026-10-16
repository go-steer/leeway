---
title: Operations
description: Day-2 operations for leeway — tuning thresholds and LeewayPolicy CRDs, configuring webhook sinks, and monitoring SLIs and dashboards.
sidebar:
  order: 1
---

1. **[Tuning placement findings](/operations/tuning/)** — How to adjust dwell windows, baseline bands, `LeewayPolicy` severity overrides, and per-domain series cardinality controls.
2. **[Webhooks and sinks](/operations/webhooks-and-sinks/)** — Configuring `--webhook-url` / `--sink=webhook`, bearer token authentication, deduplication, and resolution event callbacks.
3. **[Observability & Dashboard](/operations/observability/)** — Prometheus `/metrics`, `/healthz`, `/readyz`, OTLP metric push, and the prebuilt Grafana dashboard (`deploy/dashboards/leeway.json`).
