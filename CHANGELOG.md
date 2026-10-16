# Changelog

All notable changes to `leeway` are documented in this file.

## [Unreleased]

### Added

- Extracted `leeway` into a standalone Go module (`github.com/go-steer/leeway`) and binary from `k8s-lookout`.
- Added `leeway report` one-shot multi-cluster CLI mode (`--clusters`, `--clusters-from`, `--profiles-dir`, `--kubeconfig`, `--context`, `--in-cluster`) producing a structured JSON report (`leeway.report/v1alpha1`) with fleet summary rollups, per-cluster domain availability census, topology skew findings, and ComputeClass priority rank findings.
- Added `leeway watch` continuous daemon mode with multi-cluster informer support, SQLite state persistence (`--store`), Prometheus `/metrics`, `/healthz`, `/readyz`, and HTTP webhook incident/resolution callbacks (`--webhook-url` / `--sink=webhook --sink-url=...`) carrying structured `leeway_finding` and `leeway_rank_finding` objects.
