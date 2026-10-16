# Changelog

All notable changes to `leeway` are documented in this file.

## [Unreleased]

## [0.1.0] - 2026-10-16

Initial standalone release of `leeway` (`github.com/go-steer/leeway`), extracted from `k8s-lookout` as a dedicated Kubernetes topology placement-drift and ComputeClass priority-rank analyzer and incident daemon.

### Added

- **Pure Scoring & Attribution Engine (`pkg/leeway`)**: Zero-Kubernetes-dependency placement drift and ComputeClass priority-rank evaluation engine (`Intent`, `Score`, `Judge`, `Attribute`, `AlertTable`, `BaselineTable`, `Finding`, `RankTracker`, `RankFinding`).
- **One-Shot Multi-Cluster CLI Mode (`leeway report`)**: Point-in-time snapshot evaluators (`topologydrift.EvaluateSnapshot` and `computeclass.EvaluateSnapshot`) and concurrent multi-cluster collector (`pkg/report`) producing structured `leeway.report/v1alpha1` JSON reports across `--clusters`, `--clusters-from`, `--profiles-dir`, `--kubeconfig`, `--context`, and `--in-cluster`.
- **Continuous Daemon Mode (`leeway watch`)**: Single- and multi-cluster shared informer loop with SQLite state persistence (`--store`), Prometheus `/metrics`, `/healthz`, `/readyz`, OTLP metric push (`--otel-exporter=otlp`), and HTTP webhook incident/resolution callbacks (`--webhook-url` / `--sink=webhook --sink-url=...`) carrying structured `leeway_finding` and `leeway_rank_finding` payloads.
- **Deployment & Observability Assets**: `LeewayPolicy` and `ClusterLeewayPolicy` CRDs (`deploy/crds/leewaypolicies.yaml`), Kustomize deployment manifests (`deploy/`), and prebuilt Grafana dashboard (`deploy/dashboards/leeway.json`).
