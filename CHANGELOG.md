# Changelog

All notable changes to `leeway` are documented in this file.

## [Unreleased]

## [0.1.2] - 2026-10-19

### Added

- **Deterministic Remediation Recommendations (`pkg/leeway/remediation.go`)**: Added `Finding.Remediation` (`manifest_patch`, `rollout_restart`, `cluster_capacity`, `cluster_nodes`, `storage_topology`) with ready-to-apply `topologySpreadConstraints` YAML snippets and `kubectl rollout restart` commands on topology-drift findings.

### Changed

- **Informer & Report Object Trimming (`pkg/kube/transform.go`, `pkg/report/collector.go`)**: Stripped `spec.containers`, `spec.initContainers`, `spec.ephemeralContainers`, `status.containerStatuses`, `status.initContainerStatuses`, `status.ephemeralContainerStatuses`, and non-`Ready` node conditions on ingest across both `leeway watch` and `leeway report`, reducing per-Pod memory footprint by ~5–8x.

## [0.1.1] - 2026-10-16

### Added

- **Drill & E2E Tunability Flags (`cmd/leeway`)**: Added `--topology-domain-unavailable-keys`, `--topology-dwell`, `--compute-class-window`, `--compute-class-dwell`, `--compute-class-rank0-floor`, and `--compute-class-last-rank-ceiling` to `leeway watch` and `leeway report`, automatically scaling evaluation/coalesce intervals when short drill dwells are configured.
- **3-Tier End-to-End & Scale Suite (`test/e2e`, `examples/`, `dev/drills/`)**:
  - In-repo Go binary E2E suite (`test/e2e/e2e_test.go`) exercising `leeway report` and `leeway watch` subprocesses against live mock Kubernetes API servers, HTTP webhook receivers, and SQLite `--store`.
  - Hermetic multi-zone `kind` E2E harness (`examples/kind/`, `examples/watcher/`, `examples/scenarios/`, `examples/e2e`, `.github/workflows/e2e-kind.yml`) covering `placement-drift`, `zone-unavailable`, and `compute-class-fallback`.
  - KWOK synthetic 5,000-pod scale and memory regression guard (`examples/kwok/`, `.github/workflows/scale-kwok.yml`) and live GKE NAP drill runbook (`dev/drills/compute-class-fallback.md`).

### Fixed

- **Kustomize Watcher Deployment Flags (`deploy/51-deployment-watcher.yaml`)**: Updated default container args (`--cluster-name=local`, `--metrics-addr=:9090`) to match `cmd/leeway`.

## [0.1.0] - 2026-10-16

Initial standalone release of `leeway` (`github.com/go-steer/leeway`), extracted from `k8s-lookout` as a dedicated Kubernetes topology placement-drift and ComputeClass priority-rank analyzer and incident daemon.

### Added

- **Pure Scoring & Attribution Engine (`pkg/leeway`)**: Zero-Kubernetes-dependency placement drift and ComputeClass priority-rank evaluation engine (`Intent`, `Score`, `Judge`, `Attribute`, `AlertTable`, `BaselineTable`, `Finding`, `RankTracker`, `RankFinding`).
- **One-Shot Multi-Cluster CLI Mode (`leeway report`)**: Point-in-time snapshot evaluators (`topologydrift.EvaluateSnapshot` and `computeclass.EvaluateSnapshot`) and concurrent multi-cluster collector (`pkg/report`) producing structured `leeway.report/v1alpha1` JSON reports across `--clusters`, `--clusters-from`, `--profiles-dir`, `--kubeconfig`, `--context`, and `--in-cluster`.
- **Continuous Daemon Mode (`leeway watch`)**: Single- and multi-cluster shared informer loop with SQLite state persistence (`--store`), Prometheus `/metrics`, `/healthz`, `/readyz`, OTLP metric push (`--otel-exporter=otlp`), and HTTP webhook incident/resolution callbacks (`--webhook-url` / `--sink=webhook --sink-url=...`) carrying structured `leeway_finding` and `leeway_rank_finding` payloads.
- **Deployment & Observability Assets**: `LeewayPolicy` and `ClusterLeewayPolicy` CRDs (`deploy/crds/leewaypolicies.yaml`), Kustomize deployment manifests (`deploy/`), and prebuilt Grafana dashboard (`deploy/dashboards/leeway.json`).
