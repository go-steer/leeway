# Changelog

All notable changes to `leeway` are documented in this file.

## [Unreleased]

### Fixed

- **Docs Drift Against the Code (`docs/site/src/content/docs/`)**: `agents.md`, `reference/finding-kinds.md`, `reference/cli.md`, `getting-started/report.md`, `concepts/scoring-and-attribution.md` and `concepts/tiers-and-intent.md` now match `cmd/leeway` and `pkg/leeway`: the report envelope is documented as snake_case (`schema_version`, `topology_findings`, `rank_findings`, `domains`, `summary.clusters_total`, …); `suspectedCause` lists the real vocabulary (`domain_outage`, `domain_capacity_shortfall`, `taint_exclusion`, `consolidation`, `volume_pinning`, `rollout_bias`, `constraint_ignored`, `unknown`) instead of names the binary never prints; `intent.source` / `confidence` / `weighting`, `remediation.kind` and the rank-finding fields are listed as emitted; the `report` flags are `--parallelism`, `--pretty`, `--namespace`, `--all`, `--min-drift` (there is no `--concurrency` or `--compact`) and `--clusters-from` takes `kubeconfig[:/path]`, `profiles:/path` or `<project>[/<location>]`, not a file; the `watch` flag table lists only flags the binary defines; the intent precedence list follows `IntentSource` order. All JSON examples are taken from a real `leeway report` run. `concepts/scoring-and-attribution.md` also explains why `excessSkew` can exceed `observedSkew` in `leeway report` (Pending/Unschedulable replicas are counted into the excess), and `TestCollect_CauseAttribution` pins that behaviour. The same corrections are applied to `README.md`, `index.mdx`, `getting-started/deploy.md`, `operations/observability.md`, `operations/tuning.md`, `operations/webhooks-and-sinks.md` and `reference/metrics.md`: `--metrics-addr` (not `--listen-addr`), `--cluster-name` (not `--cluster`), `--recovery-stable-for` default `2m` (not `--recovery-window 5m`), `--dedup-window` default `15m`; the `--topology-capacity-ratio`, `--topology-learn-baselines`, `--topology-baseline-*`, `--topology-per-domain-{min-drift,namespaces,exclude-namespaces,max-keys,collapse-states}` and `--topology-max-node-groups` rows are removed because the binary defines no such flags (their values are fixed defaults); the webhook open and resolved payloads are replaced with bodies captured from the `test/e2e` webhook receiver (no `schema_version`, `object_kind`, `object_name`, `summary`, `incident_id` or `*_seconds` fields exist on the wire).

### Changed

- **Conflicting-Affinity Remediation Payload (`pkg/leeway/remediation.go`)**: When a `topologySpreadConstraint` is contradicted by a `nodeAffinity`/`nodeSelector` on the same key, the `manifest_patch` remediation's `summary` now states that the affinity must be removed or widened first and that the `yaml` alone changes nothing, and the `yaml` opens with a comment saying the same. The finding never learns which selector entry to delete, so the removal cannot be rendered as a patch; the schema (`kind`, `summary`, `command`, `yaml`) is unchanged.
- **Report Test Fixture Names (`pkg/report/report_test.go`, `pkg/leeway/remediation_test.go`)**: Renamed the cause-attribution fixture (`TestCollect_SeededD` → `TestCollect_CauseAttribution`) and its workloads to names that describe the mechanism under test (`affinity-pinned-api`, `zonal-pv-store`, `cpu-starved-worker`, `pool-pinned-filler`, namespace `topology`) instead of a downstream evaluation fixture's names. Scenarios are unchanged.

## [0.1.3] - 2026-10-19

### Added

- **Snapshot Report Thresholds & Causal Tier-C Surfacing (`pkg/leeway/score.go`, `pkg/sources/topologydrift/snapshot.go`)**: Added `ReportThresholds()` (`MinReplicasForScoring: 2`, `MinRelocationSmallN: 1`) for one-shot `leeway report` evaluations and surfaced breached Tier-C workload findings when positive causal attribution (`volume_pinning`, `domain_capacity_shortfall`, etc.) is present.
- **Conflicting `nodeAffinity` / `nodeSelector` Detection on Spread Axes (`pkg/sources/topologydrift/predicate.go`, `pkg/sources/topologydrift/resolve.go`, `pkg/leeway/remediation.go`)**: Detected workloads that declare a `Spread` `topologySpreadConstraint` on an axis while simultaneously pinning placement to a single domain on that same axis via `nodeSelector` or required `nodeAffinity`, emitting `manifest_patch` remediation.
- **Cloud CSI Volume Topology Label Aliases (`pkg/sources/topologydrift/pinning.go`)**: Recognised provider-specific CSI volume node-affinity keys (`topology.gke.io/zone`, `topology.ebs.csi.aws.com/zone`, `topology.disk.csi.azure.com/zone`, `topology.gke.io/region`) when scoring standard zone/region axes.
- **Unschedulable Shortfall Scoring for `DoNotSchedule` Spread Constraints (`pkg/sources/topologydrift/score.go`)**: Accounted for `Pending` (`Unschedulable`) pods blocked by a `DoNotSchedule` spread constraint and single-zone capacity starvation when only 1–2 replicas are `Running`.
- **Credential Proxy Shell Sandbox (`CREDENTIAL_PROXY_URL`) Support (`pkg/clusters`, `pkg/report`)**: Added `ShellMode` broker discovery and `kubectl`-backed snapshot collection so `leeway report` operates credential-free inside `kube-agents`'s `agent-sandbox` container via the `/opt/credential-proxy/bin/kubectl` shim.

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
