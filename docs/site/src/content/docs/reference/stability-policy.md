---
title: Stability policy
description: Compatibility guarantees for leeway.report/v1alpha1, deterministic finding identity, the pkg/leeway Go API, and leeway report CLI flags and exit codes.
sidebar:
  order: 5
---

Governs the four external compatibility surfaces of `github.com/go-steer/leeway` — the `leeway.report/v1alpha1` JSON wire schema, cross-sweep finding identity, the exported `pkg/leeway` Go API, and `leeway report` CLI flags and exit codes — pre-1.0, and names the rule that takes over at 1.0. The canonical repository copy lives at `docs/stability-policy.md`.

## Consumers governed by this policy

| Consumer | Surface it depends on |
| --- | --- |
| `kube-agents` | `leeway report` (`leeway.report/v1alpha1` JSON), 7-day finding deduplication ledger (`leeway_watch.py`), `topology-drift` skills, and `leeway watch` incident webhooks |
| `shipmind` | `leeway` CLI pinned in `versions.yaml`, `leeway report` JSON, and deterministic finding identity for the scheduled posture ledger |
| `k8s-lookout` | `github.com/go-steer/leeway/pkg/leeway` Go module API (`DecodeComputeClass` and static `ComputeClass` types in `lookout audit computeclass`; `LatestScaleUp`, `IsScaleUpReason`, `ScaleUpVerdict`, `AutoscalerEvent`, and `Reason*ScaleUp` constants in `pkg/sources/capacity`) |

---

## 1. Report schema (`leeway.report/v1alpha1`)

### Wire version decision

`leeway report` keeps `schema_version: "leeway.report/v1alpha1"` (`report.SchemaVersion`) as its current wire version so existing consumers that check `schema_version == "leeway.report/v1alpha1"` continue working unbroken.

### Additive-only evolution within `v1alpha1`

- Envelope structs (`Report`, `Summary`, `ClusterReport`, `DomainSummary`) use `snake_case`; embedded finding and `ComputeClass` objects (`Finding`, `SubjectRef`, `FindingScore`, `FindingIntent`, `FindingDomain`, `FindingRemediation`, `RankFinding`, `RankObservation`, `RankBaseline`, `RankShare`, `ClassSummary`) use `camelCase`. See [Finding kinds and JSON schemas](/reference/finding-kinds/) for full examples.
- **No removals, renames, re-casing, or type changes.** Every field and wire enum string (`status`, `kind`, `tier`, `severity`, `suspectedCause`, `intent.source`, `intent.confidence`, `intent.weighting`, `remediation.kind`, `rule`, `ordering`, `scaleUp`) is pinned in `pkg/report/schema_stability_test.go`.
- **Additive optional fields only.** New fields added within `leeway.report/v1alpha1` must carry `omitempty` and preserve existing field semantics.

### Dual-version rule before any `leeway.report/v1` cut

Promoting to `leeway.report/v1` requires:
1. Shipping opt-in `--schema-version=leeway.report/v1` while keeping `leeway.report/v1alpha1` as the default (and accepting `--schema-version=leeway.report/v1alpha1` explicitly), announced under `CHANGELOG.md` `### Deprecated`.
2. Retaining `leeway.report/v1alpha1` as the default for **both `>= 2` minor releases AND `>= 60` calendar days** pre-1.0 (**`>= 90` calendar days** at 1.0+).
3. Keeping `--schema-version=leeway.report/v1alpha1` selectable for an additional `>= 2` minor releases and `>= 60` calendar days after the default flips to `v1`.

---

## 2. Finding identity (cross-sweep deduplication key)

Posture and weekly refresh ledgers deduplicate findings across `leeway report` runs with a six-part `/` key (implemented in Go by `report.CanonicalCluster`, `leeway.TopologyFindingKey`, and `leeway.RankFindingKey`):

1. **Canonical `<cluster>`:** If `clusterReport.source` is `gke:<project>/<location>/<cluster>` (three non-empty `/` segments), use `<project>/<location>/<cluster>`; otherwise use `clusterReport.cluster`.
2. **`topology-drift` key:**
   ```text
   <cluster>/<namespace>/<subject.kind>/<subject.name>/<topologyKey>/<kind>
   ```
   Cluster-scoped subjects (`Domain` on `leeway.domain_unavailable`, `NodeGroup`, cluster-scoped `Custom`) use `_cluster` (`leeway.ClusterWideNamespace`) in `<namespace>`.
3. **`compute-class` key:**
   ```text
   <cluster>/_cluster/<objectKind>/<subject.name>/<ruleKey>/<kind>
   ```
   `<objectKind>` defaults to `ComputeClass`; `<ruleKey>` is `rule:<rule>` on axis-wide rules (`wedged`, `rank0-share`, `last-rank`, `no-migration`, `rank-baseline`, or when `focus` is null / `< 0`), `rule:<rule>/<focus>` when `focus != null && focus >= 0` (`leeway.rank_tier_unused`, e.g. `rule:tier-unused/0` or `rule:tier-unused/1` — check `!= null && >= 0`, as tier `0` is valid), and `compute-class` if `rule` is empty. (One-shot `leeway report` sweeps run without multi-window history, so `leeway.rank_tier_unused`, `rank-baseline`, and `leeway.baseline_breach` only fire under `leeway watch`.)

Mutable episode fields (`suspectedCause`, `severity`, `score.*`, `domains[]`, `remediation.*`, `specHash`, `observed.*`, `ranks[]`, `firstSeenAt`) stay outside the key so cause changes and severity escalations update an active ledger entry in place rather than opening a duplicate row. When `clusterReport.status == "error"`, consumers retain existing ledger entries for that cluster until a later sweep succeeds.

---

## 3. `pkg/leeway` Go API

`github.com/go-steer/leeway/pkg/leeway` has zero transitive `k8s.io/client-go`, `internal/...`, `pkg/sources`, or `pkg/store` dependencies (`pkg/leeway/purity_test.go`).

- **Static `ComputeClass` spec decoding:** `DecodeComputeClass`, `ComputeClass` (`Axis`, `ScaleUp`, `OptimizeRulePriority`, `Scorable`), `PreferenceAxis` (`Key`, `Ordering`, `Rules`, `Tiers`, `SpecHash`, `NewPreferenceAxis`, `Scorable`), `AxisKey`, `Provider` (`ProviderGKEComputeClass`), `PreferenceRule` (`Index`, `Score`, `Rank`, `Raw`, `Peer`, `Render`), `OrderingMode`, `Rank` (`RankUnknown`, `RankUnsatisfiable`, `RankOffAxis`, `IsTier`), and `ScaleUpPolicy`.
- **Autoscaler scale-up verdicts:** `ReasonTriggeredScaleUp`, `ReasonNotTriggerScaleUp`, `ReasonFailedScaleUp`, `IsScaleUpReason`, `ScaleUpVerdict` (`ScaleUpUnheard`, `ScaleUpProvisioning`, `ScaleUpDeclined`), `AutoscalerEvent`, and `LatestScaleUp`.
- **Pure scoring, attribution, remediation & identity:** `SubjectRef`, `ParseSubjectRef`, `TopologyKey`, `Domain`, `DomainUnknown`, `NormaliseDomain`, `SortDomains`, `Distribution`, `NewDistribution`, `Intent`, `ResolveIntents`, `NodeView`, `EligibilityOptions`, `DefaultEligibilityOptions`, `EligibleDomains`, `Uncapped`, `Apportionment`, `Apportion`, `ApportionAligned`, `Thresholds`, `DefaultThresholds`, `ReportThresholds`, `Scores`, `Score`, `(*Scores).Judge`, `Tier`, `Verdict`, `SuspectedCause`, `DomainFacts`, `Evidence`, `CauseConfig`, `DefaultCauseConfig`, `Attribution`, `(*Scores).Attribute`, `Finding`, `NewFinding`, `DomainAvailability`, `NewDomainFinding`, `FindingRemediation`, `SuggestRemediation`, `PlatformManagedRemediation`, `RankFinding`, `NewRankFinding`, `RankMessage`, `ClusterWideNamespace`, `TopologyFindingKey`, and `RankFindingKey`.

**Deprecation window:** Breaking any guaranteed `pkg/leeway` symbol requires keeping a working `// Deprecated:` shim for **both `>= 2` minor releases AND `>= 60` calendar days** pre-1.0 (`>= 90` calendar days at 1.0+) from the release carrying the `CHANGELOG.md` `### Deprecated` notice.

---

## 4. `leeway report` CLI flags and exit codes

All 22 shipped `leeway report` options (23 flag names including `-o`, see [CLI reference](/reference/cli/)) and the exit codes below are **Frozen** (`cmd/leeway/report_flags_stability_test.go`). Breaking flag changes on future Stable flags require `>= 2` minor releases and `>= 60` calendar days with warnings written strictly to `stderr` (`stdout` is reserved for the JSON payload). Default changes require a `CHANGELOG.md` `### Changed` entry and keep the prior behavior reachable by flag.

| Exit code | Meaning |
| --- | --- |
| `0` | Report written (including when findings exist or when individual clusters report `"status": "error"` inside `clusters[]`), or `-h` / `--help` |
| `1` | Runtime failure outside per-cluster collection (target discovery error, JSON marshal failure, or output write failure) |
| `2` | CLI flag or argument validation error |
