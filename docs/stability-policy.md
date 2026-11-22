# Stability Policy

**Status:** settled (2026-11-22, closes [#30](https://github.com/go-steer/leeway/issues/30)). Governs the four external compatibility surfaces of `github.com/go-steer/leeway` — the `leeway.report/v1alpha1` JSON wire schema, cross-sweep finding identity, the exported `pkg/leeway` Go API, and `leeway report` CLI flags and exit codes — pre-1.0, and names the rule that takes over at 1.0.

## Why this exists

`go-steer/leeway` was extracted for a single caller and now serves three independent repositories across CLI, JSON wire, and Go module boundaries:

| Consumer | Surface it depends on |
| --- | --- |
| `kube-agents` | `leeway report` (`leeway.report/v1alpha1` JSON), 7-day finding deduplication ledger (`leeway_watch.py`), `topology-drift` skills, and `leeway watch` incident webhooks |
| `shipmind` (`go-steer/shipmind#127`) | `leeway` CLI pinned in `versions.yaml`, `leeway report` JSON, and deterministic finding identity for the scheduled posture ledger |
| `k8s-lookout` (`go-steer/k8s-lookout#841`, `#842`) | `github.com/go-steer/leeway/pkg/leeway` Go module API (`DecodeComputeClass` and static `ComputeClass` types in `lookout audit computeclass`; `LatestScaleUp`, `IsScaleUpReason`, `ScaleUpVerdict`, `AutoscalerEvent`, and `Reason*ScaleUp` constants in `pkg/sources/capacity`) |

**The zero-deployed-users posture is expired.** With published GHCR images, an external docs site, and three downstream repositories pinning or parsing `leeway`, no change may break an existing wire field, finding key, guaranteed `pkg/leeway` symbol, or `leeway report` flag without following the rules below.

---

## 1. Report Schema (`leeway.report/v1alpha1`)

### 1.1 Wire version decision: stay on `leeway.report/v1alpha1`

`leeway report` keeps `schema_version: "leeway.report/v1alpha1"` (`report.SchemaVersion`) as its current wire version. Promoting the default `schema_version` string to `leeway.report/v1` today would immediately break `kube-agents` (`leeway_watch.py` and `topology-drift` skill guards assert `schema_version == "leeway.report/v1alpha1"`) for zero structural gain. Instead, `leeway.report/v1alpha1` is frozen against breaking changes under §1.3, and any future `v1` cut must follow §1.4's dual-version window.

### 1.2 Stable field inventory

Envelope objects use `snake_case`; embedded finding and `ComputeClass` summary objects use `camelCase`. Every field listed below is **Frozen** within `leeway.report/v1alpha1`.

#### Envelope (`pkg/report`, `pkg/sources/computeclass`)

| Object | JSON field | JSON type | Presence |
| --- | --- | --- | --- |
| `Report` | `schema_version` | `string` | always (`"leeway.report/v1alpha1"`) |
| `Report` | `generated_at` | `string` (RFC 3339) | always |
| `Report` | `clusters` | `ClusterReport[]` | always |
| `Report` | `summary` | `Summary` | always |
| `Summary` | `clusters_total`, `clusters_succeeded`, `clusters_failed`, `topology_findings`, `rank_findings` | `int` | always |
| `Summary` | `by_severity`, `by_tier` | `map[string]int` | `omitempty` (omitted when zero findings) |
| `ClusterReport` | `cluster` | `string` | always |
| `ClusterReport` | `source` | `string` | `omitempty` (`direct`, `kubeconfig:<ctx>`, `gke:<project>/<location>/<cluster>`, `endpoint:<name>`, or profile name) |
| `ClusterReport` | `status` | `string` | always (`"ok"` or `"error"`) |
| `ClusterReport` | `error` | `string` | `omitempty` (set when `status == "error"`) |
| `ClusterReport` | `domains` | `map[topologyKey]map[domain]DomainSummary` | `omitempty` |
| `ClusterReport` | `topology_findings` | `Finding[]` | `omitempty` |
| `ClusterReport` | `compute_classes` | `ClassSummary[]` | `omitempty` |
| `ClusterReport` | `rank_findings` | `RankFinding[]` | `omitempty` |
| `ClusterReport` | `subjects` | `Finding[]` | `omitempty` (populated with `--all` or `--min-drift > 0`) |
| `DomainSummary` | `nodes`, `ready`, `usable` | `int64` | always |
| `DomainSummary` | `allocatable_cpu_milli`, `allocatable_mem_bytes` | `int64` | `omitempty` |
| `ClassSummary` | `name` | `string` | always |
| `ClassSummary` | `scorable` | `bool` | always |
| `ClassSummary` | `ordering` | `string` | `omitempty` |
| `ClassSummary` | `podsByRank` | `map[string]int` | `omitempty` |
| `ClassSummary` | `pendingPods`, `pendingProvisioning`, `pendingDeclined` | `int` | always |

#### Topology findings (`leeway.Finding` in `topology_findings[]` and `subjects[]`)

| Object | JSON field | JSON type | Presence |
| --- | --- | --- | --- |
| `Finding` | `kind` | `string` | always (`leeway.contract_violated`, `leeway.placement_drift`, `leeway.baseline_breach`, `leeway.domain_unavailable`) |
| `Finding` | `subject` | `SubjectRef` | always (`kind`, optional `namespace` with `omitempty`, `name`) |
| `Finding` | `topologyKey` | `string` | always |
| `Finding` | `tier` | `string` | always (`"A"`, `"B"`, `"C"`, or `""` on non-breaching `subjects[]` entries) |
| `Finding` | `severity` | `string` | always (`"critical"`, `"warning"`, `"info"`, or `""` on non-breaching `subjects[]` entries) |
| `Finding` | `score` | `FindingScore` | always (`drift`, `observedSkew`, `minAchievableSkew`, `excessSkew`, `relocationDistance`, `maxDomainShare`) |
| `Finding` | `intent` | `FindingIntent` | `omitempty` (`source`, `confidence`, `weighting`, optional `maxSkew`, optional `evidence[]`) |
| `Finding` | `domains` | `FindingDomain[]` | always (`domain`, `actual`, `expected`, `delta`, `readyNodes`, optional `note`) |
| `Finding` | `suspectedCause` | `string` | always (`unknown`, `domain_outage`, `domain_capacity_shortfall`, `taint_exclusion`, `consolidation`, `volume_pinning`, `rollout_bias`, `constraint_ignored`, `selector_confined`) |
| `Finding` | `contributingFactors` | `string[]` | `omitempty` |
| `Finding` | `untestedCauses` | `string[]` | `omitempty` |
| `Finding` | `firstSeenAt` | `string` (RFC 3339) | always |
| `Finding` | `transient` | `string` | `omitempty` |
| `Finding` | `relaxed`, `severityFromPolicy` | `bool` | `omitempty` |
| `Finding` | `remediation` | `FindingRemediation` | `omitempty` (`kind` in `manifest_patch` \| `rollout_restart` \| `cluster_capacity` \| `cluster_nodes` \| `storage_topology` \| `platform_managed`; `summary`; optional `command`; optional `yaml`) |

#### ComputeClass priority-rank findings (`leeway.RankFinding` in `rank_findings[]`)

| Object | JSON field | JSON type | Presence |
| --- | --- | --- | --- |
| `RankFinding` | `kind` | `string` | always (`leeway.rank_wedged`, `leeway.rank_degraded`, `leeway.rank_no_migration`, `leeway.rank_tier_unused`) |
| `RankFinding` | `subject` | `SubjectRef` | always (`{kind: "PreferenceAxis", name: "<class>"}`) |
| `RankFinding` | `provider`, `objectKind`, `specHash` | `string` | always (`gke-computeclass`, `ComputeClass`, 16-hex SHA-256 prefix) |
| `RankFinding` | `tier`, `severity`, `rule` | `string` | always (`rule` in `wedged`, `rank0-share`, `last-rank`, `no-migration`, `tier-unused`, `rank-baseline`) |
| `RankFinding` | `focus` | `int` | `omitempty` (present on per-tier findings such as `leeway.rank_tier_unused`) |
| `RankFinding` | `observed` | `RankObservation` | always (`windowSeconds`, `tierSeconds`, `rank0Share`, `lastRank`, `lastRankShare`, `meanRank`, `pendingPods`, `pendingProvisioning`, `pendingDeclined`, `degradedPods`, `improving`, `worsening`, `lateral`, optional `baseline` `{mean, band, samples, firstSeen}`) |
| `RankFinding` | `ranks` | `RankShare[]` | always (`rank`, optional `rules`, `podSeconds`, `share`, `pods`) |
| `RankFinding` | `ordering` | `string` | always (`list-position`, `priority-score`, `invalid`) |
| `RankFinding` | `tiers` | `int` | always |
| `RankFinding` | `scaleUp` | `string` | always (`unset`, `scale-up-anyway`, `do-not-scale-up`, `unrecognised`) |
| `RankFinding` | `optimizeRulePriority` | `bool` | always |
| `RankFinding` | `reason`, `firstSeenAt` | `string` | always |

### 1.3 Additive-only evolution within `v1alpha1`

1. **No removals, renames, re-casing, or type changes.** Every field and string enum constant in §1.2 is pinned by `pkg/report/schema_stability_test.go`. Removing a field, changing `snake_case` / `camelCase` spelling, changing a field's JSON type, or flipping its `omitempty` presence fails CI.
2. **Additive optional fields only.** New fields added within `leeway.report/v1alpha1` must carry `omitempty`, leave existing fields' semantics unchanged, and be added to §1.2 and `schema_stability_test.go` in the same PR.
3. **Enum extensions.** Existing vocabulary strings (`kind`, `suspectedCause`, `intent.source`, `remediation.kind`, `rule`, `scaleUp`, `ordering`, `status`) never change spelling. New enum values may be added only for genuinely new detections or causes and must be documented under `CHANGELOG.md` `### Added`; consumers should fall back cleanly on unknown string values.

### 1.4 Dual-version and deprecation rule before any `leeway.report/v1` cut

Promoting `leeway report` to `leeway.report/v1` (or introducing any non-additive wire change) requires a two-step transition:

1. **Dual-version opt-in window:** Ship `--schema-version=leeway.report/v1` while keeping `leeway.report/v1alpha1` as the default (and accepting `--schema-version=leeway.report/v1alpha1` explicitly). Announce the deprecation of `v1alpha1` in `CHANGELOG.md` under `### Deprecated`.
2. **Minimum window before flipping the default:** Keep `leeway.report/v1alpha1` as the default for **both `>= 2` minor releases AND `>= 60` calendar days** pre-1.0 (**`>= 90` calendar days** at 1.0+), measured from the release date of the version that first shipped `--schema-version=leeway.report/v1`.
3. **Post-flip retention:** After flipping the default to `leeway.report/v1`, keep `--schema-version=leeway.report/v1alpha1` working for at least another `>= 2` minor releases and `>= 60` calendar days before removing it under `CHANGELOG.md` `### Removed`.

---

## 2. Finding Identity (Cross-Sweep Deduplication Key)

Scheduled posture consumers (`shipmind`'s posture ledger and `kube-agents`'s 7-day `ledger.json`) deduplicate findings across independent `leeway report` runs using a deterministic six-part `/` key. Go callers can compute the exact key via `report.CanonicalCluster` / `ClusterReport.CanonicalCluster`, `leeway.TopologyFindingKey` / `ClusterReport.TopologyFindingKey`, and `leeway.RankFindingKey` / `ClusterReport.RankFindingKey`.

### 2.1 Canonical cluster segment (`<cluster>`)

Resolve `<cluster>` from each `ClusterReport` before keying its findings:

1. If `clusterReport.source` starts with `"gke:"` followed by `<project>/<location>/<cluster>` (three non-empty `/`-separated segments), `<cluster>` is `<project>/<location>/<cluster>` (so two GKE clusters named `prod` in different projects or regions never collide).
2. Otherwise, `<cluster>` is `clusterReport.cluster`.

> **Note on `/` inside segments:** Both `<cluster>` (e.g. `p1/us-central1/c1`) and `<topologyKey>` (e.g. `topology.kubernetes.io/zone`) legitimately contain `/`. Treat the rendered key as an opaque UTF-8 map key rather than splitting on `/` with a fixed field count.

### 2.2 `topology-drift` finding key

```text
<cluster>/<namespace>/<subject.kind>/<subject.name>/<topologyKey>/<kind>
```

Normalization rules (`leeway.TopologyFindingKey`):
- `<namespace>`: `finding.subject.namespace` when non-empty; `"_cluster"` (`leeway.ClusterWideNamespace`) when empty (cluster-scoped `Domain`, `NodeGroup`, or `Custom` subjects).
- `<subject.kind>`: `finding.subject.kind` (fallback `"Cluster"` if empty).
- `<subject.name>`: `finding.subject.name` (fallback `finding.kind`, else `"unknown"`).
- `<topologyKey>`: `finding.topologyKey` (fallback `"unknown"`).
- `<kind>`: `finding.kind` (fallback `"unknown"`).

Examples:
- Namespaced workload: `p1/us-central1/c1/shop/Deployment/batch-worker/topology.kubernetes.io/zone/leeway.contract_violated`
- Cluster-scoped domain outage/cordon: `p1/us-east1/c1/_cluster/Domain/us-east1-c/topology.kubernetes.io/zone/leeway.domain_unavailable`
- Cluster-scoped node group: `p1/us-east1/c1/_cluster/NodeGroup/spot-pool/topology.kubernetes.io/zone/leeway.placement_drift`

### 2.3 `compute-class` finding key

```text
<cluster>/_cluster/<objectKind>/<subject.name>/<ruleKey>/<kind>
```

Normalization rules (`leeway.RankFindingKey`):
- `<namespace>`: always `"_cluster"` (`ComputeClass` is cluster-scoped).
- `<objectKind>`: `finding.objectKind` (fallback `"ComputeClass"`).
- `<subject.name>`: `finding.subject.name` — the `ComputeClass` name (fallback `"unknown"`).
- `<ruleKey>`:
  - When `finding.rule` is non-empty and `finding.focus` is omitted, null, or `< 0` (axis-wide rules: `wedged`, `rank0-share`, `last-rank`, `no-migration`, `rank-baseline`, or non-tier sentinel `Rank` values): `"rule:" + finding.rule` (e.g. `rule:last-rank`). Including `<rule>` separates `rank0-share` and `last-rank`, which share `kind: "leeway.rank_degraded"` on the same `ComputeClass`.
  - When `finding.rule` is non-empty and `finding.focus != null && finding.focus >= 0` (per-tier rules: `leeway.rank_tier_unused`; note that `0` is a valid tier, so non-Go consumers must check `!= null && >= 0` rather than truthiness): `"rule:" + finding.rule + "/" + strconv.Itoa(finding.focus)` (e.g. `rule:tier-unused/0`, `rule:tier-unused/1`), matching `RankVerdict.EpisodeKey()` so two unused tiers on the same `ComputeClass` do not overwrite each other. (One-shot `leeway report` sweeps evaluate a synthetic single window without multi-day history, so `leeway.rank_tier_unused`, `rank-baseline`, and `leeway.baseline_breach` only fire under continuous `leeway watch` unless multi-window state is supplied; current `leeway report` outputs therefore emit axis-wide `RankFinding`s only.)
  - When `finding.rule` is empty: `"compute-class"`.
- `<kind>`: `finding.kind` (fallback `"unknown"`).

Examples:
- Axis-wide degradation: `p1/us-central1/c1/_cluster/ComputeClass/gold/rule:last-rank/leeway.rank_degraded`
- Per-tier unused rung: `p1/us-central1/c1/_cluster/ComputeClass/gold/rule:tier-unused/1/leeway.rank_tier_unused`

### 2.4 Why mutable episode attributes stay outside the key

- **`suspectedCause`, `severity`, `score.*`, `domains[]`, `remediation.*`, `specHash`, `observed.*`, `ranks[]`, and `firstSeenAt` are excluded.** In one-shot `leeway report` sweeps `firstSeenAt` is always the sweep timestamp, and a workload whose root cause shifts (e.g. `rollout_bias` -> `volume_pinning`), whose severity escalates (`info` -> `warning`), or whose `ComputeClass` spec receives an unrelated edit is still the *same* open subject-axis episode. Ledgers store `suspectedCause` and `severity` on the entry to trigger immediate re-notification on escalation without orphaning the prior entry.
- **Unreadable clusters (`status == "error"`):** When a cluster fails collection, consumers must retain existing ledger entries for that `<cluster>` rather than retiring them as resolved, preventing re-page storms after transient control-plane timeouts.
- **Relation to `leeway watch` webhook identity:** Continuous `leeway watch` incidents carry `uid` (`leeway:<SubjectRef>|<topologyKey>` or `leeway-rank:PreferenceAxis/<name>|<provider>/<episodeKey>`) for per-object session binding and `fingerprint` (`sha256(kind || NUL || reasonClass || NUL || objectClass || NUL || failureDomain)`) for object-agnostic fleet class rollups. Do not mix the two: `fingerprint` intentionally omits workload names, while report ledgers require per-workload identity.

---

## 3. `pkg/leeway` Go API

### 3.1 Purity invariant

`github.com/go-steer/leeway/pkg/leeway` is a pure scoring and attribution library with **zero** transitive imports of `k8s.io/client-go`, `k8s.io/metrics`, `k8s.io/kubectl`, `internal/...`, `pkg/sources`, or `pkg/store` (enforced in CI by `pkg/leeway/purity_test.go` and `pkg/sources/layering_test.go`). Only `k8s.io/api` and `k8s.io/apimachinery` value types appear in its dependency closure.

### 3.2 Guaranteed exported symbols

The following `pkg/leeway` symbols are **Stable** for external Go module callers and compile-checked in `pkg/report/schema_stability_test.go`:

1. **Static `ComputeClass` spec decoding (used by `k8s-lookout` `audit computeclass`):**
   - `DecodeComputeClass(name string, spec map[string]any) (*ComputeClass, error)`
   - `ComputeClass` (`Axis *PreferenceAxis`, `ScaleUp ScaleUpPolicy`, `OptimizeRulePriority bool`, `(*ComputeClass).Scorable() bool`)
   - `PreferenceAxis` (`Key AxisKey`, `Ordering OrderingMode`, `Rules []PreferenceRule`, `Tiers int`, `SpecHash string`, `NewPreferenceAxis(AxisKey, []PreferenceRule) *PreferenceAxis`, `(*PreferenceAxis).Scorable() bool`)
   - `AxisKey` (`Provider Provider`, `Name string`, `(AxisKey).String() string`) and `Provider` (`ProviderGKEComputeClass`)
   - `PreferenceRule` (`Index int`, `Score *int`, `Rank Rank`, `Raw map[string]any`, `(PreferenceRule).Peer(PreferenceRule) bool`, `(PreferenceRule).Render() string`)
   - `OrderingMode` (`OrderingInvalid`, `ByListPosition`, `ByPriorityScore`, `(OrderingMode).String() string`)
   - `Rank` (`RankUnknown`, `RankUnsatisfiable`, `RankOffAxis`, `(Rank).IsTier() bool`, `(Rank).String() string`)
   - `ScaleUpPolicy` (`ScaleUpPolicyUnset`, `ScaleUpPolicyAnyway`, `ScaleUpPolicyDoNotScaleUp`, `ScaleUpPolicyUnrecognised`, `(ScaleUpPolicy).String() string`)

2. **Cluster Autoscaler per-pod scale-up verdicts (used by `k8s-lookout` `pkg/sources/capacity`):**
   - Event reason constants: `ReasonTriggeredScaleUp`, `ReasonNotTriggerScaleUp`, `ReasonFailedScaleUp`
   - `IsScaleUpReason(reason string) bool`
   - `ScaleUpVerdict` (`ScaleUpUnheard`, `ScaleUpProvisioning`, `ScaleUpDeclined`, `(ScaleUpVerdict).String() string`)
   - `AutoscalerEvent` (`Reason string`, `At time.Time`, `PodUID string`)
   - `LatestScaleUp(events []AutoscalerEvent, podUID string, podCreated time.Time) ScaleUpVerdict`

3. **Pure scoring, attribution, finding, remediation, and identity types:**
   - Domain & subject model: `TopologyKey`, `Domain`, `DomainUnknown`, `NormaliseDomain`, `SortDomains`, `SubjectKind` (`SubjectDeployment`, `SubjectStatefulSet`, `SubjectDaemonSet`, `SubjectJob`, `SubjectNodeGroup`, `SubjectCustom`, `SubjectDomain`, `SubjectPreferenceAxis`), `SubjectRef`, `ParseSubjectRef`, `Distribution`, `NewDistribution`
   - Intent & eligibility: `Intent`, `IntentMode` (`ModeSpread`, `ModeColocate`, `ModeIgnore`), `IntentSource` (`Source*` constants, `IntentSources`), `Confidence` (`Confidence*` constants), `Weighting` (`Weight*` constants), `ResolveIntents`, `NodeView`, `EligibilityOptions`, `DefaultEligibilityOptions`, `Eligibility`, `EligibleDomains`
   - Apportionment, scoring & attribution: `Uncapped`, `Apportionment`, `Apportion`, `ApportionAligned`, `Thresholds`, `DefaultThresholds`, `ReportThresholds`, `Scores`, `Score`, `(*Scores).Judge`, `Tier` (`TierNone`, `TierA`, `TierB`, `TierC`), `Verdict`, `SuspectedCause` (`Cause*` constants), `DomainFacts`, `Evidence`, `CauseConfig`, `DefaultCauseConfig`, `Attribution`, `(*Scores).Attribute`
   - Findings, remediation & ledger keys: `Kind*` constants (all 8 finding kinds), `Finding`, `FindingScore`, `FindingIntent`, `FindingDomain`, `FindingInput`, `NewFinding`, `DomainAvailability`, `NewDomainFinding`, `RemediationKind` (`Remediation*` constants), `FindingRemediation`, `SuggestRemediation`, `PlatformManagedRemediation`, `RankFinding`, `RankObservation`, `RankBaseline`, `RankShare`, `RankFindingInput`, `NewRankFinding`, `RankMessage`, `ClusterWideNamespace`, `TopologyFindingKey`, `RankFindingKey`

### 3.3 Deprecation rule for `pkg/leeway` symbols

- Breaking a guaranteed symbol in §3.2 (removing or renaming a type/function/method/constant, changing a function signature, removing or re-typing a struct field, or incompatibly altering documented semantics) requires keeping the existing symbol working via a compatibility wrapper marked `// Deprecated: use <replacement>.` for **both `>= 2` minor releases AND `>= 60` calendar days** pre-1.0 (**`>= 90` calendar days** at 1.0+), measured from the tagged release that ships the `// Deprecated:` comment and records it under `CHANGELOG.md` `### Deprecated`.
- Adding fields to input/output structs (`Finding`, `EligibilityOptions`, `Thresholds`, etc.) is non-breaking; external callers must construct structs with keyed field literals rather than positional literals.
- Other packages under `pkg/` (`pkg/clusters`, `pkg/kube`, `pkg/sources/*`, `pkg/engine`, `pkg/inject`, `pkg/store`) carry standard pre-1.0 Go module semantics unless explicitly promoted into §3.2 (except `report.Report` wire types in §1.2 and `report.CanonicalCluster` / `ClusterReport.*FindingKey` in §2).

---

## 4. `leeway report` CLI Flags and Exit Codes

### 4.1 Stability tiers

Every `leeway report` CLI element sits in one of three tiers:

| Tier | Contents | Rule |
| --- | --- | --- |
| **Frozen** | Exit codes (`0`, `1`, `2`), `stdout`-is-JSON-payload invariant, and the 22 `leeway report` options (23 flag names including `-o`) in §4.4 | Never removed or syntax-broken; guardian-tested in `cmd/leeway/report_flags_stability_test.go` |
| **Stable** | Any future flag shipped in a tagged release without `(experimental)` in its help text | Deprecate-then-remove only (§4.2) |
| **Experimental** | Flags explicitly marked `(experimental)` in `leeway report --help`, plus unreleased flags on `main` | May change without a deprecation window |

### 4.2 Deprecation and default-change rules

1. **Breaking flag changes (rename, removal, bool-to-value syntax change):** Keep the old flag accepted for **both `>= 2` minor releases AND `>= 60` calendar days** pre-1.0 (**`>= 90` calendar days** at 1.0+) from the release that first logs a one-line deprecation warning to **`stderr` (never `stdout` — `stdout` is exclusively the JSON report payload)**; record under `CHANGELOG.md` `### Deprecated` on open and `### Removed` on close; update `deploy/`, `examples/`, and `docs/` in the same PR that opens the window.
2. **Defaults are behavior:** Changing a flag's default value is a minor-version event requiring a `CHANGELOG.md` `### Changed` entry naming the old and new defaults and keeping the previous behavior reachable via explicit flag.
3. **At 1.0:** Frozen and Stable surfaces follow Semantic Versioning; breaking CLI, schema, or `pkg/leeway` changes land only at major versions after satisfying both the `>= 2`-minor and `>= 90`-day deprecation window.

### 4.3 Frozen exit codes

| Exit code | Meaning | `stdout` / `stderr` guarantee |
| --- | --- | --- |
| `0` | Report succeeded (including when findings exist **and** when individual target clusters fail collection with `"status": "error"` inside `clusters[]`), or `-h` / `--help` was requested | Complete `leeway.report/v1alpha1` JSON on `stdout` (unless `--output` or `--help`); skip/warning diagnostics on `stderr` only |
| `1` | Runtime failure outside per-cluster collection: target discovery failed (`clusters.Resolve`), JSON encoding failed, or writing `stdout` / `--output` failed | No partial JSON claimed on `stdout`; error message on `stderr` |
| `2` | CLI usage / flag validation error (unknown flag, unexpected positional argument, `--parallelism <= 0`, `--timeout <= 0`, invalid `--sources`, empty `--topology-keys`, malformed `--topology-cluster-defaults`) | Empty `stdout`; error/usage on `stderr` |

### 4.4 Frozen `leeway report` flag surface

| Flag | Type | Default |
| --- | --- | --- |
| `--clusters` | `string` | `""` |
| `--clusters-from` | `string` | `""` |
| `--profiles-dir` | `string` | `""` |
| `--kubeconfig` | `string` | `""` |
| `--context` | `string` | `""` |
| `--in-cluster` | `bool` | `false` |
| `--cluster-name` | `string` | `""` |
| `--project` | `string` | `""` |
| `--location` | `string` | `""` |
| `--namespace` | `string` | `""` |
| `--sources` | `string` | `"topology-drift,compute-class"` |
| `--topology-keys` | `string` | `"topology.kubernetes.io/zone,topology.kubernetes.io/region"` |
| `--topology-domain-unavailable-keys` | `string` | `"topology.kubernetes.io/zone,topology.kubernetes.io/region"` |
| `--topology-cluster-defaults` | `string` | `""` |
| `--topology-tier-c-signals` | `bool` | `false` |
| `--compute-class-tier-c-signals` | `bool` | `false` |
| `--all` | `bool` | `false` |
| `--min-drift` | `float64` | `0` |
| `--parallelism` | `int` | `8` |
| `--timeout` | `duration` | `60s` (`1m0s`) |
| `--output`, `-o` | `string` | `""` (stdout) |
| `--pretty` | `bool` | `true` |

---

## 5. Enforcement

- **`pkg/report/schema_stability_test.go`**:
  - `TestReportSchemaV1Alpha1_StructJSONFieldsFrozen` reflects over all 15 wire structs (`Report`, `Summary`, `ClusterReport`, `DomainSummary`, `ClassSummary`, `Finding`, `SubjectRef`, `FindingScore`, `FindingIntent`, `FindingDomain`, `FindingRemediation`, `RankFinding`, `RankObservation`, `RankBaseline`, `RankShare`) and pins every JSON key, JSON type class, and `omitempty` tag.
  - `TestReportSchemaV1Alpha1_PopulatedWireSerialization` marshals a fully populated `leeway.report/v1alpha1` document and verifies every JSON key on the wire.
  - `TestReportSchemaV1Alpha1_WireVocabularyFrozen` pins all finding kinds, suspected causes, remediation kinds, intent sources, and severities.
  - `TestClusterReport_FindingIdentityHelpers` and `pkg/leeway/identity_test.go` pin the six-part `topology-drift` and `compute-class` deduplication keys.
  - `TestPkgLeeway_ExternalGoAPISurfaceFrozen` compile-pins every guaranteed `pkg/leeway` symbol and signature in §3.2.
- **`pkg/leeway/purity_test.go`** & **`pkg/sources/layering_test.go`**: enforce zero `client-go` / `internal/...` / `pkg/sources` / `pkg/store` dependencies in `pkg/leeway`.
- **`cmd/leeway/report_flags_stability_test.go`**: pins all 22 `leeway report` flags, bool/value syntax, defaults, and `0` / `1` / `2` exit codes.
