---
title: Finding kinds and JSON schemas
description: Inventory of all 8 leeway.* finding kinds and their JSON wire schemas.
sidebar:
  order: 3
---

All `leeway` finding kinds are frozen under the `leeway.` prefix. The JSON examples on this page are taken from `leeway report` output against fake clusters; only arrays have been shortened.

## Topology findings (`topology-drift`)

| Kind | Tier | Default Severity | Meaning |
| --- | --- | --- | --- |
| `leeway.contract_violated` | **A** | `critical` | A workload with a hard placement contract declared in its pod spec (a `DoNotSchedule` spread constraint, or a required pod anti-affinity) has `excessSkew > 0`, or a domain holding more objects than the anti-affinity's per-domain ceiling. |
| `leeway.placement_drift` | **B** / **C** | `warning` / `info` | A workload has `drift > 0.2` against its expectation. Tier B when the expectation came from a declared or inferred intent that is not a hard contract (`ScheduleAnyway`, pod (anti-)affinity, a `LeewayPolicy`, or a cluster default); Tier C when no intent exists and the expectation is an apportionment over the eligible domains. Tier C escalates to `warning` when `maxDomainShare > 0.5`. |
| `leeway.baseline_breach` | **C** | `info` / `warning` | A workload whose only expectation is its own learned baseline (`leeway watch`) has a domain outside the learned band. |
| `leeway.domain_unavailable` | **B** | `warning` | A topology domain the cluster has, or recently had, now has no usable (Ready and schedulable) node. Raised per domain, not per workload. |

### `leeway.Finding` JSON schema

A Tier B `placement_drift` finding whose spread constraint is contradicted by a required `nodeAffinity` on the same key:

```json
{
  "kind": "leeway.placement_drift",
  "subject": {
    "kind": "Deployment",
    "namespace": "topology",
    "name": "affinity-pinned-api"
  },
  "topologyKey": "topology.kubernetes.io/zone",
  "tier": "B",
  "severity": "warning",
  "score": {
    "drift": 0.5,
    "observedSkew": 2,
    "minAchievableSkew": 0,
    "excessSkew": 1,
    "relocationDistance": 1,
    "maxDomainShare": 1
  },
  "intent": {
    "source": "topology-spread-constraint",
    "confidence": "declared",
    "weighting": "Equal",
    "maxSkew": 1,
    "evidence": [
      "topologySpreadConstraints[0]: topologyKey=topology.kubernetes.io/zone maxSkew=1 whenUnsatisfiable=ScheduleAnyway labelSelector=app=affinity-pinned-api",
      "conflicting nodeAffinity/nodeSelector on topology.kubernetes.io/zone restricts placement to 1 domain(s), contradicting the spread constraint on topology.kubernetes.io/zone"
    ]
  },
  "domains": [
    {
      "domain": "us-central1-a",
      "actual": 2,
      "expected": 1,
      "delta": 1,
      "readyNodes": 1
    },
    {
      "domain": "us-central1-b",
      "actual": 0,
      "expected": 1,
      "delta": -1,
      "readyNodes": 1
    }
  ],
  "suspectedCause": "constraint_ignored",
  "contributingFactors": [
    "0 pods pinned by zonal volumes",
    "whenUnsatisfiable: ScheduleAnyway with maxSkew 1 and observed skew 2"
  ],
  "untestedCauses": [
    "rollout_bias"
  ],
  "firstSeenAt": "2026-10-19T12:00:00Z",
  "remediation": {
    "kind": "manifest_patch",
    "summary": "Remove, or widen to more than one domain, the conflicting nodeAffinity/nodeSelector on topology.kubernetes.io/zone from Deployment/topology/affinity-pinned-api so its topologySpreadConstraint can spread replicas across eligible domains; the yaml only restates the spread constraint and does not remove the affinity, so applying it alone changes nothing.",
    "command": "kubectl -n topology rollout restart deployment/affinity-pinned-api",
    "yaml": "# Step 2 of 2. This patch only restates the topologySpreadConstraint on topology.kubernetes.io/zone.\n# Step 1, which it cannot express, is to remove (or widen to more than one\n# domain) the nodeAffinity/nodeSelector on topology.kubernetes.io/zone in this pod template;\n# applying this patch on its own changes nothing.\nspec:\n  template:\n    spec:\n      topologySpreadConstraints:\n      - maxSkew: 1\n        topologyKey: topology.kubernetes.io/zone\n        whenUnsatisfiable: ScheduleAnyway\n        labelSelector:\n          matchLabels:\n            app: affinity-pinned-api\n        matchLabelKeys:\n        - pod-template-hash"
  }
}
```

Field vocabularies and presence rules:

- `subject.kind`: `Deployment`, `StatefulSet`, `DaemonSet`, `Job`, `Custom`, `NodeGroup`, or `Domain`. `subject.namespace` is omitted for cluster-scoped subjects.
- `tier`: `A`, `B`, `C`. `severity`: `critical`, `warning`, `info`. In `subjects[]` (`--all` / `--min-drift`) both are empty strings on a subject that did not breach.
- `score`: `drift`, `observedSkew`, `minAchievableSkew`, `excessSkew`, `relocationDistance`, `maxDomainShare`, always present. See [Scoring and attribution](/concepts/scoring-and-attribution/).
- `intent`: omitted when the subject expressed no intent on this key (the Tier C apportioned case). `source` is one of `policy-crd`, `workload-annotation`, `topology-spread-constraint`, `pod-anti-affinity-required`, `pod-affinity-required`, `cluster-default-declared`, `pod-anti-affinity-preferred`, `pod-affinity-preferred`, `learned-baseline`, `cluster-default-assumed`. `confidence` is one of `declared`, `inferred`, `assumed`, `learned`. `weighting` is one of `Equal`, `NodeCount`, `AllocatableCPU`, `AllocatableMemory`. `maxSkew` is omitted when the source declared none. `evidence[]` lines from a lower-precedence source that lost resolution are prefixed `(not applied) `.
- `domains[]`: one row per eligible domain (`domain`, `actual`, `expected`, `delta`, `readyNodes`) plus an optional `note`.
- `suspectedCause`: `unknown`, `domain_outage`, `domain_capacity_shortfall`, `taint_exclusion`, `consolidation`, `volume_pinning`, `rollout_bias`, or `constraint_ignored`. `contributingFactors[]` carries the evidence lines, including ruled-out ones.
- `untestedCauses[]`: causes more specific than `suspectedCause` whose evidence this deployment could not consult. `leeway report` has no rollout source, so it reports `rollout_bias` here whenever a less specific cause won. Omitted when empty.
- `transient` (string), `relaxed` (bool) and `severityFromPolicy` (bool): omitted when empty or false. `transient` is one of `cluster-warmup`, `domain-outage`, `rollout`, `node-drain`, `recent-scale`, the `leeway watch` state the subject breached through; `relaxed` says the thresholds were widened for it; `severityFromPolicy` says a `LeewayPolicy` set `severity`.
- `remediation`: present on breached findings (every entry in `topology_findings`; in `subjects[]` only those that breached). `kind` is one of `manifest_patch`, `rollout_restart`, `cluster_capacity`, `cluster_nodes`, `storage_topology`. `summary` is always set; `command` and `yaml` are omitted when there is nothing to run or apply. Only `manifest_patch` carries `yaml`.

### `leeway.domain_unavailable` example

The subject is the domain itself, `score` is all zeros because nothing was scored, and `domains[]` has one row carrying the node census in `note`:

```json
{
  "kind": "leeway.domain_unavailable",
  "subject": {
    "kind": "Domain",
    "name": "us-east1-c"
  },
  "topologyKey": "topology.kubernetes.io/zone",
  "tier": "B",
  "severity": "warning",
  "score": {
    "drift": 0,
    "observedSkew": 0,
    "minAchievableSkew": 0,
    "excessSkew": 0,
    "relocationDistance": 0,
    "maxDomainShare": 0
  },
  "domains": [
    {
      "domain": "us-east1-c",
      "actual": 0,
      "expected": 0,
      "delta": 0,
      "readyNodes": 1,
      "note": "1 node(s) Ready, none schedulable"
    }
  ],
  "suspectedCause": "taint_exclusion",
  "firstSeenAt": "2026-10-19T12:00:00Z",
  "remediation": {
    "kind": "cluster_nodes",
    "summary": "All Ready nodes in us-east1-c on topology.kubernetes.io/zone are cordoned or tainted; uncordon or remove blocking taints in us-east1-c."
  }
}
```

`suspectedCause` is `consolidation` when the domain has no nodes left, `domain_outage` when nodes are present but none is Ready, and `taint_exclusion` when nodes are Ready but none is schedulable. The `remediation.kind` is `cluster_capacity` for the first and `cluster_nodes` for the other two.

## ComputeClass rank findings (`compute-class`)

| Kind | Tier | Default Severity | Meaning |
| --- | --- | --- | --- |
| `leeway.rank_wedged` | **A** | `critical` | Pods are Pending against a class that declares `whenUnsatisfiable: DoNotScaleUp`, and the autoscaler either declined to scale up for them or has given no verdict (`rule: wedged`). |
| `leeway.rank_degraded` | **B** | `warning` | Pod-time share at rank 0 fell below `--compute-class-rank0-floor` (`rule: rank0-share`), or pod-time share at the last rank rose above `--compute-class-last-rank-ceiling` (`rule: last-rank`). In `leeway watch`, mean achieved rank rising above the axis's learned normal is reported under this kind too (`rule: rank-baseline`, Tier C). |
| `leeway.rank_no_migration` | **B** | `warning` | The class declares `activeMigration.optimizeRulePriority: true`, rank 0 has come back into use, and pods are still at a worse tier after the migration grace period with no migration observed (`rule: no-migration`). |
| `leeway.rank_tier_unused` | **C** | `info` | A preference tier accrued zero pod-seconds for the whole time the axis has been observed (`rule: tier-unused`, one finding per tier, `focus` names it). |

### `leeway.RankFinding` JSON schema

```json
{
  "kind": "leeway.rank_degraded",
  "subject": {
    "kind": "PreferenceAxis",
    "name": "gold"
  },
  "provider": "gke-computeclass",
  "objectKind": "ComputeClass",
  "specHash": "842f2f5b7a0deb21",
  "tier": "B",
  "severity": "warning",
  "rule": "last-rank",
  "observed": {
    "windowSeconds": 3600,
    "tierSeconds": 21600,
    "rank0Share": 0,
    "lastRank": 1,
    "lastRankShare": 1,
    "meanRank": 1,
    "pendingPods": 1,
    "pendingProvisioning": 0,
    "pendingDeclined": 0,
    "degradedPods": 6,
    "improving": 0,
    "worsening": 0,
    "lateral": 0
  },
  "ranks": [
    {
      "rank": 0,
      "rules": "machineFamily=n2",
      "podSeconds": 0,
      "share": 0,
      "pods": 0
    },
    {
      "rank": 1,
      "rules": "machineFamily=e2",
      "podSeconds": 21600,
      "share": 1,
      "pods": 6
    }
  ],
  "ordering": "list-position",
  "tiers": 2,
  "scaleUp": "unset",
  "optimizeRulePriority": false,
  "reason": "rank 1 share 1.00 is above the ceiling 0.90: running on last-resort capacity, which on most classes is the spot or the cheapest rule",
  "firstSeenAt": "2026-10-19T12:00:00Z"
}
```

- `rank` values are integers; negative values are sentinels (`-1` unknown, `-2` unsatisfiable: the node matches no rule, `-3` off-axis: provisioned outside the priority list).
- `focus` (an integer rank) is present only on per-tier findings (`leeway.rank_tier_unused`).
- `ordering` is `list-position` or `priority-score`. `scaleUp` is `unset`, `scale-up-anyway`, `do-not-scale-up`, or `unrecognised`.
