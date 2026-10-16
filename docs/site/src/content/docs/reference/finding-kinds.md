---
title: Finding kinds and JSON schemas
description: Inventory of all 8 leeway.* finding kinds and their JSON wire schemas.
sidebar:
  order: 3
---

All `leeway` finding kinds are frozen under the `leeway.` prefix.

## Topology findings (`topology-drift`)

| Kind | Tier | Default Severity | Meaning |
| --- | --- | --- | --- |
| `leeway.contract_violated` | **A** | `critical` | A workload with a hard placement contract (`DoNotSchedule` spread constraint, required pod anti-affinity, or `LeewayPolicy` contract) has `excessSkew > 0` or `relocationDistance > 0`. |
| `leeway.placement_drift` | **B** / **C** | `warning` / `info` | A workload with a soft preference (`ScheduleAnyway`, preferred anti-affinity, or assumed default) has drifted beyond its threshold. |
| `leeway.baseline_breach` | **C** | `info` / `warning` | A workload without declared constraints has deviated outside its learned EWMA baseline band (`--topology-baseline-band`). |
| `leeway.domain_unavailable` | **B** | `warning` | A topology domain (zone/region) that previously held ready nodes now has `0` ready nodes. |

### `leeway.Finding` JSON schema

```json
{
  "kind": "leeway.contract_violated",
  "subject": {
    "kind": "Deployment",
    "namespace": "prod",
    "name": "checkout"
  },
  "topologyKey": "topology.kubernetes.io/zone",
  "tier": "A",
  "severity": "critical",
  "score": {
    "drift": 0.45,
    "observedSkew": 5,
    "minAchievableSkew": 0,
    "excessSkew": 4,
    "relocationDistance": 4,
    "maxDomainShare": 0.67
  },
  "intent": {
    "source": "pod-spec",
    "confidence": "explicit",
    "weighting": "capacity-weighted",
    "maxSkew": 1,
    "evidence": [
      "TopologySpreadConstraint topology.kubernetes.io/zone maxSkew=1 DoNotSchedule"
    ]
  },
  "domains": [
    {
      "domain": "us-east1-b",
      "actual": 6,
      "expected": 2,
      "delta": 4,
      "readyNodes": 12
    },
    {
      "domain": "us-east1-c",
      "actual": 1,
      "expected": 5,
      "delta": -4,
      "readyNodes": 12,
      "note": "0 ready eligible nodes"
    }
  ],
  "suspectedCause": "capacity_shortfall",
  "contributingFactors": [
    "2 pending pods with FailedScheduling (Insufficient cpu)"
  ],
  "firstSeenAt": "2026-10-16T10:20:00Z"
}
```

## ComputeClass rank findings (`compute-class`)

| Kind | Tier | Default Severity | Meaning |
| --- | --- | --- | --- |
| `leeway.rank_wedged` | **A** | `critical` | Pods are running on `unsatisfiable` nodes, or `DoNotScaleUp` is set with zero tier pods and active `FailedScaleUp` events. |
| `leeway.rank_degraded` | **B** | `warning` | Pod-time share on `rank 0` fell below `--compute-class-rank0-floor` or pod-time share on the last rank (`K-1`) exceeded `--compute-class-last-rank-ceiling`. |
| `leeway.rank_no_migration` | **B** | `warning` | `activeMigration.optimizeRulePriority: true` is enabled, a preferred rank has ready idle nodes, and fallback pods did not decrease across `--compute-class-window`. |
| `leeway.rank_tier_unused` | **C** | `info` | An intermediate priority tier (`0 < r < K-1`) accumulated zero pod-seconds across `--compute-class-window`. |

### `leeway.RankFinding` JSON schema

```json
{
  "kind": "leeway.rank_degraded",
  "computeClass": "latency-sensitive",
  "axis": "priority_rule",
  "tier": "B",
  "severity": "warning",
  "rule": "last_rank_ceiling",
  "window": "1h0m0s",
  "meanRank": 1.85,
  "rank0Share": 0.05,
  "lastRankShare": 0.95,
  "threshold": 0.9,
  "ranks": [
    {
      "rank": "0",
      "label": "c3-standard",
      "pods": 1,
      "readyNodes": 0,
      "share": 0.05
    },
    {
      "rank": "1",
      "label": "n2-standard-fallback",
      "pods": 19,
      "readyNodes": 6,
      "share": 0.95
    }
  ],
  "firstSeenAt": "2026-10-16T10:20:00Z"
}
```
