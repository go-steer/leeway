---
title: Using leeway from an AI agent
description: Copy-runnable recipes for AI troubleshooting and fleet-inspection agents invoking leeway report or consuming leeway watch webhook incidents.
---

`leeway` is built to be invoked directly by an AI agent or external orchestrator, or to push structured placement incidents to an agent receiver.

## 1. One-shot multi-cluster inspection (`leeway report`)

When an agent needs the current topology skew, zone availability census, and ComputeClass priority rank status across one or more clusters, invoke `leeway report`:

```sh
# Evaluate explicit cluster endpoints or profile names
leeway report --clusters=prod-us-east1=https://10.0.0.1:6443,prod-us-west1=https://10.0.0.2:6443

# Evaluate clusters from a file (one target per line)
leeway report --clusters-from=/etc/leeway/clusters.txt

# Evaluate all kube-agents profile directories
leeway report --profiles-dir=/var/run/kube-agents/profiles

# Evaluate the current kubeconfig context
leeway report --kubeconfig=~/.kube/config --context=my-cluster
```

### Reading the JSON output

`leeway report` writes a single `leeway.report/v1alpha1` JSON document to `stdout` (or `--output=<path>`):

- `summary`: Fleet-wide counts (`clustersTotal`, `clustersSucceeded`, `clustersFailed`, `topologyFindings`, `domainUnavailable`, `rankFindings`, `byTier`, `bySeverity`).
- `clusters[]`: Per-cluster report containing:
  - `domainAvailability[]`: Ready vs. total node counts for every observed `(topologyKey, domain)` pair. Check this first to see if a zone has zero ready nodes.
  - `topologyFindings[]`: Array of [`leeway.Finding`](/reference/finding-kinds/#topology-findings) objects (`leeway.contract_violated`, `leeway.placement_drift`, `leeway.baseline_breach`, `leeway.domain_unavailable`). Each finding includes:
    - `tier` (`A`, `B`, `C`) and `severity` (`critical`, `warning`, `info`)
    - `score` (`drift`, `observedSkew`, `minAchievableSkew`, `excessSkew`, `relocationDistance`, `maxDomainShare`)
    - `intent` (`source`, `confidence`, `weighting`, `maxSkew`, `evidence[]`)
    - `domains[]` (`domain`, `actual`, `expected`, `delta`, `readyNodes`, `note`)
    - `suspectedCause` (`capacity_shortfall`, `affinity_conflict`, `taint_mismatch`, `volume_topology_lock`, `domain_cordoned`, `rollout_bias`, `descheduler_absent`, `unknown`) and `contributingFactors[]`.
  - `rankFindings[]`: Array of [`leeway.RankFinding`](/reference/finding-kinds/#computeclass-rank-findings) objects (`leeway.rank_wedged`, `leeway.rank_degraded`, `leeway.rank_no_migration`, `leeway.rank_tier_unused`).

## 2. Consuming daemon webhook incidents (`leeway watch`)

When `leeway watch` runs continuously with `--webhook-url=<base-url>`, it posts JSON payloads to two endpoints on your receiver:

1. **`POST <base-url>/incidents`**:
   - Dispatched when a finding clears its dwell window (`--topology-dwell` or `--compute-class-dwell`).
   - Carries standard incident envelope fields (`schema_version`, `kind`, `fingerprint`, `severity`, `cluster`, `namespace`, `object_kind`, `object_name`, `summary`) plus the full structured `leeway_finding` or `leeway_rank_finding`.
   - Return HTTP `200`/`201` with `{"id": "<session-id>"}` so `leeway` binds the incident for deduplication and recovery tracking.
2. **`POST <base-url>/incidents/<id>/events`**:
   - Dispatched when the symptom clears and remains stable for `--recovery-window` (default `5m`).
   - Carries `{"kind": "resolved", "resolution": "recovered" | "object_deleted", ...}`.

See [Webhooks and sinks](/operations/webhooks-and-sinks/) for the complete wire contract.
