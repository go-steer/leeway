---
title: Using leeway from an AI agent
description: Copy-runnable recipes for AI troubleshooting and fleet-inspection agents invoking leeway report or consuming leeway watch webhook incidents.
---

`leeway` is built to be invoked directly by an AI agent or external orchestrator, or to push structured placement incidents to an agent receiver.

## 1. One-shot multi-cluster inspection (`leeway report`)

When an agent needs the current topology skew, zone availability census, and ComputeClass priority rank status across one or more clusters, invoke `leeway report`:

```sh
# Evaluate explicit targets: GKE triples, name=endpoint pairs, or kubeconfig contexts
leeway report --clusters=my-project/us-east1/prod-us-east1,prod-us-west1=https://10.0.0.2:6443

# Evaluate every context in a kubeconfig, or every GKE cluster in a project
leeway report --clusters-from=kubeconfig:/etc/leeway/kubeconfig
leeway report --clusters-from=my-project

# Evaluate every Cluster Agent profile in a Hermes profiles directory
leeway report --profiles-dir=/opt/data/profiles

# Evaluate one kubeconfig context
leeway report --kubeconfig=~/.kube/config --context=my-cluster

# Include Tier C findings and every evaluated subject, as single-line JSON
leeway report --context=my-cluster --topology-tier-c-signals --all --pretty=false
```

`--clusters-from` names a discovery source (`kubeconfig[:/path]`, `profiles:/path`, or `<project>[/<location>]`), never a file of targets. See [One-shot multi-cluster reports](/getting-started/report/) for every flag.

### Reading the JSON output

`leeway report` writes a single `leeway.report/v1alpha1` JSON document to `stdout` (or `--output=<path>`). The envelope is snake_case; the finding bodies inside it are camelCase.

```json
{
  "schema_version": "leeway.report/v1alpha1",
  "generated_at": "2026-10-19T12:00:00Z",
  "clusters": [
    {
      "cluster": "gke-cordoned",
      "source": "kubeconfig:gke-cordoned",
      "status": "ok",
      "domains": {
        "topology.kubernetes.io/zone": {
          "us-east1-a": {
            "nodes": 1,
            "ready": 1,
            "usable": 1,
            "allocatable_cpu_milli": 4000,
            "allocatable_mem_bytes": 17179869184
          },
          "us-east1-c": {
            "nodes": 1,
            "ready": 1,
            "usable": 0
          }
        }
      },
      "topology_findings": [
        {
          "kind": "leeway.domain_unavailable",
          "subject": {
            "kind": "Domain",
            "name": "us-east1-c"
          },
          "topologyKey": "topology.kubernetes.io/zone",
          "tier": "B",
          "severity": "warning",
          "suspectedCause": "taint_exclusion",
          "remediation": {
            "kind": "cluster_nodes",
            "summary": "All Ready nodes in us-east1-c on topology.kubernetes.io/zone are cordoned or tainted; uncordon or remove blocking taints in us-east1-c."
          }
        }
      ]
    }
  ],
  "summary": {
    "clusters_total": 1,
    "clusters_succeeded": 1,
    "clusters_failed": 0,
    "topology_findings": 1,
    "rank_findings": 0,
    "by_severity": {
      "warning": 1
    },
    "by_tier": {
      "B": 1
    }
  }
}
```

(The finding above is shortened; `score`, `domains` and `firstSeenAt` are always present.)

- `summary`: Fleet-wide counts: `clusters_total`, `clusters_succeeded`, `clusters_failed`, `topology_findings`, `rank_findings`, and `by_severity` / `by_tier` maps over both finding arrays (omitted when there are no findings).
- `clusters[]`: One entry per target, in the order resolved:
  - `cluster`, `source` (`direct`, `kubeconfig:<context>`, `gke:<project>/<location>/<cluster>`, `endpoint:<name>`, or the profile directory name), `status` (`ok` or `error`), and `error` when the cluster could not be evaluated.
  - `domains`: Map of topology key → domain → `{nodes, ready, usable, allocatable_cpu_milli, allocatable_mem_bytes}`. Check this first: `usable: 0` means nothing can be scheduled into that domain.
  - `topology_findings[]`: [`leeway.Finding`](/reference/finding-kinds/#topology-findings) objects (`leeway.contract_violated`, `leeway.placement_drift`, `leeway.baseline_breach`, `leeway.domain_unavailable`). Each carries:
    - `tier` (`A`, `B`, `C`) and `severity` (`critical`, `warning`, `info`)
    - `score` (`drift`, `observedSkew`, `minAchievableSkew`, `excessSkew`, `relocationDistance`, `maxDomainShare`)
    - `intent` (`source`, `confidence`, `weighting`, `maxSkew`, `evidence[]`), omitted when the subject declared nothing
    - `domains[]` (`domain`, `actual`, `expected`, `delta`, `readyNodes`, `note`)
    - `suspectedCause` (`unknown`, `domain_outage`, `domain_capacity_shortfall`, `taint_exclusion`, `consolidation`, `volume_pinning`, `rollout_bias`, `constraint_ignored`), `contributingFactors[]`, and `untestedCauses[]` for causes `leeway report` had no evidence source for
    - `remediation` (`kind` of `manifest_patch`, `rollout_restart`, `cluster_capacity`, `cluster_nodes`, `storage_topology`; `summary`; optional `command` and `yaml`). Read `summary` before applying `yaml`: a `manifest_patch` can be the second half of a two-step fix.
  - `compute_classes[]`: One row per ComputeClass (`name`, `scorable`, `ordering`, `podsByRank`, `pendingPods`, `pendingProvisioning`, `pendingDeclined`).
  - `rank_findings[]`: [`leeway.RankFinding`](/reference/finding-kinds/#computeclass-rank-findings) objects (`leeway.rank_wedged`, `leeway.rank_degraded`, `leeway.rank_no_migration`, `leeway.rank_tier_unused`).
  - `subjects[]`: Only with `--all` or `--min-drift`. Every evaluated `(subject, topologyKey)` as a `leeway.Finding`, including non-breaching ones, which have empty `tier` and `severity` and no `remediation`.

## 2. Consuming daemon webhook incidents (`leeway watch`)

When `leeway watch` runs continuously with `--webhook-url=<base-url>`, it posts JSON payloads to two endpoints on your receiver:

1. **`POST <base-url>/incidents`**:
   - Dispatched when a finding clears its dwell window (`--topology-dwell` or `--compute-class-dwell`, default `10m`).
   - The body is the signal-schema v1 payload: `kind`, `reason`, `namespace`, `kind_of_object`, `name`, `uid`, `message`, `count`, `first_seen`, `last_seen`, `cluster`, `severity`, `fingerprint`, `context`, `type`, plus the full structured `leeway_finding` or `leeway_rank_finding` object described above.
   - Return HTTP `2xx` with `{"id": "<incident-id>"}` so `leeway` binds the incident for deduplication and recovery tracking.
2. **`POST <base-url>/incidents/<id>/events`**:
   - Dispatched when the symptom clears and remains clear for `--recovery-stable-for` (default `2m`).
   - Carries `"kind": "resolved"` and a `resolution` of `recovered` or `object_deleted`.

See [Webhooks and sinks](/operations/webhooks-and-sinks/) for the complete wire contract.
