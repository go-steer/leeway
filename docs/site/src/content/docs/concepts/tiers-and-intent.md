---
title: Confidence tiers and intent hierarchy
description: How leeway resolves placement intent from policies, pod specs, and learned baselines and assigns Tier A, B, or C.
sidebar:
  order: 2
---

Every `leeway` finding carries a **Tier** (`A`, `B`, or `C`). The tier is not a synonym for severity — it records **where the placement expectation came from**.

| Tier | Where the expectation came from | Default Severity | Finding Kind |
| --- | --- | --- | --- |
| **A** | An explicit hard contract declared by the workload owner (`TopologySpreadConstraints` with `whenUnsatisfiable: DoNotSchedule`, required pod anti-affinity, or a `LeewayPolicy` contract). | `critical` | `leeway.contract_violated` |
| **B** | A declared soft preference (`whenUnsatisfiable: ScheduleAnyway`, preferred pod anti-affinity, `LeewayPolicy` soft target) or assumed cluster default. | `warning` | `leeway.placement_drift` |
| **C** | No declared constraint. Scored either against the subject's own learned EWMA history or an unanchored apportionment. | `info` (or `warning` on max-domain-share breach) | `leeway.baseline_breach` / `leeway.placement_drift` |

## Why Tier A violations happen at runtime

The Kubernetes scheduler enforces `DoNotSchedule` constraints only **at admission time**. A runtime Tier A violation occurs when pods were placed legally and the cluster changed underneath them:
- Nodes in one zone were drained, cordoned, or terminated and replacement pods could not schedule or were unevenly evicted.
- A node's topology labels were modified after pods landed.
- Pods predate a tightened `TopologySpreadConstraint`.

Because admission controllers never re-evaluate running pods, `leeway` is often the only component in a cluster that notices a broken `DoNotSchedule` contract.

## The 6-level intent precedence hierarchy

For every `(subject, topologyKey)` pair, `leeway` resolves intent using strict precedence (highest wins; lower-precedence observations are retained in `intent.evidence[]` with `(not applied)`):

1. **Namespaced `LeewayPolicy` CRD** (`leeway.lookout.go-steer.io/v1alpha1`)
2. **Cluster-scoped `ClusterLeewayPolicy` CRD**
3. **Workload annotations** (`leeway.lookout.go-steer.io/*`)
4. **Pod spec constraints** (`spec.topologySpreadConstraints` and `spec.affinity.podAntiAffinity`)
5. **Assumed cluster-level scheduler defaults** (`--topology-cluster-defaults`, capped at Tier B)
6. **Learned EWMA baseline** (`--topology-learn-baselines`, always Tier C)
