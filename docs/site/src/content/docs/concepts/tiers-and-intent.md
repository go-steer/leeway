---
title: Confidence tiers and intent hierarchy
description: How leeway resolves placement intent from policies, pod specs, and learned baselines and assigns Tier A, B, or C.
sidebar:
  order: 2
---

Every `leeway` finding carries a **Tier** (`A`, `B`, or `C`). The tier is not a synonym for severity — it records **where the placement expectation came from**.

| Tier | Where the expectation came from | Default Severity | Finding Kind |
| --- | --- | --- | --- |
| **A** | An explicit hard contract declared in the pod spec (`topologySpreadConstraints` with `whenUnsatisfiable: DoNotSchedule`, or a required pod anti-affinity), and the rule that fired was that contract. | `critical` | `leeway.contract_violated` |
| **B** | Any other declared or inferred intent: `whenUnsatisfiable: ScheduleAnyway`, preferred or required pod (anti-)affinity, a `LeewayPolicy` / `ClusterLeewayPolicy`, or a declared or assumed cluster default. | `warning` | `leeway.placement_drift` |
| **C** | No declared or inferred intent. Scored either against the subject's own learned baseline (`leeway watch`) or against an apportionment over its eligible domains. | `info` (`warning` when `maxDomainShare > 0.5`) | `leeway.baseline_breach` / `leeway.placement_drift` |

A `LeewayPolicy` carries no `maxSkew` of its own, so a policy alone never produces Tier A. A `DoNotSchedule` `maxSkew` declared in the pod spec survives a policy outranking it, so a subject with both is still judged at Tier A on that bound. A `LeewayPolicy` can set `thresholds.severity`, which replaces the tier's default severity; such findings carry `severityFromPolicy: true`.

## Why Tier A violations happen at runtime

The Kubernetes scheduler enforces `DoNotSchedule` constraints only **at admission time**. A runtime Tier A violation occurs when pods were placed legally and the cluster changed underneath them:
- Nodes in one zone were drained, cordoned, or terminated and replacement pods could not schedule or were unevenly evicted.
- A node's topology labels were modified after pods landed.
- Pods predate a tightened `TopologySpreadConstraint`.

Because admission controllers never re-evaluate running pods, `leeway` is often the only component in a cluster that notices a broken `DoNotSchedule` contract.

## Intent precedence

For every `(subject, topologyKey)` pair, `leeway` resolves one intent using strict precedence (highest wins; lower-precedence observations are retained in `intent.evidence[]` prefixed `(not applied)`). The value in `intent.source` names the winner:

1. `policy-crd` — a `LeewayPolicy` (namespaced) or `ClusterLeewayPolicy` (cluster-scoped) in `leeway.lookout.go-steer.io/v1alpha1` whose selector matches the pod; a namespaced policy outranks a cluster-scoped one.
2. `workload-annotation` — reserved in the vocabulary; the shipped resolver does not produce it.
3. `topology-spread-constraint` — `spec.topologySpreadConstraints` on the pod.
4. `pod-anti-affinity-required` — `requiredDuringSchedulingIgnoredDuringExecution` pod anti-affinity.
5. `pod-affinity-required` — required pod affinity (a colocation intent).
6. `cluster-default-declared` — kube-scheduler default constraints passed in `--topology-cluster-defaults`.
7. `pod-anti-affinity-preferred` — preferred pod anti-affinity.
8. `pod-affinity-preferred` — preferred pod affinity.
9. `learned-baseline` — the subject's own learned history (`leeway watch` only; always Tier C).
10. `cluster-default-assumed` — kube-scheduler's system default constraints, assumed when `--topology-cluster-defaults` is not set (`confidence: assumed`, capped at Tier B).

`intent.confidence` records how much the winner is trusted: `declared` (written by the workload owner or operator), `inferred` (derived from an affinity rule), `assumed` (a cluster default nobody declared), or `learned` (measured from history).
