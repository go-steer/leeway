---
title: Scoring and root-cause attribution
description: How leeway computes expected domain counts, excess skew, relocation distance, and attributes the most specific root cause.
sidebar:
  order: 3
---

## Capacity-aware expectation (Hamilton apportionment)

Assuming every zone should receive an equal `1/D` share of pods produces false positives whenever zones have unequal node capacity.

`leeway` sums the allocatable CPU of the ready, eligible nodes in each domain for the subject. When the largest-to-smallest domain capacity ratio reaches `1.25` (`leeway.CapacityWeightingRatioTrigger`, not a flag), a subject whose intent declared no weighting switches from `Equal` to `AllocatableCPU` weighting, and the finding's `intent.weighting` reports which one was used. A subject with a declared `maxSkew` or a required anti-affinity ceiling stays on `Equal`: kube-scheduler bounds pod counts, not millicores, so a capacity-weighted expectation would make the declared bound unviolable. Expected counts are apportioned with **Hamilton's largest-remainder method**, so integer `expected` values always sum to the replica count `N`.

## Core score metrics

Every `leeway.Finding` reports six numbers in its `score` block:

- **`drift`** (`[0.0, 1.0]`): `relocationDistance / N`, which is the normalized L1 distance $\frac{1}{2N}\sum_d |O_d - E_d|$ — the fraction of the workload that is in the wrong domain.
- **`observedSkew`**: $\max_d(O_d) - \min_d(O_d)$ across eligible domains, counting Running pods only.
- **`minAchievableSkew`**: The skew of the expectation itself — the best any placement can do given integer division, unequal weights, and per-domain caps.
- **`excessSkew`**: $\max(0, \text{observedSkew} - \max(\text{maxSkew}, \text{minAchievableSkew}))$ — the skew *beyond* what the contract permits.
- **`relocationDistance`**: $\sum_d \max(0, O_d - E_d)$ — how many pods sit above their domain's expectation, which is the number that would have to move.
- **`maxDomainShare`**: $\max_d(O_d) / N$, catching single-domain blast-radius concentration.

## The root-cause attribution ladder

Once a subject breaches, `leeway` evaluates an ordered attribution ladder (`pkg/leeway/cause.go`) against per-domain evidence to name `suspectedCause`. The first rung with positive evidence wins; every rung's evidence, including ruled-out ones, is listed in `contributingFactors[]`:

1. **`domain_outage`**: A deficit domain has NotReady nodes, and at least as many of them as Ready nodes.
2. **`consolidation`**: A deficit domain lost a node to autoscaler consolidation inside the attribution window (2h).
3. **`taint_exclusion`**: A deficit domain became ineligible through a taint or cordon inside the window.
4. **`domain_capacity_shortfall`**: Pods are Pending with `FailedScheduling` citing insufficient resources, or a deficit domain's Ready node count fell inside the window.
5. **`volume_pinning`**: At least half of the pods that would have to move are pinned to a surplus domain by a zonal PersistentVolume.
6. **`rollout_bias`**: A rollout completed inside the window, more than 10 minutes ago, and the drift did not recover.
7. **`constraint_ignored`**: The subject declared `maxSkew` with `whenUnsatisfiable: ScheduleAnyway` and the observed skew exceeds it — nothing malfunctioned, the scheduler was told to place the pods anyway.
8. **`unknown`**: Fallback when no rung has positive evidence.

Rungs whose evidence comes from a source that is not running — `rollout_bias` in `leeway report`, which has no rollout source — are listed in `untestedCauses[]` when they sit above the winning rung, so a reader can tell "ruled out" from "never asked".
