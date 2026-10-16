---
title: Scoring and root-cause attribution
description: How leeway computes expected domain counts, excess skew, relocation distance, and attributes the most specific root cause.
sidebar:
  order: 3
---

## Capacity-aware expectation (Hamilton apportionment)

Assuming every zone should receive an equal `1/D` share of pods produces false positives whenever zones have unequal node capacity.

`leeway` queries the **Capacity Oracle** for the allocatable CPU of ready, eligible nodes in each domain for the subject. When the largest-to-smallest domain capacity ratio reaches `--topology-capacity-ratio` (default `1.25`), `leeway` switches from uniform weighting to **capacity-proportional weighting** and uses **Hamilton's largest-remainder method** so integer `expected` pod counts always sum to the exact replica count `N`.

## Core score metrics

Every `leeway.Finding` reports six numbers in its `score` block:

- **`drift`** (`[0.0, 1.0]`): Normalized L1 distance $\frac{1}{2N}\sum_d |O_d - E_d|$, representing the fraction of the workload displaced from expectation.
- **`observedSkew`**: $\max_d(O_d) - \min_d(O_d)$ across eligible domains.
- **`minAchievableSkew`**: The theoretical best-case skew given integer division and fixed/pinned pods.
- **`excessSkew`**: $\max(0, \text{observedSkew} - \max(\text{maxSkew}, \text{minAchievableSkew}))$ — the skew *beyond* what the contract permits.
- **`relocationDistance`**: Minimum number of pod moves required to bring every domain within the contract's `maxSkew`.
- **`maxDomainShare`**: $\max_d(O_d) / N$, catching single-domain blast-radius concentration.

## The 8-rung root-cause attribution ladder

Once a subject breaches, `leeway` evaluates an ordered attribution ladder (`pkg/leeway/attribute.go`) against per-domain evidence to name `suspectedCause`:

1. **`domain_cordoned`**: Deficit domains have nodes, but all or most are cordoned (`spec.unschedulable=true`).
2. **`volume_topology_lock`**: Pods mount PersistentVolumes with `nodeAffinity` pinning them to surplus zones.
3. **`taint_mismatch`**: Ready nodes in deficit domains carry taints that the workload's pod spec does not tolerate.
4. **`affinity_conflict`**: Node selectors or required `nodeAffinity` rules exclude nodes in deficit domains.
5. **`capacity_shortfall`**: Deficit domains have zero ready/allocatable nodes or pending pods failing scheduling due to `Insufficient cpu/memory`.
6. **`rollout_bias`**: A recent controller rollout completed shortly before the skew appeared, leaving replicas concentrated where capacity was momentarily free.
7. **`descheduler_absent`**: All domains are healthy, eligible, and have capacity, meaning the skew is a historical artifact that persists because nothing is evicting excess pods.
8. **`unknown`**: Fallback when no specific mechanical cause dominates.
