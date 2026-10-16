---
title: ComputeClass priority ranks
description: How leeway evaluates GKE ComputeClass node-pool preference ladders and detects rank degradation, wedged fallbacks, and stalled scaleback.
sidebar:
  order: 4
---

GKE `ComputeClass` (`cloud.google.com/v1`) lets operators define an ordered list of `priorities` (machine families, spot vs. on-demand, nodepool labels, or reservations) and a `whenUnsatisfiable` policy (`ScaleUpAnyway` vs. `DoNotScaleUp`).

Admission-time metrics tell you whether pods scheduled, not **which priority rank** in the ladder they actually landed on. `leeway`'s `compute-class` source tracks achieved priority ranks across two axes:
- **`priority_rule`**: Which index (`0..K-1`) of `spec.priorities` a node satisfies (`0` is most preferred).
- **`autopilot`**: Whether the node sits in a custom node pool (`0`) or fell back to GKE Autopilot general-purpose compute (`1`).

## How rank resolution works

For every node claiming a `ComputeClass`:
1. If GKE's node controller has stamped `cloud.google.com/compute-class-priority-rank`, `leeway` reads it directly AND runs its own matcher against `spec.priorities`. If the two disagree, `leeway` increments `lookout_leeway_preference_disagreement`.
2. If the annotation is absent, `leeway` infers the rank by evaluating each `spec.priorities[i]` rule against the node's labels (`cloud.google.com/machine-family`, `cloud.google.com/gke-spot`, `cloud.google.com/gke-nodepool`, `cloud.google.com/reservation-name`, etc.).

## The four rank finding kinds

Over a rolling window (`--compute-class-window`, default `1h`), `leeway` integrates **pod-seconds per rank** and evaluates four rules:

| Finding Kind | Tier | Default Severity | Condition |
| --- | --- | --- | --- |
| `leeway.rank_wedged` | **A** | `critical` | Pods are running on `unsatisfiable` nodes, OR `DoNotScaleUp` is set with zero tier pods and active `FailedScaleUp` events. |
| `leeway.rank_degraded` | **B** | `warning` | Share of pod-seconds on `rank 0` drops below `--compute-class-rank0-floor` (opt-in), OR share on the last fallback rank (`K-1`) exceeds `--compute-class-last-rank-ceiling` (default `0.90`). |
| `leeway.rank_no_migration` | **B** | `warning` | `spec.activeMigration.optimizeRulePriority: true` is declared, a preferred rank has ready idle nodes, and fallback pod count has not decreased across the window. |
| `leeway.rank_tier_unused` | **C** | `info` | An intermediate priority tier (`0 < r < K-1`) has accumulated zero pod-seconds across the window (emitted as a signal when `--compute-class-tier-c-signals` is set). |
