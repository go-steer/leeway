---
title: Concepts
description: How leeway infers placement intent, computes capacity-weighted expectations, assigns confidence tiers, attributes root causes, and evaluates ComputeClass priority ranks.
sidebar:
  order: 1
---

`leeway` separates **what is measured** (the pure mathematical engine in `pkg/leeway`) from **how Kubernetes state is collected** (`pkg/sources/topologydrift`, `pkg/sources/computeclass`, and `pkg/report`).

1. **[Confidence Tiers & Intent Hierarchy](/concepts/tiers-and-intent/)** — Why every finding carries a Tier (`A`, `B`, `C`) based on *who declared the expectation*, and the 6-level precedence order for placement intent.
2. **[Scoring & Root-Cause Attribution](/concepts/scoring-and-attribution/)** — How capacity-weighted Hamilton apportionment, normalized L1 drift, excess skew, relocation distance, and the 8-rung attribution ladder work.
3. **[ComputeClass Priority Ranks](/concepts/compute-class-ranks/)** — How `leeway` tracks GKE `ComputeClass` preference ladders (`priority_rule` and `autopilot` axes) over rolling pod-time windows.
