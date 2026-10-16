---
title: Prometheus metrics reference
description: Complete inventory of Prometheus metrics exported by leeway watch on /metrics.
sidebar:
  order: 4
---

All series exported on `/metrics` carry the `cluster` label (`--cluster` in single-cluster mode, or the resolved cluster name in multi-cluster mode).

## Standalone process metrics

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lookout_leeway_standalone_info` | Gauge | `version` | Always `1`; identifies a `leeway` process and its build version. |
| `lookout_leeway_standalone_signals_total` | Counter | `cluster`, `source`, `kind`, `severity` | Total signals emitted by the sources after clearing dwell. |

## Topology placement drift (`topology-drift`)

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lookout_leeway_drift` | Gauge | `cluster`, `subject_kind`, `namespace`, `subject`, `topology_key`, `intent_source`, `confidence`, `weighting` | Normalized L1 drift score (`[0.0, 1.0]`). |
| `lookout_leeway_skew_observed` | Gauge | `cluster`, `subject_kind`, `namespace`, `subject`, `topology_key` | Observed max-minus-min pod count across eligible domains. |
| `lookout_leeway_skew_excess` | Gauge | `cluster`, `subject_kind`, `namespace`, `subject`, `topology_key` | Skew in excess of `max(maxSkew, minAchievableSkew)`. |
| `lookout_leeway_relocation_distance` | Gauge | `cluster`, `subject_kind`, `namespace`, `subject`, `topology_key` | Minimum pod moves required to satisfy the contract. |
| `lookout_leeway_max_domain_share` | Gauge | `cluster`, `subject_kind`, `namespace`, `subject`, `topology_key` | Largest fraction of replicas in a single domain (`[0.0, 1.0]`). |
| `lookout_leeway_alert_state` | Gauge | `cluster`, `subject_kind`, `namespace`, `subject`, `topology_key`, `tier`, `suspected_cause` | Open episode state (`1` = pending dwell, `2` = firing). Absent when healthy. |
| `lookout_leeway_domain_objects` | Gauge | `cluster`, `subject_kind`, `namespace`, `subject`, `topology_key`, `domain`, `state` | Observed object count in a domain (gated by cardinality controls). |
| `lookout_leeway_domain_expected` | Gauge | `cluster`, `subject_kind`, `namespace`, `subject`, `topology_key`, `domain` | Apportioned expected object count in a domain. |
| `lookout_leeway_domain_ready_nodes` | Gauge | `cluster`, `topology_key`, `domain` | Ready nodes in each topology domain. |
| `lookout_leeway_domains_unavailable` | Gauge | `cluster`, `topology_key` | Count of domains with zero ready nodes. |
| `lookout_leeway_subjects_tracked` | Gauge | `cluster`, `subject_kind`, `tier` | Number of subjects currently tracked. |
| `lookout_leeway_transient_subjects` | Gauge | `cluster`, `state` | Subject-axes currently suppressed or relaxed by transient state. |
| `lookout_leeway_domain_series_withheld` | Gauge | `cluster`, `reason` | Subject-axes whose per-domain series were withheld by cardinality controls. |
| `lookout_leeway_counter_mismatch_total` | Counter | `cluster` | Disagreements repaired between incremental counters and full cache rebuilds. |

## ComputeClass priority ranks (`compute-class`)

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lookout_leeway_preference_pods` | Gauge | `cluster`, `compute_class`, `axis`, `rank`, `rank_label` | Running pods per ComputeClass and priority rank. |
| `lookout_leeway_preference_nodes` | Gauge | `cluster`, `compute_class`, `axis`, `rank`, `rank_label`, `ready` | Nodes per ComputeClass, priority rank, and readiness. |
| `lookout_leeway_preference_pod_seconds_total` | Counter | `cluster`, `compute_class`, `axis`, `rank`, `rank_label` | Cumulative pod-seconds spent at each priority rank. |
| `lookout_leeway_preference_rank_weighted_pod_seconds_total` | Counter | `cluster`, `compute_class`, `axis` | Cumulative `rank * pod_seconds` over numbered tier ranks (`0..K-1`). |
| `lookout_leeway_preference_alert_state` | Gauge | `cluster`, `compute_class`, `axis`, `rule`, `tier` | Open rank episode state (`1` = pending, `2` = firing). |
| `lookout_leeway_preference_disagreement` | Gauge | `cluster`, `compute_class` | Nodes where GKE's rank annotation and `leeway`'s rule inference disagree. |
| `lookout_leeway_preference_rank_pending` | Gauge | `cluster`, `compute_class` | Nodes awaiting GKE's rank annotation where rule inference cannot decide. |
