---
title: Prometheus metrics reference
description: Complete inventory of Prometheus metrics exported by leeway watch on /metrics.
sidebar:
  order: 4
---

`leeway watch` serves `/metrics` on `--metrics-addr` (default `:9090`). The inventory below is `topologydrift.MetricDocs()`, `computeclass.MetricDocs()`, the two collectors in `cmd/leeway/emit.go`, and the OTLP export SLIs in `internal/telemetry`; the first two are held to the live exporter by `TestMetricDocs_MatchTheExporter`, and the label sets were confirmed against a scrape of the `test/e2e` daemon.

Two things apply to every `lookout_leeway_*` series and are not repeated in the tables:

- **`cluster` label.** In multi-cluster mode (`--clusters`, `--clusters-from`, `--profiles-dir`) every series carries `cluster=<resolved name>`. In single-cluster mode it carries `cluster=<--cluster-name>` only when `--cluster-name` is set; without it there is no `cluster` label. The Go runtime and process collectors (`go_*`, `process_*`) never carry it.
- **Presence.** The `lookout_leeway_*` sources are OpenTelemetry instruments bridged into the registry (no `target_info` or `otel_scope_*` series are added). Observable gauges that report "open episodes" or "subjects under a condition" have no rows when there are none, and a counter is absent until its first increment — `lookout_leeway_counter_mismatch_total` on a healthy cluster is the usual example.

## Standalone process metrics

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lookout_leeway_standalone_info` | Gauge | `version` | Always `1`, labelled with the version of the `leeway` binary. |
| `lookout_leeway_standalone_signals_total` | Counter | `source`, `kind`, `severity` | Findings the sources raised in this process after clearing dwell. `source` is `sentinel`; `kind` is the `leeway.*` finding kind. |

## Topology placement drift (`topology-drift`)

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lookout_leeway_subjects_tracked` | Gauge | `subject_kind` | Subjects with a tracked distribution, by kind. |
| `lookout_leeway_node_groups_discovered` | Gauge | — | Distinct node groups resolved from the node-group label precedence list, before the 200-group tracking bound is applied. |
| `lookout_leeway_domains_unavailable` | Gauge | `topology_key` | Domains with no usable node, per configured axis; zero when nothing is out. The live reading behind `leeway.domain_unavailable`. |
| `lookout_leeway_domain_ready_nodes` | Gauge | `topology_key`, `domain` | Usable nodes per topology domain. |
| `lookout_leeway_domain_objects` | Gauge | `namespace`, `subject`, `subject_kind`, `topology_key`, `domain`, `state` | Objects counted per subject, domain and scheduling state. `state` is `active` or `waiting` (the four raw states are collapsed). Subject to the per-domain cardinality gate, so absent for most subjects. |
| `lookout_leeway_domain_expected` | Gauge | `namespace`, `subject`, `subject_kind`, `topology_key`, `domain` | Objects apportioned to each domain — the expectation `domain_objects` is scored against. Gated like `domain_objects`. |
| `lookout_leeway_domain_series_withheld` | Gauge | `reason` | Subjects whose per-domain breakdown was withheld, by control: `gate` (below the drift floor and not alerting), `namespace`, `key_cap`. |
| `lookout_leeway_observed_skew` | Gauge | `namespace`, `subject`, `subject_kind`, `topology_key` | `S`, fullest minus emptiest eligible domain. |
| `lookout_leeway_excess_skew` | Gauge | `namespace`, `subject`, `subject_kind`, `topology_key` | `E`, observed skew beyond what the arithmetic and the declared bound allow. |
| `lookout_leeway_drift` | Gauge | `namespace`, `subject`, `subject_kind`, `topology_key` | `ρ`, the fraction of a subject's objects that would have to move (`[0.0, 1.0]`). |
| `lookout_leeway_max_domain_share` | Gauge | `namespace`, `subject`, `subject_kind`, `topology_key` | Share of a subject's objects held by its fullest domain. |
| `lookout_leeway_relocation_distance` | Gauge | `namespace`, `subject`, `subject_kind`, `topology_key` | `R`, the number of objects that would have to move. |
| `lookout_leeway_intent_info` | Gauge | `namespace`, `subject`, `subject_kind`, `topology_key`, `mode`, `source`, `confidence`, `weighting`, `max_skew` | Constant `1` describing the resolved intent on one axis. Only subjects that expressed an intent have a row; learned baselines are deliberately absent. |
| `lookout_leeway_alert_state` | Gauge | `namespace`, `subject`, `subject_kind`, `topology_key`, `tier`, `phase` | Dwell-machine position of one subject-axis: `1` pending, `2` firing (a `resolving` episode still reads `2`). Only open episodes have a row. |
| `lookout_leeway_transient_subjects` | Gauge | `topology_key`, `transient` | Subject-axes whose judgement was suppressed or relaxed, by transient (`cluster-warmup`, `domain-outage`, `rollout`, `node-drain`, `recent-scale`). Zero rows is the healthy reading. |
| `lookout_leeway_last_event_timestamp_seconds` | Gauge | `resource` | Unix time of the last informer event processed, per resource. |
| `lookout_leeway_evaluation_duration_seconds` | Histogram | `subject_kind` | Time spent evaluating one coalesced subject (buckets 100µs–5s). |
| `lookout_leeway_counter_mismatch_total` | Counter | `subject_kind` | Subjects whose incremental distribution disagreed with a rebuild from the pod cache and were repaired. Any non-zero rate is a bug in `leeway`, not a cluster condition. |
| `lookout_leeway_baselines` | Gauge | `state` | Subject-axes with a learned baseline, by `learning`, `mature`, `frozen` (overlapping; they do not sum). |
| `lookout_leeway_baseline_samples_total` | Counter | `outcome` | Baseline samples by what the estimator did: `applied`, `seeded`, `reset`, `held`, `empty`. A sustained `reset` rate is the failure mode worth alerting on. |
| `lookout_leeway_watch_events_total` | Counter | `resource`, `outcome` | Informer events delivered, by resource and `applied` / `inert`. |

## ComputeClass priority ranks (`compute-class`)

`rank` values are the derived preference tier (`0`, `1`, …) or one of the sentinels `unknown`, `unsatisfiable`, `off-axis`. `provider` is `gke-computeclass`; `axis` is the ComputeClass name.

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lookout_leeway_preference_pod_time_seconds_total` | Counter | `provider`, `axis`, `spec_hash`, `rank` | Pod-seconds accumulated at each rank; only pods that asked for the class by `nodeSelector` accrue time. `spec_hash` changes when the class is edited. |
| `lookout_leeway_preference_rank_weighted_time_seconds_total` | Counter | `provider`, `axis`, `spec_hash` | Sum of rank × pod-seconds over tier ranks; divide by `pod_time` over the same ranks for mean achieved rank. |
| `lookout_leeway_preference_pods` | Gauge | `provider`, `axis`, `rank` | Pods currently at each rank (same population as `pod_time`). |
| `lookout_leeway_preference_nodes` | Gauge | `provider`, `axis`, `rank`, `rule_index`, `rule`, `source` | Nodes resolved to each rank; `source` is `annotation`, `inferred`, or `none`. |
| `lookout_leeway_preference_transitions_total` | Counter | `provider`, `axis`, `from_rank`, `to_rank`, `lateral` | Nodes moving between placements on one axis; `lateral=true` is a move between two rules sharing a tier. |
| `lookout_leeway_preference_unmatched` | Gauge | `provider`, `axis` | Nodes no priority rule matched (threshold-zero alert). |
| `lookout_leeway_preference_ambiguous` | Gauge | `provider`, `axis` | Nodes more than one priority rule matched. |
| `lookout_leeway_preference_disagreement` | Gauge | `provider`, `axis` | Nodes where GKE's `ccc_priority_index` annotation and the inferred rank disagree (threshold-zero alert). |
| `lookout_leeway_preference_out_of_range` | Gauge | `provider`, `axis` | Nodes whose annotation names a priority index the class no longer has. |
| `lookout_leeway_preference_axis_invalid` | Gauge | `provider`, `axis` | Nodes on a class whose priorities are partially scored, which `leeway` declines to order. |
| `lookout_leeway_preference_no_rule_matching` | Gauge | `provider`, `axis` | Nodes GKE itself could not fit to any rule (`ccc_no_rule_matching`); rank `unsatisfiable`. |
| `lookout_leeway_preference_off_axis` | Gauge | `provider`, `axis` | Nodes GKE provisioned outside the priority list (`ccc_scale_up_anyway`); rank `off-axis`. |
| `lookout_leeway_preference_rank_pending` | Gauge | `provider`, `axis` | Nodes carrying a compute class with no rank resolved yet. |
| `lookout_leeway_preference_unsupported_rules` | Gauge | `provider`, `axis`, `field` | Priority rules the matcher could not evaluate, by the field responsible. |
| `lookout_leeway_preference_unreadable_class_nodes` | Gauge | `provider`, `class` | Nodes labelled with a compute class this source has no decoded spec for. |
| `lookout_leeway_preference_decode_errors_total` | Counter | `provider`, `class` | ComputeClass objects the source refused to read. |
| `lookout_leeway_preference_tracker_underflows_total` | Counter | — | Unbalanced pod departures in the rank accounting (threshold-zero alert; a bug in `leeway`). |
| `lookout_leeway_preference_axis_info` | Gauge | `provider`, `axis`, `spec_hash`, `ordering`, `rules`, `tiers`, `scale_up`, `active_migration` | Constant `1` describing one axis: `ordering` is `list-position`, `priority-score` or `invalid`; `scale_up` is `whenUnsatisfiable` as declared. |
| `lookout_leeway_preference_alert_state` | Gauge | `provider`, `axis`, `rank_rule`, `tier`, `phase` | Dwell-machine position of one rule on one axis: `1` pending, `2` firing. Episodes are per rule; only open ones have a row. |
| `lookout_leeway_preference_wedged_pods` | Gauge | `provider`, `class` | Unscheduled pods that named a compute class by `nodeSelector` and did not get one. |

## OTLP export SLIs (only with `--otel-exporter=otlp`)

Plain Prometheus collectors on the pull registry, describing the push path:

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `lookout_otlp_exports_total` | Counter | `outcome` | OTLP export attempts, `ok` or `failed`. |
| `lookout_otlp_points_exported_total` | Counter | — | Data points delivered to the collector. |
| `lookout_otlp_points_dropped_total` | Counter | — | Data points discarded because their export failed or ran out of time (no retry queue by design). |
| `lookout_otlp_export_last_success_timestamp_seconds` | Gauge | — | Unix time of the last export the collector accepted; zero until the first. Alert on its age. |
| `lookout_otlp_export_inflight` | Gauge | — | `1` while an export is in flight. |

The standard Go runtime (`go_*`) and process (`process_*`) collectors are registered as well.
