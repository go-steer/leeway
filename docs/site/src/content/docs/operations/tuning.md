---
title: Tuning placement findings
description: What the three confidence tiers mean, which flags move each one, how to author a LeewayPolicy, and how to control per-domain metric cardinality.
sidebar:
  order: 2
---

Nothing on this page changes what is measured. Every flag below changes what is *reported*, and the core Prometheus gauges stay complete either way. `leeway watch --help` prints the authoritative list for the installed build.

## Per-workload severity with `LeewayPolicy`

The tier sets a finding's default severity (`A` → `critical`, `B` → `warning`, `C` → `info`). To override severity for specific workloads or namespaces, install `deploy/crds/leewaypolicies.yaml` and create a `LeewayPolicy` (namespaced) or `ClusterLeewayPolicy`:

```yaml
apiVersion: leeway.lookout.go-steer.io/v1alpha1
kind: LeewayPolicy
metadata:
  name: api-zone-spread
  namespace: payments
spec:
  selector:
    matchLabels:
      app: api
  topologyKeys:
    - key: topology.kubernetes.io/zone
      mode: Spread
      thresholds:
        severity: critical   # info | warning | critical
```

The policy's key must be one of the tracked axes (`--topology-keys`); a policy carries no `maxSkew`, so it changes the severity and the expectation (`mode`, `weighting`, `expectedDistribution`), not the tier. Findings whose severity came from a policy carry `severityFromPolicy: true`.

## Which flag moves which tier

### Tier A and B — `topology-drift`

| Flag | Default | Effect |
| --- | --- | --- |
| `--topology-dwell` | `10m` | How long a breach must persist in `leeway watch` before it emits a signal. Raise on clusters that deploy continuously. |
| `--topology-keys` | `topology.kubernetes.io/zone,topology.kubernetes.io/region` | Topology axes evaluated, in precedence order. |
| `--topology-domain-unavailable-keys` | `topology.kubernetes.io/zone,topology.kubernetes.io/region` | Axes on which a domain with no usable node raises `leeway.domain_unavailable` (Tier B). Pass an empty string to turn the detector off. |
| `--topology-cluster-defaults` | `""` | Your `kube-scheduler` `defaultConstraints` as `key=maxSkew[:DoNotSchedule\|ScheduleAnyway],...`, or `none`. Empty assumes the system defaults, which are capped at Tier B; a declared default is judged like any other declared constraint. |

The switch from uniform to capacity-weighted expectation (largest-to-smallest domain allocatable CPU ratio of `1.25`) is a constant, not a flag.

### Tier C — learned baselines

Baselines are learned automatically in `leeway watch` for every scored subject, with fixed parameters (12h half-life; a band of 4 learned deviations with a 0.05 floor; mature after 200 samples and 6h). There is no flag to turn them off or retune them.

| Flag | Default | Effect |
| --- | --- | --- |
| `--store` | `""` | SQLite file that persists learned baselines (and dwell timers) across restarts. Without it a restart starts learning from scratch. |
| `--topology-tier-c-signals` | `false` | Opt-in to emit Tier C findings (`leeway.baseline_breach`, Tier C `leeway.placement_drift`) as signals/webhook incidents. |

### `compute-class`

| Flag | Default | Effect |
| --- | --- | --- |
| `--compute-class-dwell` | `10m` | Dwell time for rank findings in `leeway watch`. |
| `--compute-class-window` | `1h` | Rolling pod-time integration window for rank shares. |
| `--compute-class-last-rank-ceiling` | `0.9` | Fires `leeway.rank_degraded` when more than this share of pod-time ran on the least-preferred priority (`0` disables). |
| `--compute-class-rank0-floor` | `0` (off) | Fires `leeway.rank_degraded` when less than this share of pod-time ran on `rank 0`. |
| `--compute-class-tier-c-signals` | `false` | Opt-in to emit `leeway.rank_tier_unused` as signals/webhook incidents. |

## Per-domain metric cardinality controls

The per-domain breakdown gauges emit one series per `(subject, topology_key, domain, state)` for `lookout_leeway_domain_objects` and one per `(subject, topology_key, domain)` for `lookout_leeway_domain_expected`. By default they are gated: a subject exports its breakdown when it has an open episode or its drift is at least `0.05`, on at most 4 topology keys, with the four pod states collapsed to `active` / `waiting`. Subjects held back are counted in `lookout_leeway_domain_series_withheld{reason="gate"|"namespace"|"key_cap"}`. Node-group subjects are tracked up to 200 groups.

| Flag | Default | Effect |
| --- | --- | --- |
| `--topology-per-domain-series` | `false` | Export per-domain series for every tracked subject regardless of drift. |

The floor, key cap, state collapsing, namespace filters and node-group bound are fixed in this build (`topologydrift.Config` defaults); there are no flags for them.
