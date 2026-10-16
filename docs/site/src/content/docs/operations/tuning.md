---
title: Tuning placement findings
description: What the three confidence tiers mean, which flags move each one, how to author a LeewayPolicy, and how to control per-domain metric cardinality.
sidebar:
  order: 2
---

Nothing on this page changes what is measured. Every flag below changes what is *reported*, and the core Prometheus gauges stay complete either way.

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

## Which flag moves which tier

### Tier A and B — `topology-drift`

| Flag | Default | Effect |
| --- | --- | --- |
| `--topology-dwell` | `10m` | How long a breach must persist in `leeway watch` before it emits a signal. Raise on clusters that deploy continuously. |
| `--topology-keys` | `topology.kubernetes.io/zone,topology.kubernetes.io/region` | Topology axes evaluated by default. |
| `--topology-capacity-ratio` | `1.25` | Largest-to-smallest domain allocatable CPU ratio at which expectation switches from uniform to capacity-weighted. |
| `--topology-cluster-defaults` | — | Your `kube-scheduler` `defaultConstraints` (`key:maxSkew:ScheduleAnyway,...`). Capped at Tier B. |

### Tier C — learned baselines

| Flag | Default | Effect |
| --- | --- | --- |
| `--topology-learn-baselines` | `true` | Enables EWMA baseline learning for workloads without declared constraints. |
| `--topology-baseline-band` | `4` | Width of the learned tolerance band (in learned mean-absolute-deviations). Widen first if Tier C is noisy. |
| `--topology-baseline-half-life` | `12h` | How fast a learned baseline absorbs a step change. |
| `--topology-tier-c-signals` | `false` | Opt-in to emit Tier C findings (`leeway.baseline_breach`) as signals/webhook incidents. |

### `compute-class`

| Flag | Default | Effect |
| --- | --- | --- |
| `--compute-class-dwell` | `10m` | Dwell time for rank findings in `leeway watch`. |
| `--compute-class-window` | `1h` | Rolling pod-time integration window for rank shares. |
| `--compute-class-last-rank-ceiling` | `0.9` | Fires `leeway.rank_degraded` when more than this share of pod-time ran on the least-preferred priority (`K-1`). |
| `--compute-class-rank0-floor` | `0` (off) | Fires `leeway.rank_degraded` when less than this share of pod-time ran on `rank 0`. |
| `--compute-class-tier-c-signals` | `false` | Opt-in to emit `leeway.rank_tier_unused` as signals/webhook incidents. |

## Per-domain metric cardinality controls

The per-domain breakdown gauges (`lookout_leeway_domain_objects` and `lookout_leeway_domain_expected`) emit one series per `(subject, topology_key, domain, state)`. By default they are gated by a minimum drift threshold:

| Flag | Default | Effect |
| --- | --- | --- |
| `--topology-per-domain-min-drift` | `0.05` | Only subjects drifting by at least this fraction export per-domain series. |
| `--topology-per-domain-series` | `false` | Export per-domain series for every tracked subject regardless of drift. |
| `--topology-per-domain-namespaces` | all | Restrict per-domain series to named namespaces. |
| `--topology-per-domain-exclude-namespaces` | — | Exclude named namespaces from per-domain series (takes precedence over include list). |
| `--topology-per-domain-max-keys` | `4` | Maximum topology keys per subject that may export per-domain series. |
| `--topology-per-domain-collapse-states` | `true` | Collapses 4 pod states onto 2 (`running`, `other`), halving series cardinality. |
| `--topology-max-node-groups` | `200` | Upper bound on node groups before node-group scoring is withheld (`lookout_leeway_domain_series_withheld`). |
