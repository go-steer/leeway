# Drill: ComputeClass priority fallback & wedged scale-up (`leeway.rank_degraded`, `leeway.rank_wedged`)

This runbook exercises `leeway`'s `compute-class` source end-to-end against a
live GKE Standard cluster with Node Auto-Provisioning (NAP) and the GKE
`ComputeClass` (`cloud.google.com/v1`) CRD.

It validates both execution modes:
- **`leeway report`**: One-shot JSON report evaluation of `compute_classes` and `rank_findings`.
- **`leeway watch`**: Continuous daemon with Prometheus `/metrics` and HTTP webhook callbacks (`POST /incidents` and `POST /incidents/<id>/events`).

## Signals under test

| Signal kind | Trigger condition |
|---|---|
| `leeway.rank_degraded` (`last-rank`) | `> 90%` of windowed pod-seconds on the class's least-preferred rank after `--compute-class-window` and `--compute-class-dwell` |
| `leeway.rank_wedged` (`wedged`) | `whenUnsatisfiable: DoNotScaleUp` class has a `Pending` pod with a `NotTriggerScaleUp` / `FailedScaleUp` Event from `cluster-autoscaler` (and a control `Pending` pod with `TriggeredScaleUp` does **not** fire `rank_wedged`) |

## Prerequisites

- A GKE Standard cluster (`1.30+`) with Node Auto-Provisioning enabled.
- `kubectl` pointed at the staging cluster context.
- Local `leeway` binary (`go build -o bin/leeway ./cmd/leeway`).

## 1. Start the webhook receiver and `leeway watch`

In terminal 1, start the local stub webhook receiver:

```sh
python3 dev/drills/stub-webhook.py 7777
```

In terminal 2, start `leeway watch` with drill-speed windows:

```sh
./bin/leeway watch \
  --cluster-name=gke-drill \
  --sources=compute-class \
  --compute-class-window=5m \
  --compute-class-dwell=60s \
  --recovery-stable-for=60s \
  --webhook-url=http://127.0.0.1:7777
```

## 2. Apply the `ComputeClass` and fallback workload

Apply a 2-priority `ComputeClass` where priority 0 requests an unavailable machine family/shape in the target zone and priority 1 falls back to an available family (`n2-standard-4`), with `whenUnsatisfiable: DoNotScaleUp`:

```sh
kubectl apply -f examples/scenarios/compute-class-fallback/workloads.yaml
```

## 3. Verify one-shot `leeway report`

Run `leeway report` against the cluster:

```sh
./bin/leeway report --sources=compute-class | jq '.clusters[0] | {compute_classes, rank_findings}'
```

Expected:
- `compute_classes` lists `gold-ccc` with `scorable: true`, `pendingPods: 2`, `pendingProvisioning: 1`, `pendingDeclined: 1`.
- `rank_findings` includes `leeway.rank_wedged` and `leeway.rank_degraded` (`rule: "last-rank"`).

## 4. Verify `leeway watch` webhook delivery and resolution

1. Observe `INCIDENT-OPEN` lines in terminal 1 for `leeway.rank_wedged` and (once the 5m window + 60s dwell elapse) `leeway.rank_degraded`.
2. Delete the declined pod (`kubectl -n leeway-cc delete pod pod-wedged`) while leaving `pod-provisioning` (`TriggeredScaleUp`) in place.
3. After `--recovery-stable-for=60s`, verify `INCIDENT-EVENT` logs `kind=resolved` with `"resolution":"recovered"` for the `leeway.rank_wedged` incident ID.

## 5. Cleanup

```sh
examples/scenarios/compute-class-fallback/revert
```
