# leeway false-positive soak

Leeway's detection rules are verified end to end on breaching fixtures by
`examples/scenarios/placement-drift`, `zone-unavailable`, and
`compute-class-fallback`. Those scenarios prove `leeway` fires when it should.
This soak tests the other half: that `leeway watch` stays quiet when it should.
It runs `leeway watch` at production settings against a regional-shaped cluster
that churns the way a healthy cluster churns, for hours or days, and records
every finding that reaches the webhook receiver or watcher log alongside
periodic `leeway report` snapshots. With no fault injected the right
dwell-gated finding count is zero, so each `leeway watch` finding is either a
false positive or real drift that the churn produced. `summarize.py` lays out
the evidence for deciding which.

This is not a unit-CI step: run it locally for a 45-minute smoke pass or on a
spare VM for a 72-hour soak and paste the verdict.

## What it builds

- **Cluster.** A kind cluster with one control plane and six workers:
  - two workers in each of `zone-a`, `zone-b`, and `zone-c` on the real
    `topology.kubernetes.io/zone` label;
  - two node pools (`cloud.google.com/gke-nodepool`), each spanning all
    three zones, so that node-group subjects are scored too.
- **Watcher (`leeway watch`).** The shipped `deploy/` manifests with
  production thresholds and dwell:
  - Every `--topology-*` and `--compute-class-*` threshold/dwell stays at its
    default: 10m dwell, learned EWMA baselines on, assumed cluster defaults.
  - `--topology-tier-c-signals` and `--compute-class-tier-c-signals` are added
    so Tier C (`info`) findings are dispatched to the webhook and counted too.
  - `--sources` uses the binary default (`topology-drift,compute-class`,
    auto-skipping `compute-class` when the GKE CRD is not served).
  - Runs on the control plane alongside the webhook capture stub so node churn
    never disrupts either pod.
- **One-shot report (`leeway report`).** Every 10 minutes (and once at the end
  of the run), `soak` also runs `./cmd/leeway`'s `leeway report` against the
  cluster (`reports.jsonl`, `report-final.json`) to verify the snapshot path
  completes cleanly under churn and to contrast instantaneous (dwell-bypassed)
  skew against dwell-gated `leeway watch` incidents.
- **Workloads.** 36 workloads in `fp-soak`, built by `workloads.py`:
  - no declared intent (scored against assumed cluster defaults and learned
    baselines);
  - zone spread, soft (`ScheduleAnyway`) and hard (`DoNotSchedule`);
  - pod anti-affinity, required and preferred, on zone and on host;
  - `podAffinity` colocation;
  - two StatefulSets and five `PodDisruptionBudget`s;
  - `pause` containers with minimal requests so scheduler spread rules — never
    capacity — decide placement.
- **Webhook capture stub.** `dev/drills/stub-webhook.py`'s handler (`POST
  /incidents` and `POST /incidents/<id>/events`), served by `serve.py` on a
  `ThreadingHTTPServer` so concurrent keep-alive connections never block
  webhook deliveries, timestamped by `stamp.py`, and written to a `hostPath`
  mount (`$SOAK_DIR/wire/wire.log`).
- **Churn.** Run by `churn.py`, seeded (`SOAK_SEED`, default `416`), with every
  action logged:
  - Every 1–4 minutes: a rollout restart, an image-tag bump, a replica scale of
    ±1..3 within bounds, or a pod deletion.
  - An autoscaled Deployment (`hpa-osc`) follows a 90-minute sine wave.
  - Every 30–90 minutes it disrupts **one** worker node in one of two ways:
    - drain via the Eviction API (honoring PDBs), hold cordoned for 2–7
      minutes, then uncordon;
    - `docker pause` for 50–100 s so kubelet misses heartbeats and blips
      `NotReady`.
  - No zone ever loses both of its nodes.

### Unlabelled control-plane nodes & `SOAK_ZONE_AFFINITY`

On managed GKE, every schedulable node carries `topology.kubernetes.io/zone`;
on `kind`, the control-plane node has no zone label (`DomainUnknown`,
`__unknown__`). `pkg/leeway`'s `Eligibility.DropEmptyUnknown` excludes
`__unknown__` whenever a subject holds zero pods there so an unlabelled
control plane never receives an unachievable share expectation. By default
`workloads.py` also sets `SOAK_ZONE_AFFINITY=1` (matching GKE's universal
zone-label invariant); pass `SOAK_ZONE_AFFINITY=0` to exercise
`DropEmptyUnknown` directly without `nodeAffinity`.

## What it records

Everything is written under `$SOAK_DIR`:

| File | Contents |
|---|---|
| `wire/wire.log` | Every `INCIDENT-OPEN` / `INCIDENT-EVENT` webhook request `leeway watch` sent to `stub-webhook`, UTC-timestamped |
| `watcher.log`, `watcher-final.log` | `leeway watch` container stderr logs across restarts |
| `store/leeway.db` | SQLite `--store` file holding persisted alert dwell timers, EWMA baselines, and bound incident sessions |
| `metrics.log` | `lookout_leeway_*` and Go/process memory metrics scraped from `leeway-metrics:9090/metrics` every 10 min |
| `placement.log` | Node zones/readiness and every pod's node, phase, and owner every 10 min |
| `reports.jsonl`, `report-final.json` | Periodic and end-of-run `leeway report` JSON snapshots (dwell bypassed) |
| `churn.log` | Every seeded workload and node churn action |
| `summary.txt` | Output of `summarize.py` ending with `VERDICT:` |

How to read a finding in `summary.txt`:
- A finding raised after a scale-down whose placement in `placement.log` truly
  remained skewed past the 10-minute dwell is real drift (the ReplicaSet
  controller selects deletion victims without regard to zone spread).
- A finding whose placement is even, or one that fired on a short drain or
  `NotReady` blip that §7.6 transient suppression covers, is a false positive.

## Running it

On any Linux host with `docker`, `kind`, `kubectl`, and `python3` (`soak up`
auto-builds `leeway:fp-soak` from `./Dockerfile` and `./cmd/leeway` if not
already built):

```sh
docker build -t leeway:fp-soak .
SOAK_DIR=$PWD/out dev/soak/leeway-fp/soak up
SOAK_DIR=$PWD/out SOAK_CHURN_SCALE=0.25 dev/soak/leeway-fp/soak run 45m   # smoke
SOAK_DIR=$PWD/out dev/soak/leeway-fp/soak run 72h
dev/soak/leeway-fp/soak down
```

`soak` exports `KUBECONFIG=$SOAK_DIR/kubeconfig` and never touches
`~/.kube/config`. Before a full 72-hour run, tear down the smoke cluster
(`soak down`, then `soak up`) and use a fresh `SOAK_DIR` so smoke baselines and
wire logs do not carry over.

### On a throwaway GCE VM

For a multi-day soak on an `e2-standard-8` VM with no external IP (reached over
IAP, with Cloud NAT for one-off setup downloads):

```sh
gcloud compute instances create leeway-soak-fp --zone us-central1-a \
  --machine-type e2-standard-8 --image-family debian-12 --image-project debian-cloud \
  --boot-disk-size 50GB --no-address --labels purpose=leeway-soak,issue=29 \
  --max-run-duration 78h --instance-termination-action STOP
docker build -t leeway:fp-soak \
  --build-arg VERSION="$(git describe --tags --always --dirty)" \
  --build-arg COMMIT="$(git rev-parse HEAD)" \
  --build-arg BUILD_DATE="$(date -u +%FT%TZ)" .
docker save leeway:fp-soak -o /tmp/leeway-fp-soak.tar
tar czf /tmp/bundle.tgz deploy dev/drills/stub-webhook.py dev/soak/leeway-fp
gcloud compute scp --tunnel-through-iap /tmp/bundle.tgz /tmp/leeway-fp-soak.tar leeway-soak-fp:/tmp/
gcloud compute ssh --tunnel-through-iap leeway-soak-fp --command '
  sudo mkdir -p /opt/fpsoak && sudo tar xzf /tmp/bundle.tgz -C /opt/fpsoak &&
  sudo mv /tmp/leeway-fp-soak.tar /opt/fpsoak/ && sudo /opt/fpsoak/dev/soak/leeway-fp/vm-bootstrap'
gcloud compute ssh --tunnel-through-iap leeway-soak-fp --command '
  cd /opt/fpsoak && sudo systemd-run --unit fpsoak --working-directory=/opt/fpsoak \
    --setenv=SOAK_DIR=/opt/fpsoak/run --setenv=LEEWAY_IMAGE_TAR=/opt/fpsoak/leeway-fp-soak.tar \
    /bin/bash -c "dev/soak/leeway-fp/soak up && dev/soak/leeway-fp/soak run 72h; sync; poweroff" \
    >/dev/null'
```

When the run finishes (VM state `TERMINATED`), start the instance again to read
`/opt/fpsoak/run/summary.txt`, then delete the VM:

```sh
gcloud compute instances start leeway-soak-fp --zone us-central1-a
gcloud compute ssh --tunnel-through-iap leeway-soak-fp --command 'sudo cat /opt/fpsoak/run/summary.txt'
```
