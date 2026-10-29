---
title: Webhooks and sinks
description: How leeway watch dispatches incident open, reminder, and resolution payloads to an HTTP webhook callback.
sidebar:
  order: 3
---

In `leeway watch` mode, findings that clear their dwell window are logged to `stderr`, counted in `lookout_leeway_standalone_signals_total`, and—when a webhook is configured—delivered over HTTP with deduplication and automatic recovery tracking.

## Configuring the webhook sink

Pass `--webhook-url=<base-url>` (or `--sink=webhook --sink-url=<base-url>`):

```sh
leeway watch \
  --cluster-name=gke-prod-us-east1 \
  --webhook-url=http://localhost:8080/webhook \
  --sink-token-env=LEEWAY_WEBHOOK_TOKEN \
  --dedup-window=15m \
  --recovery-stable-for=2m
```

- `--sink-token-env`: Name of an environment variable holding a Bearer token sent in `Authorization: Bearer <token>`.
- `--dry-run`: Logs every payload that would be POSTed to stderr without making outbound HTTP requests.
- `--dedup-window` (default `15m`): Rolling deduplication window for the same `(uid, reason)`; while an incident stays open, reminders are appended on a doubling schedule starting at this window.
- `--recovery-stable-for` (default `2m`): How long a symptom must stay continuously clear before `leeway` sends a `resolved` event.

The payloads below were captured from the in-repo end-to-end suite (`test/e2e`), which runs the real `leeway watch` binary against a mock API server and an HTTP receiver.

## 1. Incident open (`POST <base-url>/incidents`)

When an incident opens, `leeway` sends `POST <base-url>/incidents` with `Content-Type: application/json`. The body is the signal-schema v1 `inject.Payload`; `reason` is the finding's `suspectedCause` (or, for rank findings, its `rule`), and the full finding rides under `leeway_finding` (`leeway_rank_finding` for `leeway.rank_*` kinds):

```json
{
  "kind": "leeway.contract_violated",
  "reason": "unknown",
  "namespace": "leeway-drift",
  "kind_of_object": "Deployment",
  "name": "spread-hard",
  "uid": "leeway:Deployment/leeway-drift/spread-hard|lookout-examples/zone",
  "message": "topology contract violated on lookout-examples/zone (tier A, drift 0.50, excess skew 3, 2 object(s) misplaced): zone-a 4/2, zone-b 0/2 — suspected unknown; 0 pods pinned by zonal volumes; intent topology-spread-constraint (declared)",
  "count": 1,
  "first_seen": "2026-10-25T15:12:18.723996735Z",
  "last_seen": "2026-10-25T15:12:19.024241074Z",
  "cluster": "e2e-drift",
  "source": "sentinel",
  "severity": "critical",
  "fingerprint": "sha256:cd76b5406118e483e38c111e863e4c8207ead1b808c92d1dc534d80ad057ee0e",
  "context": {},
  "type": "",
  "leeway_finding": {
    "kind": "leeway.contract_violated",
    "subject": {
      "kind": "Deployment",
      "namespace": "leeway-drift",
      "name": "spread-hard"
    },
    "topologyKey": "lookout-examples/zone",
    "tier": "A",
    "severity": "critical",
    "score": {
      "drift": 0.5,
      "observedSkew": 4,
      "minAchievableSkew": 0,
      "excessSkew": 3,
      "relocationDistance": 2,
      "maxDomainShare": 1
    },
    "intent": {
      "source": "topology-spread-constraint",
      "confidence": "declared",
      "weighting": "Equal",
      "maxSkew": 1,
      "evidence": [
        "topologySpreadConstraints[0]: topologyKey=lookout-examples/zone maxSkew=1 whenUnsatisfiable=DoNotSchedule labelSelector=app.kubernetes.io/name=spread-hard"
      ]
    },
    "domains": [
      {
        "domain": "zone-a",
        "actual": 4,
        "expected": 2,
        "delta": 2,
        "readyNodes": 1
      },
      {
        "domain": "zone-b",
        "actual": 0,
        "expected": 2,
        "delta": -2,
        "readyNodes": 1
      }
    ],
    "suspectedCause": "unknown",
    "contributingFactors": [
      "0 pods pinned by zonal volumes"
    ],
    "untestedCauses": [
      "rollout_bias"
    ],
    "firstSeenAt": "2026-10-25T15:12:18.723996735Z",
    "remediation": {
      "kind": "rollout_restart",
      "summary": "Spread intent on lookout-examples/zone is already configured for Deployment/leeway-drift/spread-hard and all eligible domains are healthy; trigger a rolling restart (or run Descheduler RemovePodsViolatingTopologySpreadConstraint) to rebalance existing replicas.",
      "command": "kubectl -n leeway-drift rollout restart deployment/spread-hard"
    }
  }
}
```

`project`, `region` and `zone` are added when the cluster's identity is known; `reason_class` and `repeat_count` appear only when set. See [Finding kinds](/reference/finding-kinds/) for the `leeway_finding` and `leeway_rank_finding` schemas.

### Session binding response

If your webhook receiver responds with HTTP `2xx` and a JSON body containing `"id"`:

```json
{"id": "inc-42"}
```

`leeway` binds the incident to `"inc-42"`: reminders while it stays open, and its resolution, are posted to `/incidents/inc-42/events`. A `2xx` without a parseable `id` is accepted too; `leeway` then generates a local id (logged once) and the receiver sees it only in the append URL. Reminders carry the same payload shape as the open, with `count` incremented and `repeat_count` set when nothing material changed since the previous one.

## 2. Incident resolution (`POST <base-url>/incidents/<id>/events`)

Once the source's clearance observer confirms the breach has cleared and remained clear for `--recovery-stable-for`, `leeway` sends `POST <base-url>/incidents/<id>/events` with an `inject.ResolvedPayload`. Durations are Go `time.Duration` strings:

```json
{
  "kind": "resolved",
  "reason": "wedged",
  "namespace": "",
  "kind_of_object": "PreferenceAxis",
  "name": "gold-ccc",
  "uid": "leeway-rank:PreferenceAxis/gold-ccc|gke-computeclass/wedged",
  "fingerprint": "sha256:c33e81a9664d5aa23aa7443a08fb4d7fc76d937bebb13a884938adf6a6480b22",
  "cluster": "e2e-cc",
  "first_seen": "2026-10-25T15:12:19.403691119Z",
  "resolved_at": "2026-10-25T15:12:22.202256311Z",
  "cleared_after": "2.348378328s",
  "observed_stable_for": "450.186874ms",
  "resolution": "recovered",
  "context": {}
}
```

`resolution` is `recovered` (the object or its replacement is healthy) or `object_deleted` (the object is gone). If the symptom recurs after resolution, a `"kind": "resolved.reverted"` record with `reverted_after` follows on the same endpoint.
