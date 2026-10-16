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
  --cluster=gke-prod-us-east1 \
  --webhook-url=http://localhost:8080/webhook \
  --sink-token-env=LEEWAY_WEBHOOK_TOKEN \
  --dedup-window=10m \
  --recovery-window=5m
```

- `--sink-token-env`: Name of an environment variable holding a Bearer token sent in `Authorization: Bearer <token>`.
- `--dry-run`: Logs every payload that would be POSTed without making outbound HTTP requests.
- `--dedup-window` (default `10m`): Suppresses duplicate emissions for the same `(UID, Reason)` within the window.
- `--recovery-window` (default `5m`): How long a symptom must stay continuously clear before `leeway` sends a `resolved` event (`0` disables recovery tracking).

## 1. Incident open & reminder (`POST <base-url>/incidents`)

When an incident opens (or re-notifies after `--dedup-window` while still open), `leeway` sends `POST <base-url>/incidents` with `Content-Type: application/json`:

```json
{
  "schema_version": 1,
  "kind": "leeway.contract_violated",
  "fingerprint": "a1b2c3d4e5f60718",
  "severity": "critical",
  "cluster": "gke-prod-us-east1",
  "namespace": "prod",
  "object_kind": "Deployment",
  "object_name": "checkout",
  "reason": "capacity_shortfall",
  "summary": "[gke-prod-us-east1] critical leeway.contract_violated on Deployment prod/checkout",
  "message": "topology contract violated on topology.kubernetes.io/zone",
  "first_seen": "2026-10-16T10:20:00Z",
  "last_seen": "2026-10-16T10:30:00Z",
  "count": 1,
  "leeway_finding": {
    "kind": "leeway.contract_violated",
    "tier": "A",
    "severity": "critical",
    "subject": {
      "kind": "Deployment",
      "namespace": "prod",
      "name": "checkout"
    },
    "topologyKey": "topology.kubernetes.io/zone",
    "score": {
      "drift": 0.45,
      "observedSkew": 5,
      "minAchievableSkew": 0,
      "excessSkew": 4,
      "relocationDistance": 4,
      "maxDomainShare": 0.67
    },
    "suspectedCause": "capacity_shortfall"
  }
}
```

### Session binding response

If your webhook receiver responds with HTTP `2xx` and a JSON body containing `"id"` (or `"incident_id"`):

```json
{"id": "inc-42"}
```

`leeway` binds the incident to `"inc-42"`. Subsequent reminders include `"session_id": "inc-42"`, and recovery notifications are posted to `/incidents/inc-42/events`.

## 2. Incident resolution (`POST <base-url>/incidents/<id>/events`)

Once the source's `ClearanceObserver` confirms the breach has cleared and remained clear for `--recovery-window`, `leeway` sends `POST <base-url>/incidents/<id>/events`:

```json
{
  "schema_version": 1,
  "kind": "resolved",
  "fingerprint": "a1b2c3d4e5f60718",
  "cluster": "gke-prod-us-east1",
  "namespace": "prod",
  "object_kind": "Deployment",
  "object_name": "checkout",
  "reason": "capacity_shortfall",
  "resolution": "recovered",
  "cleared_after_seconds": 420,
  "observed_stable_for_seconds": 300,
  "resolved_at": "2026-10-16T10:37:00Z"
}
```
