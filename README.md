# Leeway (`github.com/go-steer/leeway`)

**Leeway** is a standalone Kubernetes placement-drift and compute-class priority rank analyzer and incident daemon extracted from `k8s-lookout`.

Leeway evaluates two classes of scheduling and placement behavior across one or more Kubernetes clusters:
1. **Topology Placement Drift (`topology-drift`)**: Evaluates workload pod distributions across topology domains (`topology.kubernetes.io/zone`, `topology.kubernetes.io/region`, `kubernetes.io/hostname`, or custom keys declared via `LeewayPolicy` / `ClusterLeewayPolicy`), infers or reads explicit placement intent, computes capacity-weighted or uniform expectation, scores excess skew and relocation distance, and attributes root causes (`capacity_shortfall`, `affinity_conflict`, `taint_mismatch`, `volume_topology_lock`, `domain_cordoned`, `rollout_bias`, `descheduler_absent`).
2. **ComputeClass Priority Rank Occupancy (`compute-class`)**: Evaluates GKE `ComputeClass` (`cloud.google.com/v1`) node-pool preference-order ladders (`priority_rule` and `autopilot` axes), tracking pod-seconds and pod shares per priority rank to detect rank degradation (`leeway.rank_degraded`), wedged fallback tiers (`leeway.rank_wedged`), stalled scaleback (`leeway.rank_no_migration`), and unused preferred tiers (`leeway.rank_tier_unused`).

---

## Building & Testing

```bash
# Build ./bin/leeway
make build

# Run all unit and integration tests
make test

# Run tests with the race detector
make test-race
```

---

## Execution Modes

Leeway supports two primary execution modes from a single binary (`leeway`):

### 1. One-Shot Multi-Cluster Report (`leeway report`)

Designed to be invoked by an external process, CLI workflow, or AI agent. Connects to one or more target Kubernetes clusters in parallel, takes a point-in-time snapshot of nodes, pods, workloads, PV/PVCs, events, `LeewayPolicy` CRDs, and `ComputeClass` CRDs, runs the full Leeway evaluation pipeline with dwell bypassed (`dwell=0`), and prints a structured JSON report to `stdout` (or `--output=<file>`).

```bash
# Evaluate explicit cluster endpoints or profile names
leeway report --clusters=gke-prod-us-east1=https://10.0.0.1:6443,gke-prod-us-west1=https://10.0.0.2:6443

# Evaluate clusters listed in a file (one per line; supports '#' comments)
leeway report --clusters-from=/etc/leeway/clusters.txt

# Evaluate all kube-agents profile directories under ~/.kube-agents/profiles
leeway report --profiles-dir=~/.kube-agents/profiles

# Evaluate a single kubeconfig / context
leeway report --kubeconfig=~/.kube/config --context=gke_my-project_us-central1_prod
```

#### `leeway report` Flags

| Flag | Default | Description |
|---|---|---|
| `--clusters` | `""` | Comma-separated cluster targets (`name=https://host:port`, `https://host:port`, or profile name) |
| `--clusters-from` | `""` | Path to file containing cluster targets (one per line) |
| `--profiles-dir` | `$KUBE_AGENTS_PROFILES_DIR` | Directory of `kube-agents` cluster profiles (`env` files with `KUBE_AGENT_API_SERVER` / `KUBE_AGENT_PROXY_URL`) |
| `--kubeconfig` | `$KUBECONFIG` | Path to kubeconfig file (fallback when no multi-cluster targets are set) |
| `--context` | `""` | Kubeconfig context to use |
| `--in-cluster` | `false` | Use in-cluster service account credentials |
| `--sources` | `topology-drift,compute-class` | Comma-separated sources to evaluate |
| `--concurrency` | `8` | Maximum number of clusters evaluated in parallel |
| `--timeout` | `60s` | Per-cluster snapshot and evaluation timeout |
| `--output`, `-o` | `""` (stdout) | Write JSON report to file path instead of stdout |
| `--compact` | `false` | Emit single-line compact JSON instead of indented JSON |

#### Report JSON Output Structure

```json
{
  "schemaVersion": "leeway.report/v1alpha1",
  "generatedAt": "2026-10-16T10:30:00Z",
  "summary": {
    "clustersTotal": 2,
    "clustersSucceeded": 2,
    "clustersFailed": 0,
    "topologyFindings": 1,
    "domainUnavailable": 0,
    "rankFindings": 1,
    "byTier": {
      "A": 1
    },
    "bySeverity": {
      "critical": 1,
      "warning": 1
    }
  },
  "clusters": [
    {
      "cluster": "gke-prod-us-east1",
      "source": "flag",
      "collectedAt": "2026-10-16T10:30:00Z",
      "durationMillis": 142,
      "nodeCount": 36,
      "podCount": 420,
      "policiesServed": true,
      "computeClassesServed": true,
      "domainAvailability": [
        {
          "topologyKey": "topology.kubernetes.io/zone",
          "domain": "us-east1-b",
          "readyNodes": 12,
          "totalNodes": 12
        }
      ],
      "topologyFindings": [
        {
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
      ],
      "rankFindings": []
    }
  ]
}
```

---

### 2. Continuous Daemon Mode (`leeway watch`)

Runs continuously as a daemon watching one or more clusters via shared informers, maintaining stateful dwell/hysteresis (`leeway.AlertTable`), EWMA baseline learning (`leeway.BaselineTable`), SQLite persistence (`--store`), Prometheus metrics (`/metrics`), health probes (`/healthz`, `/readyz`), and dispatching incidents and recovery notifications to an HTTP webhook callback.

```bash
# Watch a single cluster and report incidents to an HTTP webhook
leeway watch \
  --cluster=gke-prod-us-east1 \
  --webhook-url=http://localhost:8080 \
  --listen-addr=:9090

# Watch multiple clusters from a single daemon with SQLite state persistence
leeway watch \
  --clusters=gke-prod-us-east1=https://10.0.0.1:6443,gke-prod-us-west1=https://10.0.0.2:6443 \
  --webhook-url=http://incident-receiver.monitoring.svc:8080 \
  --store=/var/lib/leeway/state.db \
  --listen-addr=:9090
```

#### Webhook Callback Contract

When `--webhook-url=<base-url>` (or `--sink=webhook --sink-url=<base-url>`) is configured:

1. **Incident Open / Reminder (`POST <base-url>/incidents`)**:
   - Sends an `inject.Payload` JSON body with standard incident metadata (`schema_version`, `kind`, `fingerprint`, `severity`, `cluster`, `namespace`, `object_kind`, `object_name`, `summary`, `first_seen`, `last_seen`, `count`) **plus** the full structured Leeway payload under:
     - `leeway_finding` (`*leeway.Finding`) for topology placement drift findings
     - `leeway_rank_finding` (`*leeway.RankFinding`) for ComputeClass priority rank findings
   - If the receiver responds with `{"id": "<session-id>"}` (or `{"incident_id": "<session-id>"}`), Leeway binds the incident to that session ID for deduplication reminders and recovery tracking.

2. **Incident Resolution / Reversion (`POST <base-url>/incidents/<id>/events`)**:
   - When all observers confirm the topology drift or rank degradation has cleared and remained stable for `--recovery-window` (default `5m`), Leeway posts an `inject.ResolvedPayload` (`kind: "resolved"`, `resolution: "recovered" | "object_deleted"`, `cleared_after_seconds`, `observed_stable_for_seconds`) to the bound incident's `/events` endpoint.

3. **Optional Bearer Token Authentication**:
   - Set `--sink-token-env=LEEWAY_WEBHOOK_TOKEN` to attach `Authorization: Bearer <token>` from the named environment variable on every webhook request.
