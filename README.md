# Leeway (`github.com/go-steer/leeway`)

**Leeway** is a standalone Kubernetes placement-drift and compute-class priority rank analyzer and incident daemon extracted from `k8s-lookout`.

Leeway evaluates two classes of scheduling and placement behavior across one or more Kubernetes clusters:
1. **Topology Placement Drift (`topology-drift`)**: Evaluates workload pod distributions across topology domains (`topology.kubernetes.io/zone` and `topology.kubernetes.io/region` by default; any node label via `--topology-keys`), reads or infers placement intent from `LeewayPolicy` / `ClusterLeewayPolicy`, pod specs, and cluster defaults, computes capacity-weighted or uniform expectation, scores excess skew and relocation distance, and attributes root causes (`domain_outage`, `consolidation`, `taint_exclusion`, `domain_capacity_shortfall`, `volume_pinning`, `rollout_bias`, `constraint_ignored`, or `unknown`).
2. **ComputeClass Priority Rank Occupancy (`compute-class`)**: Evaluates GKE `ComputeClass` (`cloud.google.com/v1`) priority ladders, tracking pod-seconds and pod shares per priority rank to detect rank degradation (`leeway.rank_degraded`), wedged fallback tiers (`leeway.rank_wedged`), stalled scaleback (`leeway.rank_no_migration`), and unused preferred tiers (`leeway.rank_tier_unused`).

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

Designed to be invoked by an external process, CLI workflow, or AI agent. Connects to one or more target Kubernetes clusters in parallel, takes a point-in-time snapshot of nodes, pods, workloads, PV/PVCs, events, `LeewayPolicy` CRDs, and `ComputeClass` CRDs, runs the full Leeway evaluation pipeline with dwell bypassed, and prints a structured JSON report to `stdout` (or `--output=<file>`).

```bash
# Evaluate explicit targets: GKE triples, name=endpoint pairs, or kubeconfig contexts
leeway report --clusters=my-project/us-east1/gke-prod-us-east1,gke-prod-us-west1=https://10.0.0.2:6443

# Evaluate every context in a kubeconfig, or every GKE cluster in a project
leeway report --clusters-from=kubeconfig:/etc/leeway/kubeconfig
leeway report --clusters-from=my-project

# Evaluate every Cluster Agent profile in a Hermes profiles directory
leeway report --profiles-dir=/opt/data/profiles

# Evaluate a single kubeconfig / context
leeway report --kubeconfig=~/.kube/config --context=gke_my-project_us-central1_prod
```

#### `leeway report` Flags

| Flag | Default | Description |
|---|---|---|
| `--clusters` | `""` | Comma-separated cluster targets (`project/location/cluster`, `name=https://host:port`, or kubeconfig context names); a filter when `--profiles-dir` or `--clusters-from` is set |
| `--clusters-from` | `""` | Fleet discovery source: `kubeconfig[:/path]`, `profiles:/path/to/profiles`, or `<project>[/<location>]` |
| `--profiles-dir` | `""` | Hermes profiles directory; each Cluster Agent profile's `config.yaml` carries a `cluster_identity` block |
| `--kubeconfig` | `""` | Path to kubeconfig file (falls back to `KUBECONFIG`, then `~/.kube/config`) |
| `--context` | `""` | Kubeconfig context to use |
| `--in-cluster` | `false` | Use in-cluster service account credentials |
| `--cluster-name` | `""` | Logical cluster name for single-cluster reports (default: the context name, else `default`) |
| `--namespace` | `""` | Comma-separated namespaces to evaluate (empty queries all namespaces) |
| `--sources` | `topology-drift,compute-class` | Comma-separated sources to evaluate |
| `--topology-tier-c-signals`, `--compute-class-tier-c-signals` | `false` | Include Tier C findings in `topology_findings` / `rank_findings` |
| `--all`, `--min-drift` | `false`, `0` | Include evaluated subjects in each cluster's `subjects` array (all of them, or those with `score.drift >= min-drift`) |
| `--parallelism` | `8` | Maximum number of clusters evaluated in parallel |
| `--timeout` | `60s` | Per-cluster snapshot and evaluation timeout |
| `--output`, `-o` | `""` (stdout) | Write JSON report to file path instead of stdout |
| `--pretty` | `true` | Indent the JSON; `--pretty=false` emits a single line |

See [the CLI reference](docs/site/src/content/docs/reference/cli.md) for `--topology-keys`, `--topology-domain-unavailable-keys`, `--topology-cluster-defaults`, `--project`, and `--location`.

#### Report JSON Output Structure

The envelope is snake_case; the finding objects inside it are camelCase. This is `leeway report --sources=topology-drift` output against a fake cluster, with the `topology.kubernetes.io/region` axis removed from `domains` for length:

```json
{
  "schema_version": "leeway.report/v1alpha1",
  "generated_at": "2026-10-19T12:00:00Z",
  "clusters": [
    {
      "cluster": "gke-prod-east",
      "source": "kubeconfig:gke-prod-east",
      "status": "ok",
      "domains": {
        "topology.kubernetes.io/zone": {
          "us-east1-a": {
            "nodes": 2,
            "ready": 2,
            "usable": 2,
            "allocatable_cpu_milli": 8000,
            "allocatable_mem_bytes": 34359738368
          },
          "us-east1-b": {
            "nodes": 2,
            "ready": 2,
            "usable": 2,
            "allocatable_cpu_milli": 8000,
            "allocatable_mem_bytes": 34359738368
          },
          "us-east1-c": {
            "nodes": 2,
            "ready": 2,
            "usable": 2,
            "allocatable_cpu_milli": 8000,
            "allocatable_mem_bytes": 34359738368
          }
        }
      },
      "topology_findings": [
        {
          "kind": "leeway.contract_violated",
          "subject": {
            "kind": "Deployment",
            "namespace": "prod",
            "name": "checkout"
          },
          "topologyKey": "topology.kubernetes.io/zone",
          "tier": "A",
          "severity": "critical",
          "score": {
            "drift": 0.6666666666666666,
            "observedSkew": 6,
            "minAchievableSkew": 0,
            "excessSkew": 6,
            "relocationDistance": 4,
            "maxDomainShare": 1
          },
          "intent": {
            "source": "topology-spread-constraint",
            "confidence": "declared",
            "weighting": "Equal",
            "maxSkew": 1,
            "evidence": [
              "topologySpreadConstraints[0]: topologyKey=topology.kubernetes.io/zone maxSkew=1 whenUnsatisfiable=DoNotSchedule labelSelector=app=checkout"
            ]
          },
          "domains": [
            {
              "domain": "us-east1-a",
              "actual": 0,
              "expected": 2,
              "delta": -2,
              "readyNodes": 2
            },
            {
              "domain": "us-east1-b",
              "actual": 6,
              "expected": 2,
              "delta": 4,
              "readyNodes": 2
            },
            {
              "domain": "us-east1-c",
              "actual": 0,
              "expected": 2,
              "delta": -2,
              "readyNodes": 2
            }
          ],
          "suspectedCause": "domain_capacity_shortfall",
          "contributingFactors": [
            "1 pods Pending with FailedScheduling: 0/6 nodes are available: 2 Insufficient cpu, 4 node(s) didn't match pod topology spread constraints.",
            "0 pods pinned by zonal volumes"
          ],
          "firstSeenAt": "2026-10-19T12:00:00Z",
          "remediation": {
            "kind": "cluster_capacity",
            "summary": "Deficit domain(s) (us-east1-a, us-east1-c) lack allocatable capacity to schedule replicas of Deployment/prod/checkout; scale node pools in us-east1-a, us-east1-c or right-size pod resource requests rather than editing workload spread constraints."
          }
        }
      ]
    }
  ],
  "summary": {
    "clusters_total": 1,
    "clusters_succeeded": 1,
    "clusters_failed": 0,
    "topology_findings": 1,
    "rank_findings": 0,
    "by_severity": {
      "critical": 1
    },
    "by_tier": {
      "A": 1
    }
  }
}
```

A cluster that could not be evaluated has `"status": "error"` and an `error` string instead of `domains` and findings. The finding schemas are in [Finding kinds](docs/site/src/content/docs/reference/finding-kinds.md).

---

### 2. Continuous Daemon Mode (`leeway watch`)

Runs continuously as a daemon watching one or more clusters via shared informers, maintaining stateful dwell/hysteresis (`leeway.AlertTable`), EWMA baseline learning (`leeway.BaselineTable`), SQLite persistence (`--store`), Prometheus metrics (`/metrics`), health probes (`/healthz`, `/readyz`), and dispatching incidents and recovery notifications to an HTTP webhook callback.

```bash
# Watch a single cluster and report incidents to an HTTP webhook
leeway watch \
  --cluster-name=gke-prod-us-east1 \
  --webhook-url=http://localhost:8080 \
  --metrics-addr=:9090

# Watch multiple clusters from a single daemon with SQLite state persistence
leeway watch \
  --clusters=gke-prod-us-east1=https://10.0.0.1:6443,gke-prod-us-west1=https://10.0.0.2:6443 \
  --webhook-url=http://incident-receiver.monitoring.svc:8080 \
  --store=/var/lib/leeway/state.db \
  --metrics-addr=:9090
```

#### Webhook Callback Contract

When `--webhook-url=<base-url>` (or `--sink=webhook --sink-url=<base-url>`) is configured:

1. **Incident Open (`POST <base-url>/incidents`)**:
   - Sends an `inject.Payload` JSON body: `kind`, `reason`, `namespace`, `kind_of_object`, `name`, `uid`, `message`, `count`, `first_seen`, `last_seen`, `cluster`, `source`, `severity`, `fingerprint`, `context`, `type` **plus** the full structured Leeway payload under:
     - `leeway_finding` (`*leeway.Finding`) for topology placement drift findings
     - `leeway_rank_finding` (`*leeway.RankFinding`) for ComputeClass priority rank findings
   - If the receiver responds `2xx` with `{"id": "<incident-id>"}`, Leeway binds the incident to that id for reminders and recovery tracking. Reminders while the incident stays open go to `POST <base-url>/incidents/<id>/events` with the same payload shape (`repeat_count` is set when nothing material changed since the last one).

2. **Incident Resolution / Reversion (`POST <base-url>/incidents/<id>/events`)**:
   - When the observers confirm the topology drift or rank degradation has cleared and remained clear for `--recovery-stable-for` (default `2m`), Leeway posts an `inject.ResolvedPayload` (`kind: "resolved"`, `resolution: "recovered" | "object_deleted"`, `cleared_after` and `observed_stable_for` as Go duration strings, `resolved_at`) to the bound incident's `/events` endpoint. If the symptom recurs afterwards, a `kind: "resolved.reverted"` record with `reverted_after` follows.

3. **Optional Bearer Token Authentication**:
   - Set `--sink-token-env=LEEWAY_WEBHOOK_TOKEN` to attach `Authorization: Bearer <token>` from the named environment variable on every webhook request.
