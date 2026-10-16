#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Shared helpers for examples/ scripts (kind/watcher setup, scenario
# inject/verify/revert, the e2e driver, and the kwok scale harness).

set -euo pipefail

examples_root() {
  cd "$(dirname "${BASH_SOURCE[0]}")" && pwd
}

repo_root() {
  git -C "$(examples_root)" rev-parse --show-toplevel
}

# ---- cluster + namespaces ------------------------------------------------

CLUSTER_NAME="${LEEWAY_EXAMPLES_CLUSTER:-leeway-examples}"
EXAMPLES_CONTEXT="${LEEWAY_EXAMPLES_CONTEXT:-kind-${CLUSTER_NAME}}"
SENTINEL_NS="agent-triage"
STATE_DIR="${LEEWAY_EXAMPLES_STATE:-${TMPDIR:-/tmp}/leeway-examples}"
LEEWAY_ZONE_KEY="lookout-examples/zone"

require_examples_context() {
  local ctx
  ctx="$(kubectl config current-context 2>/dev/null || true)"
  if [[ -z "$ctx" ]]; then
    echo "ERROR: no current kubectl context — run examples/kind/up first" >&2
    exit 1
  fi
  if [[ "$ctx" != "$EXAMPLES_CONTEXT" ]]; then
    echo "ERROR: current context is '$ctx', expected '$EXAMPLES_CONTEXT'." >&2
    echo "To run against a different staging cluster, set LEEWAY_EXAMPLES_CONTEXT." >&2
    exit 1
  fi
}

is_kind_context() {
  [[ "$EXAMPLES_CONTEXT" == kind-* ]]
}

# ---- node resource caps --------------------------------------------------

LEEWAY_KIND_NODE_CPUS="${LEEWAY_KIND_NODE_CPUS:-4}"
LEEWAY_KIND_NODE_MEMORY="${LEEWAY_KIND_NODE_MEMORY:-8g}"

cap_kind_nodes() {
  is_kind_context || return 0
  local cpus="$LEEWAY_KIND_NODE_CPUS" mem="$LEEWAY_KIND_NODE_MEMORY"
  [[ "$cpus" == none && "$mem" == none ]] && return 0

  local args=()
  [[ "$cpus" != none ]] && args+=(--cpus "$cpus")
  [[ "$mem" != none ]] && args+=(--memory "$mem" --memory-swap "$mem")

  local nodes
  nodes="$(kind get nodes --name "$CLUSTER_NAME" 2>/dev/null || true)"
  if [[ -z "$nodes" ]]; then
    return 0
  fi

  echo "▸ capping nodes at ${cpus} cpu / ${mem} mem (LEEWAY_KIND_NODE_CPUS=none to opt out)"
  local n
  while read -r n; do
    [[ -n "$n" ]] || continue
    docker update "${args[@]}" "$n" >/dev/null 2>&1 || true
  done <<<"$nodes"
}

# ---- the leeway binary ---------------------------------------------------

leeway_bin() {
  if [[ -n "${LEEWAY_BIN:-}" ]]; then
    echo "$LEEWAY_BIN"
    return 0
  fi
  local cached="$STATE_DIR/bin/leeway"
  if [[ ! -x "$cached" ]]; then
    mkdir -p "$STATE_DIR/bin"
    echo "▸ building leeway into $cached (first run only)" >&2
    (cd "$(repo_root)" && go build -o "$cached" ./cmd/leeway)
  fi
  echo "$cached"
}

run_leeway() {
  "$(leeway_bin)" "$@"
}

# ---- stub-webhook wire capture -------------------------------------------

stub_logs() {
  kubectl -n "$SENTINEL_NS" logs deploy/stub-webhook --tail=-1 2>/dev/null || true
}

stub_mark() {
  mkdir -p "$STATE_DIR"
  stub_logs | wc -l >"$STATE_DIR/$1.mark"
}

stub_since() {
  local mark=0
  [[ -f "$STATE_DIR/$1.mark" ]] && mark="$(cat "$STATE_DIR/$1.mark")"
  stub_logs | tail -n +"$((mark + 1))"
}

await() {
  local timeout="$1" desc="$2"
  shift 2
  timeout=$((timeout * ${LEEWAY_E2E_TIMEOUT_SCALE:-1}))
  local waited=0
  while true; do
    if "$@" >/dev/null 2>&1; then
      echo "  ✓ $desc (${waited}s)"
      return 0
    fi
    if ((waited >= timeout)); then
      echo "  ✗ TIMEOUT after ${timeout}s waiting for: $desc" >&2
      return 1
    fi
    sleep 5
    waited=$((waited + 5))
  done
}

await_namespace_settled() {
  if [[ "$(kubectl get namespace "$1" -o jsonpath='{.status.phase}' 2>/dev/null)" == Terminating ]]; then
    kubectl wait --for=delete "namespace/$1" --timeout=180s >/dev/null 2>&1 || true
  fi
}

fresh_namespace() {
  local ns="$1"
  await_namespace_settled "$ns"
  kubectl apply -f - <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: $ns
  labels: { app.kubernetes.io/part-of: leeway-examples }
YAML
}

await_inject() {
  local name="$1" timeout="$2"
  shift 2
  _stub_match() {
    local out
    out="$(stub_since "$name")"
    local pat
    for pat in "$@"; do
      out="$(grep -E "$pat" <<<"$out" || true)"
      [[ -n "$out" ]] || return 1
    done
    return 0
  }
  if await "$timeout" "webhook event matching: $*" _stub_match "$@"; then
    local out
    out="$(stub_since "$name")"
    local pat
    for pat in "$@"; do
      out="$(grep -E "$pat" <<<"$out" || true)"
    done
    head -n1 <<<"$out" | cut -c1-200 | sed 's/^/    wire: /'
    return 0
  fi
  return 1
}

leeway_zones() {
  kubectl label node "${CLUSTER_NAME}-worker" "$LEEWAY_ZONE_KEY=zone-a" --overwrite >/dev/null
  kubectl label node "${CLUSTER_NAME}-worker2" "$LEEWAY_ZONE_KEY=zone-b" --overwrite >/dev/null
}

leeway_wire_count() {
  local name="$1" out pat
  shift
  out="$(stub_since "$name")"
  for pat in "$@"; do
    out="$(grep -E "$pat" <<<"$out" || true)"
  done
  [[ -n "$out" ]] || {
    echo 0
    return 0
  }
  wc -l <<<"$out" | tr -d ' '
}
