// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kube

// The shared-transform field registry.
//
// A transform on the shared informer factory is process-wide state. It mutates
// the objects every consumer sees, and downstream a stripped field is
// indistinguishable from a field that was genuinely empty — so the source that
// needed it does not error, it silently stops finding things. The failure mode
// is a detector that never fires again, discovered by a user, months later.
//
// This registry is the control for that. Every field the transform touches has
// an entry saying why, and every field it deliberately preserves has an entry
// naming the informer-reachable reader that requires it, with the file:line
// where that read happens. TestSharedTransform_StripsExactlyTheRegistry runs
// the transform over a fully-populated Pod and Node and compares the fields
// that actually changed against this table, so the two cannot drift: adding a
// strip without an entry fails, and an entry describing something the code no
// longer does fails too.
//
// Adding a field to a strip list therefore costs one entry and one sentence of
// justification. That is the intended price.
//
// "Informer-reachable" is the test for whether a reader counts. Read-path code
// that lists from the API server directly is unaffected by the transform. The
// enricher is the trap: internal/watch/enrich.go holds a livePod that reads
// straight from the shared pod lister (enrich.go:129-131) and hands the result
// to pkg/checks/state, pkg/checks/delta and pkg/checks/logs (enrich.go:77-79),
// so all three are informer-reachable even though they look like read-path
// check packages.

// fieldAction is what the shared transform does to a field.
type fieldAction int

const (
	// fieldStripped means the transform zeroes, nils or filters this field.
	fieldStripped fieldAction = iota
	// fieldPreserved means the transform deliberately leaves it intact because
	// a named consumer reads it. These are the entries that make the registry
	// worth having: they are the fields a future strip would break.
	fieldPreserved
)

// fieldEntry documents one field the shared transform has an opinion about.
type fieldEntry struct {
	// Kind is "Pod" or "Node".
	Kind string
	// Path is the JSON path with slice indices collapsed to "[]", e.g.
	// "spec.containers[].env[].value". It must match what the reflective diff
	// in transform_test.go produces.
	Path string
	// Action is whether the transform strips or preserves this field.
	Action fieldAction
	// Why justifies the action in one sentence.
	Why string
	// Readers lists informer-reachable readers as "file:line — what it reads".
	// Required for fieldPreserved (a field preserved for nobody should be
	// stripped) and required to be empty for fieldStripped (the claim being
	// made is precisely that nothing reads it).
	Readers []string
}

// sharedTransformRegistry is the single source of truth for the shared
// transform's effect on other consumers.
var sharedTransformRegistry = []fieldEntry{
	// ---------------------------------------------------------------- Pod: stripped
	{
		Kind:   "Pod",
		Path:   "metadata.managedFields",
		Action: fieldStripped,
		Why:    "Server-side-apply bookkeeping. Large, high-churn, and nothing in the repo reads it.",
	},
	{
		Kind:   "Pod",
		Path:   "metadata.annotations",
		Action: fieldStripped,
		Why:    "Filtered to drop full-manifest applied-configuration annotations (kubectl/kapp/rancher/flux last-applied blobs) while preserving all operational annotations.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.containers",
		Action: fieldStripped,
		Why:    "No leeway source reads container definitions; dropping the slice wholesale prevents secret env/arg values from entering process memory and shrinks cached Pods.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.initContainers",
		Action: fieldStripped,
		Why:    "Same reasoning as spec.containers; no leeway source reads init container specs.",
	},
	{
		Kind:   "Pod",
		Path:   "spec.ephemeralContainers",
		Action: fieldStripped,
		Why:    "Same reasoning as spec.containers; debug containers can carry literal secrets via `kubectl debug --env` and are not read by any leeway source.",
	},
	{
		Kind:   "Pod",
		Path:   "status.containerStatuses",
		Action: fieldStripped,
		Why:    "Per-container state is the largest sub-structure on a running Pod and is not read by topology-drift or compute-class (which use Pod-level Phase and Conditions).",
	},
	{
		Kind:   "Pod",
		Path:   "status.initContainerStatuses",
		Action: fieldStripped,
		Why:    "Same reasoning as status.containerStatuses.",
	},
	{
		Kind:   "Pod",
		Path:   "status.ephemeralContainerStatuses",
		Action: fieldStripped,
		Why:    "Same reasoning as status.containerStatuses.",
	},

	// --------------------------------------------------------------- Pod: preserved
	{
		Kind:   "Pod",
		Path:   "status.phase",
		Action: fieldPreserved,
		Why:    "Both topology-drift and compute-class ignore terminal (Succeeded/Failed) pods via podTerminal().",
		Readers: []string{
			"pkg/sources/topologydrift/state.go:702 — podTerminal() excludes Succeeded and Failed pods",
			"pkg/sources/computeclass/state.go:542 — podTerminal() excludes Succeeded and Failed pods",
		},
	},
	{
		Kind:   "Pod",
		Path:   "status.conditions",
		Action: fieldPreserved,
		Why:    "PodScheduled and PodReady conditions drive pending/unschedulable attribution and active replica counting.",
		Readers: []string{
			"pkg/sources/topologydrift/state.go:709 — podUnschedulable() and podReady() inspect PodScheduled and PodReady",
			"pkg/sources/computeclass/state.go:549 — podUnschedulable() inspects PodScheduled=False/Unschedulable",
		},
	},

	// --------------------------------------------------------------- Node: stripped
	{
		Kind:   "Node",
		Path:   "metadata.managedFields",
		Action: fieldStripped,
		Why:    "Server-side-apply bookkeeping. Nothing reads it.",
	},
	{
		Kind:   "Node",
		Path:   "metadata.annotations",
		Action: fieldStripped,
		Why: "Filtered, not dropped: reduced to retainedNodeAnnotations. Node " +
			"annotations are unbounded and carry large cloud-provider blobs. Any " +
			"annotation a source depends on must be added to that set — see the " +
			"metadata.annotations[ccc_priority_index] entry below.",
	},
	{
		Kind:   "Node",
		Path:   "status.images",
		Action: fieldStripped,
		Why:    "The single largest node-side line item at 10-40 KiB per node — roughly 2 GiB at 50k nodes — and nothing in the repo reads it.",
	},
	{
		Kind:   "Node",
		Path:   "status.volumesInUse",
		Action: fieldStripped,
		Why:    "Attach/detach bookkeeping, unbounded in cluster size, not read.",
	},
	{
		Kind:   "Node",
		Path:   "status.volumesAttached",
		Action: fieldStripped,
		Why:    "Attach/detach bookkeeping, unbounded in cluster size, not read.",
	},
	{
		Kind:   "Node",
		Path:   "status.nodeInfo",
		Action: fieldStripped,
		Why:    "Kernel, OS, container-runtime and kubelet version strings. Not read on the watch path.",
	},
	{
		Kind:   "Node",
		Path:   "status.conditions",
		Action: fieldStripped,
		Why:    "Filtered, not dropped: reduced to Ready. Other node conditions (e.g. MemoryPressure, DiskPressure, PIDPressure, NetworkUnavailable) are not read by any leeway source.",
	},

	// -------------------------------------------------------------- Node: preserved
	{
		Kind:   "Node",
		Path:   "metadata.annotations[ccc_priority_index]",
		Action: fieldPreserved,
		Why: "GKE records the provisioned compute-class priority here. It is " +
			"unprefixed, undocumented and GKE-internal, so it looks exactly like " +
			"the provider noise the annotation filter exists to drop — dropping it " +
			"would silently disable preference-rank tracking with no error anywhere.",
		Readers: []string{
			"pkg/sources/computeclass — rank resolution, docs/leeway-design.md §7.7.2",
		},
	},
	{
		Kind:   "Node",
		Path:   "status.conditions[Ready]",
		Action: fieldPreserved,
		Why:    "Node readiness gates whether a domain counts as available.",
		Readers: []string{
			"pkg/sources/topologydrift — domain inventory, docs/leeway-design.md §6.1",
		},
	},
	{
		Kind:   "Node",
		Path:   "status.allocatable",
		Action: fieldPreserved,
		Why:    "Capacity weighting for node-group subjects.",
		Readers: []string{
			"pkg/sources/topologydrift — capacity weighting, docs/leeway-design.md §6.1",
		},
	},
	{
		Kind:   "Node",
		Path:   "spec.taints",
		Action: fieldPreserved,
		Why:    "Eligibility: a node whose taints a pod does not tolerate is not an eligible domain, and GKE auto-injects compute-class and GPU taints that make this non-obvious.",
		Readers: []string{
			"pkg/sources/topologydrift — eligible-domain computation, docs/leeway-design.md §7.1",
		},
	},
}

func init() {
	for _, w := range []struct {
		kind   string
		prefix string
	}{
		{"Deployment", "spec.template"},
		{"ReplicaSet", "spec.template"},
		{"StatefulSet", "spec.template"},
		{"DaemonSet", "spec.template"},
		{"Job", "spec.template"},
		{"CronJob", "spec.jobTemplate.spec.template"},
	} {
		sharedTransformRegistry = append(sharedTransformRegistry, workloadRegistryEntries(w.kind, w.prefix)...)
	}
}

func workloadRegistryEntries(kind, prefix string) []fieldEntry {
	return []fieldEntry{
		{
			Kind:   kind,
			Path:   "metadata.managedFields",
			Action: fieldStripped,
			Why:    "Server-side-apply bookkeeping. Large, high-churn, and nothing in the repo reads it.",
		},
		{
			Kind:   kind,
			Path:   "metadata.annotations",
			Action: fieldStripped,
			Why:    "Filtered to drop full-manifest applied-configuration annotations while preserving rollout revision and operational annotations.",
		},
		{
			Kind:   kind,
			Path:   prefix + ".metadata.annotations",
			Action: fieldStripped,
			Why:    "Filtered to drop full-manifest applied-configuration annotations on the pod template.",
		},
		{
			Kind:   kind,
			Path:   prefix + ".spec.containers",
			Action: fieldStripped,
			Why:    "Container specs on workload templates carry embedded secrets and are not read by any leeway source.",
		},
		{
			Kind:   kind,
			Path:   prefix + ".spec.initContainers",
			Action: fieldStripped,
			Why:    "InitContainer specs on workload templates carry embedded secrets and are not read by any leeway source.",
		},
		{
			Kind:   kind,
			Path:   prefix + ".spec.ephemeralContainers",
			Action: fieldStripped,
			Why:    "EphemeralContainer specs on workload templates are not read by any leeway source.",
		},
	}
}
