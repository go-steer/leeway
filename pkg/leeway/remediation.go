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

package leeway

import (
	"fmt"
	"sort"
	"strings"
)

// RemediationKind classifies the deterministic remediation action for a Finding.
type RemediationKind string

const (
	// RemediationManifestPatch indicates that the workload is missing an
	// explicit topologySpreadConstraint (or matchLabelKeys on Deployment
	// rollouts) and can be fixed via a declarative PodSpec YAML patch
	// (e.g., in a GitOps PR).
	RemediationManifestPatch RemediationKind = "manifest_patch"

	// RemediationRolloutRestart indicates that the workload already declares
	// a spread constraint and all eligible domains are healthy, so a rolling
	// restart or Descheduler pass will rebalance existing replicas without
	// any YAML change.
	RemediationRolloutRestart RemediationKind = "rollout_restart"

	// RemediationClusterCapacity indicates that the deficit domain(s) lack
	// allocatable node capacity or lost nodes to consolidation; operators
	// should scale node pools or right-size requests rather than editing
	// workload spread constraints.
	RemediationClusterCapacity RemediationKind = "cluster_capacity"

	// RemediationClusterNodes indicates that the deficit domain(s) have
	// NotReady or tainted/cordoned nodes; operators should restore or
	// uncordon nodes (or add tolerations if the taint is permanent).
	RemediationClusterNodes RemediationKind = "cluster_nodes"

	// RemediationStorageTopology indicates that replicas are pinned to
	// specific domains by zonal PersistentVolumeClaims; adding strict
	// DoNotSchedule spread constraints without re-provisioning volumes will
	// leave pods Pending.
	RemediationStorageTopology RemediationKind = "storage_topology"
)

// FindingRemediation is the optional deterministic remediation block on a
// Finding. It separates declarative manifest patches (consumable by a
// downstream GitOps step) from operational/cluster actions where patching
// workload YAML would be ineffective or harmful.
type FindingRemediation struct {
	Kind    RemediationKind `json:"kind"`
	Summary string          `json:"summary"`
	Command string          `json:"command,omitempty"`
	YAML    string          `json:"yaml,omitempty"`
}

// SuggestRemediation derives a deterministic remediation recommendation from a
// Finding's SuspectedCause, Intent, and Subject.
//
// selectorLabels, when non-empty, supplies the workload's pod selector labels
// (e.g., `app: checkout`) for rendering `labelSelector.matchLabels` in a
// `manifest_patch` YAML snippet. When nil or empty, SuggestRemediation falls
// back to `app: <subject.name>`.
func SuggestRemediation(f Finding, selectorLabels map[string]string) *FindingRemediation {
	// 1. Domain-level findings (leeway.domain_unavailable).
	if f.Kind == KindDomainUnavailable || f.Subject.Kind == SubjectDomain {
		domain := f.Subject.Name
		if domain == "" && len(f.Domains) > 0 {
			domain = string(f.Domains[0].Domain)
		}
		switch f.SuspectedCause {
		case CauseConsolidation:
			return &FindingRemediation{
				Kind:    RemediationClusterCapacity,
				Summary: fmt.Sprintf("Domain %s has no remaining nodes on %s; scale up or restore node pools in %s.", domain, f.TopologyKey, domain),
			}
		case CauseTaintExclusion:
			return &FindingRemediation{
				Kind:    RemediationClusterNodes,
				Summary: fmt.Sprintf("All Ready nodes in %s on %s are cordoned or tainted; uncordon or remove blocking taints in %s.", domain, f.TopologyKey, domain),
			}
		default:
			return &FindingRemediation{
				Kind:    RemediationClusterNodes,
				Summary: fmt.Sprintf("No Ready nodes remain in %s on %s; investigate node health or zone outage in %s.", domain, f.TopologyKey, domain),
			}
		}
	}

	// 2. Non-workload subjects (NodeGroup, PreferenceAxis).
	switch f.Subject.Kind {
	case SubjectNodeGroup:
		return &FindingRemediation{
			Kind:    RemediationClusterCapacity,
			Summary: fmt.Sprintf("Node group %s is unevenly distributed across %s; rebalance node pool zones or autoscaler limits.", f.Subject.Name, f.TopologyKey),
		}
	case SubjectPreferenceAxis, "":
		return nil
	}

	// 3. Infrastructure and storage causes take precedence over workload edits:
	// never recommend tightening or patching workload YAML when the cluster or
	// persistent storage cannot satisfy the placement.
	deficits := deficitDomainsOrAxis(f)
	switch f.SuspectedCause {
	case CauseVolumePinning:
		return &FindingRemediation{
			Kind: RemediationStorageTopology,
			Summary: fmt.Sprintf(
				"Replicas of %s are pinned to specific %s domains by PersistentVolume node affinity; do not add DoNotSchedule spread constraints unless volumes are migrated or re-provisioned across %s.",
				f.Subject, f.TopologyKey, deficits,
			),
		}
	case CauseCapacityShortfall:
		return &FindingRemediation{
			Kind: RemediationClusterCapacity,
			Summary: fmt.Sprintf(
				"Deficit domain(s) (%s) lack allocatable capacity to schedule replicas of %s; scale node pools in %s or right-size pod resource requests rather than editing workload spread constraints.",
				deficits, f.Subject, deficits,
			),
		}
	case CauseConsolidation:
		return &FindingRemediation{
			Kind: RemediationClusterCapacity,
			Summary: fmt.Sprintf(
				"Deficit domain(s) (%s) lost nodes to consolidation; scale node pools or adjust autoscaler min-per-zone limits in %s before rebalancing %s.",
				deficits, deficits, f.Subject,
			),
		}
	case CauseDomainOutage:
		return &FindingRemediation{
			Kind: RemediationClusterNodes,
			Summary: fmt.Sprintf(
				"Deficit domain(s) (%s) lost Ready nodes; restore node health in %s before rebalancing %s.",
				deficits, deficits, f.Subject,
			),
		}
	case CauseTaintExclusion:
		return &FindingRemediation{
			Kind: RemediationClusterNodes,
			Summary: fmt.Sprintf(
				"Nodes in deficit domain(s) (%s) were cordoned or carry untolerated taints; uncordon nodes in %s (or add a matching toleration if the taint is permanent) before rebalancing %s.",
				deficits, deficits, f.Subject,
			),
		}
	}

	// 4. Workload / scheduler causes (constraint_ignored, rollout_bias, unknown).
	//
	// The conflicting-affinity case is a two-step fix and only the second step
	// can be rendered: the evidence line carries the topology key and the
	// domain count, not the selector's values, so the affinity removal cannot
	// be expressed as a patch. The summary and the YAML both say so, because a
	// consumer that applies the YAML alone changes nothing.
	if f.Intent != nil && evidenceHasConflictingNodeAffinity(f.Intent.Evidence) {
		return &FindingRemediation{
			Kind: RemediationManifestPatch,
			Summary: fmt.Sprintf(
				"Remove, or widen to more than one domain, the conflicting nodeAffinity/nodeSelector on %s from %s so its topologySpreadConstraint can spread replicas across eligible domains; the yaml only restates the spread constraint and does not remove the affinity, so applying it alone changes nothing.",
				f.TopologyKey, f.Subject,
			),
			Command: rolloutRestartCommand(f.Subject),
			YAML: conflictingAffinityPreamble(f.TopologyKey) + renderTopologySpreadPatch(
				f.Subject,
				f.TopologyKey,
				maxSkewOrOne(f.Intent),
				whenUnsatisfiableFromEvidence(f.Intent),
				selectorLabels,
			),
		}
	}
	if needsExplicitSpreadConstraint(f.Intent) {
		return &FindingRemediation{
			Kind: RemediationManifestPatch,
			Summary: fmt.Sprintf(
				"Add a topologySpreadConstraint on %s to %s so kube-scheduler balances replicas across eligible domains.",
				f.TopologyKey, f.Subject,
			),
			Command: rolloutRestartCommand(f.Subject),
			YAML:    renderTopologySpreadPatch(f.Subject, f.TopologyKey, 1, "ScheduleAnyway", selectorLabels),
		}
	}

	// If a Deployment already has a topologySpreadConstraint on this axis, but
	// rollout_bias occurred and matchLabelKeys is not configured, recommend
	// adding `matchLabelKeys: [pod-template-hash]` so rolling updates count only
	// the new revision's pods.
	if f.SuspectedCause == CauseRolloutBias &&
		f.Subject.Kind == SubjectDeployment &&
		f.Intent != nil &&
		f.Intent.Source == SourceTopologySpreadConstraint.String() &&
		!evidenceHasMatchLabelKeys(f.Intent.Evidence) {
		return &FindingRemediation{
			Kind: RemediationManifestPatch,
			Summary: fmt.Sprintf(
				"Add matchLabelKeys: [pod-template-hash] to the %s topologySpreadConstraint on %s so rolling updates score only the active revision's pods, then trigger a rollout restart to rebalance.",
				f.TopologyKey, f.Subject,
			),
			Command: rolloutRestartCommand(f.Subject),
			YAML: renderTopologySpreadPatch(
				f.Subject,
				f.TopologyKey,
				maxSkewOrOne(f.Intent),
				whenUnsatisfiableFromEvidence(f.Intent),
				selectorLabels,
			),
		}
	}

	// Otherwise, the spread intent is already declared and all eligible domains
	// are healthy: a rollout restart (or Descheduler pass) rebalances replicas.
	cmd := rolloutRestartCommand(f.Subject)
	summary := fmt.Sprintf(
		"Spread intent on %s is already configured for %s and all eligible domains are healthy; trigger a rolling restart (or run Descheduler RemovePodsViolatingTopologySpreadConstraint) to rebalance existing replicas.",
		f.TopologyKey, f.Subject,
	)
	if cmd == "" {
		summary = fmt.Sprintf(
			"Spread intent on %s is already configured for %s and all eligible domains are healthy; recreate or reschedule skewed pods to rebalance across domains.",
			f.TopologyKey, f.Subject,
		)
	}
	return &FindingRemediation{
		Kind:    RemediationRolloutRestart,
		Summary: summary,
		Command: cmd,
	}
}

func needsExplicitSpreadConstraint(in *FindingIntent) bool {
	if in == nil {
		return true
	}
	switch in.Source {
	case SourceClusterDefaultAssumed.String(),
		SourceLearnedBaseline.String(),
		SourcePodAntiAffinityRequired.String(),
		SourcePodAntiAffinityPreferred.String(),
		SourcePodAffinityRequired.String(),
		SourcePodAffinityPreferred.String():
		return true
	default:
		return false
	}
}

func evidenceHasMatchLabelKeys(evidence []string) bool {
	for _, e := range evidence {
		if strings.Contains(e, "matchLabelKeys=") {
			return true
		}
	}
	return false
}

func evidenceHasConflictingNodeAffinity(evidence []string) bool {
	for _, e := range evidence {
		if strings.Contains(e, "conflicting nodeAffinity/nodeSelector") {
			return true
		}
	}
	return false
}

// conflictingAffinityPreamble is the YAML comment that precedes the spread
// constraint patch in the conflicting-affinity remediation. It is a comment
// rather than a patch because the removal cannot be rendered: the finding
// knows which key the affinity narrows and to how many domains, not which
// nodeSelector entry or nodeSelectorTerm expresses it, and a strategic merge
// patch cannot delete one matchExpression without restating the whole
// affinity block. Prepending it keeps the YAML field a valid document that
// still says, to anyone who applies it, what has to happen first.
func conflictingAffinityPreamble(key TopologyKey) string {
	return fmt.Sprintf(
		"# Step 2 of 2. This patch only restates the topologySpreadConstraint on %[1]s.\n"+
			"# Step 1, which it cannot express, is to remove (or widen to more than one\n"+
			"# domain) the nodeAffinity/nodeSelector on %[1]s in this pod template;\n"+
			"# applying this patch on its own changes nothing.\n",
		key,
	)
}

func maxSkewOrOne(in *FindingIntent) int32 {
	if in != nil && in.MaxSkew != nil && *in.MaxSkew > 0 {
		return *in.MaxSkew
	}
	return 1
}

func whenUnsatisfiableFromEvidence(in *FindingIntent) string {
	if in != nil {
		for _, e := range in.Evidence {
			if strings.Contains(e, "whenUnsatisfiable=DoNotSchedule") {
				return "DoNotSchedule"
			}
		}
	}
	return "ScheduleAnyway"
}

func deficitDomainsOrAxis(f Finding) string {
	var names []string
	for _, d := range f.Domains {
		if d.Delta < 0 {
			names = append(names, string(d.Domain))
		}
	}
	if len(names) == 0 {
		for _, d := range f.Domains {
			if d.ReadyNodes == 0 && d.Domain != "" {
				names = append(names, string(d.Domain))
			}
		}
	}
	if len(names) == 0 {
		return string(f.TopologyKey)
	}
	return strings.Join(names, ", ")
}

func rolloutRestartCommand(sub SubjectRef) string {
	var kind string
	switch sub.Kind {
	case SubjectDeployment:
		kind = "deployment"
	case SubjectStatefulSet:
		kind = "statefulset"
	case SubjectDaemonSet:
		kind = "daemonset"
	default:
		return ""
	}
	if sub.Namespace != "" {
		return fmt.Sprintf("kubectl -n %s rollout restart %s/%s", sub.Namespace, kind, sub.Name)
	}
	return fmt.Sprintf("kubectl rollout restart %s/%s", kind, sub.Name)
}

func renderTopologySpreadPatch(sub SubjectRef, key TopologyKey, maxSkew int32, whenUnsatisfiable string, selectorLabels map[string]string) string {
	var b strings.Builder
	b.WriteString("spec:\n")
	b.WriteString("  template:\n")
	b.WriteString("    spec:\n")
	b.WriteString("      topologySpreadConstraints:\n")
	fmt.Fprintf(&b, "      - maxSkew: %d\n", maxSkew)
	fmt.Fprintf(&b, "        topologyKey: %s\n", key)
	fmt.Fprintf(&b, "        whenUnsatisfiable: %s\n", whenUnsatisfiable)
	b.WriteString("        labelSelector:\n")
	b.WriteString("          matchLabels:\n")

	if len(selectorLabels) == 0 {
		fmt.Fprintf(&b, "            app: %s\n", sub.Name)
	} else {
		keys := make([]string, 0, len(selectorLabels))
		for k := range selectorLabels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "            %s: %s\n", k, selectorLabels[k])
		}
	}

	if sub.Kind == SubjectDeployment {
		b.WriteString("        matchLabelKeys:\n")
		b.WriteString("        - pod-template-hash")
	} else {
		return strings.TrimSuffix(b.String(), "\n")
	}
	return b.String()
}
