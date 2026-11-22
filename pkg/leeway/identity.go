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

import "strings"

// Normalization sentinels used by TopologyFindingKey and RankFindingKey so
// report consumers (shipmind's posture ledger and kube-agents' 7-day ledger)
// derive byte-identical deduplication keys across sweeps.
const (
	// ClusterWideNamespace occupies the namespace slot of a report finding key
	// when the subject is cluster-scoped (Domain, NodeGroup, ComputeClass, or
	// a cluster-scoped Custom subject).
	ClusterWideNamespace = "_cluster"

	defaultTopologySubjectKind  = "Cluster"
	defaultRankObjectKind       = "ComputeClass"
	defaultRankAxisFallback     = "compute-class"
	defaultIdentityUnknownValue = "unknown"
	rankRulePrefix              = "rule:"
)

// TopologyFindingKey returns the deterministic cross-sweep deduplication key
// for a topology-drift Finding on cluster:
//
//	<cluster>/<namespace>/<subject.kind>/<subject.name>/<topologyKey>/<kind>
//
// Cluster-scoped subjects (empty Subject.Namespace, such as Domain on
// leeway.domain_unavailable or NodeGroup) use "_cluster" in the namespace
// slot. SuspectedCause, Severity, Score, Domains, FirstSeenAt, and Remediation
// are deliberately excluded so cause transitions or severity escalations update
// an open ledger entry in place rather than opening a duplicate row.
func TopologyFindingKey(cluster string, f Finding) string {
	ns := strings.TrimSpace(f.Subject.Namespace)
	if ns == "" {
		ns = ClusterWideNamespace
	}
	sKind := strings.TrimSpace(string(f.Subject.Kind))
	if sKind == "" {
		sKind = defaultTopologySubjectKind
	}
	fKind := strings.TrimSpace(f.Kind)
	sName := strings.TrimSpace(f.Subject.Name)
	if sName == "" {
		if fKind != "" {
			sName = fKind
		} else {
			sName = defaultIdentityUnknownValue
		}
	}
	topoKey := strings.TrimSpace(string(f.TopologyKey))
	if topoKey == "" {
		topoKey = defaultIdentityUnknownValue
	}
	if fKind == "" {
		fKind = defaultIdentityUnknownValue
	}
	return strings.Join([]string{
		strings.TrimSpace(cluster),
		ns,
		sKind,
		sName,
		topoKey,
		fKind,
	}, "/")
}

// RankFindingKey returns the deterministic cross-sweep deduplication key for a
// compute-class RankFinding on cluster:
//
//	<cluster>/_cluster/<objectKind>/<subject.name>/<ruleKey>/<kind>
//
// For axis-wide rules (Focus == nil or non-tier sentinel Rank < 0: wedged,
// rank0-share, last-rank, no-migration, rank-baseline), ruleKey is
// "rule:<rule>". For per-tier rules (Focus != nil && Focus.IsTier(), i.e. >= 0,
// including tier 0: tier-unused), ruleKey is "rule:<rule>/<focus>" (matching
// RankVerdict.EpisodeKey) so two unused tiers on the same ComputeClass do not
// collide onto one ledger entry. When Rule is empty, ruleKey falls back to
// "compute-class". SpecHash, Observed, Ranks, Severity, Reason, and FirstSeenAt
// are deliberately excluded so unrelated ComputeClass spec edits or occupancy
// shifts update the open ledger entry in place.
func RankFindingKey(cluster string, f RankFinding) string {
	objKind := strings.TrimSpace(f.ObjectKind)
	if objKind == "" {
		objKind = defaultRankObjectKind
	}
	name := strings.TrimSpace(f.Subject.Name)
	if name == "" {
		name = defaultIdentityUnknownValue
	}
	rule := strings.TrimSpace(f.Rule)
	ruleKey := defaultRankAxisFallback
	if rule != "" {
		if f.Focus != nil && f.Focus.IsTier() {
			ruleKey = rankRulePrefix + rule + "/" + f.Focus.String()
		} else {
			ruleKey = rankRulePrefix + rule
		}
	}
	fKind := strings.TrimSpace(f.Kind)
	if fKind == "" {
		fKind = defaultIdentityUnknownValue
	}
	return strings.Join([]string{
		strings.TrimSpace(cluster),
		ClusterWideNamespace,
		objKind,
		name,
		ruleKey,
		fKind,
	}, "/")
}
