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
	"testing"
	"time"
)

func TestTopologyFindingKey_MatchesLedgerContract(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		cluster string
		finding Finding
		want    string
	}{
		{
			name:    "namespaced workload contract violation on GKE triple",
			cluster: "p1/us-central1/c1",
			finding: Finding{
				Kind:           KindContractViolated,
				Subject:        SubjectRef{Kind: SubjectDeployment, Namespace: "shop", Name: "batch-worker"},
				TopologyKey:    "topology.kubernetes.io/zone",
				Tier:           "A",
				Severity:       "critical",
				SuspectedCause: CauseCapacityShortfall,
			},
			want: "p1/us-central1/c1/shop/Deployment/batch-worker/topology.kubernetes.io/zone/leeway.contract_violated",
		},
		{
			name:    "cluster-scoped domain unavailable uses _cluster sentinel",
			cluster: "p1/us-east1/c1",
			finding: Finding{
				Kind:           KindDomainUnavailable,
				Subject:        SubjectRef{Kind: SubjectDomain, Name: "us-east1-c"},
				TopologyKey:    "topology.kubernetes.io/zone",
				Tier:           "B",
				Severity:       "warning",
				SuspectedCause: CauseTaintExclusion,
			},
			want: "p1/us-east1/c1/_cluster/Domain/us-east1-c/topology.kubernetes.io/zone/leeway.domain_unavailable",
		},
		{
			name:    "cluster-scoped node group uses _cluster sentinel",
			cluster: "gke-prod",
			finding: Finding{
				Kind:           KindPlacementDrift,
				Subject:        SubjectRef{Kind: SubjectNodeGroup, Name: "spot-pool"},
				TopologyKey:    "topology.kubernetes.io/zone",
				Tier:           "C",
				Severity:       "info",
				SuspectedCause: CauseUnknown,
			},
			want: "gke-prod/_cluster/NodeGroup/spot-pool/topology.kubernetes.io/zone/leeway.placement_drift",
		},
		{
			name:    "defensive fallbacks on zero-valued finding",
			cluster: "p1/us-central1/c1",
			finding: Finding{},
			want:    "p1/us-central1/c1/_cluster/Cluster/unknown/unknown/unknown",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := TopologyFindingKey(tc.cluster, tc.finding); got != tc.want {
				t.Errorf("TopologyFindingKey() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTopologyFindingKey_IgnoresMutableEpisodeFields(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	sweep1 := Finding{
		Kind:           KindPlacementDrift,
		Subject:        SubjectRef{Kind: SubjectStatefulSet, Namespace: "payments", Name: "ledger"},
		TopologyKey:    "topology.kubernetes.io/zone",
		Tier:           "B",
		Severity:       "info",
		Score:          FindingScore{Drift: 0.33, ObservedSkew: 2, MaxDomainShare: 0.67},
		SuspectedCause: CauseRolloutBias,
		FirstSeenAt:    t0,
		Remediation:    &FindingRemediation{Kind: RemediationRolloutRestart, Summary: "restart"},
	}
	sweep2 := sweep1
	sweep2.Severity = "warning"
	sweep2.SuspectedCause = CauseVolumePinning
	sweep2.Score = FindingScore{Drift: 0.67, ObservedSkew: 4, MaxDomainShare: 1.0}
	sweep2.FirstSeenAt = t0.Add(24 * time.Hour)
	sweep2.Remediation = &FindingRemediation{Kind: RemediationStorageTopology, Summary: "pinned"}

	if got1, got2 := TopologyFindingKey("prod", sweep1), TopologyFindingKey("prod", sweep2); got1 != got2 {
		t.Errorf("TopologyFindingKey drifted across sweeps: %q != %q", got1, got2)
	}
}

func TestRankFindingKey_MatchesLedgerContractAndSeparatesFocusedTiers(t *testing.T) {
	t.Parallel()

	tier0 := Rank(0)
	tier1 := Rank(1)
	tier2 := Rank(2)
	unknownSentinel := RankUnknown
	offAxisSentinel := RankOffAxis

	axisWide := RankFinding{
		Kind:       KindRankDegraded,
		Subject:    SubjectRef{Kind: SubjectPreferenceAxis, Name: "gold"},
		Provider:   string(ProviderGKEComputeClass),
		ObjectKind: "ComputeClass",
		SpecHash:   "842f2f5b7a0deb21",
		Tier:       "B",
		Severity:   "warning",
		Rule:       RankRuleLastRank.String(),
	}
	if got, want := RankFindingKey("p1/us-central1/c1", axisWide), "p1/us-central1/c1/_cluster/ComputeClass/gold/rule:last-rank/leeway.rank_degraded"; got != want {
		t.Errorf("RankFindingKey(axis-wide) = %q, want %q", got, want)
	}

	// Negative sentinel Focus values (e.g. RankUnknown = -1, RankOffAxis = -3)
	// are non-tiers and must stay axis-wide, matching RankVerdict.EpisodeKey().
	for _, sentinel := range []*Rank{&unknownSentinel, &offAxisSentinel} {
		withSentinel := axisWide
		withSentinel.Focus = sentinel
		if got, want := RankFindingKey("p1/us-central1/c1", withSentinel), "p1/us-central1/c1/_cluster/ComputeClass/gold/rule:last-rank/leeway.rank_degraded"; got != want {
			t.Errorf("RankFindingKey(Focus=%d) = %q, want %q", *sentinel, got, want)
		}
	}

	// SpecHash and FirstSeenAt edits must not fork a new ledger key.
	editedSpec := axisWide
	editedSpec.SpecHash = "ffffffffffffffff"
	editedSpec.FirstSeenAt = time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	if got1, got2 := RankFindingKey("p1/us-central1/c1", axisWide), RankFindingKey("p1/us-central1/c1", editedSpec); got1 != got2 {
		t.Errorf("RankFindingKey changed across SpecHash/FirstSeenAt update: %q != %q", got1, got2)
	}

	// Tier 0 is a valid tier (not falsy!) and multiple unused tiers on the
	// same ComputeClass must NOT collide onto one ledger key.
	unused0 := RankFinding{
		Kind:       KindRankTierUnused,
		Subject:    SubjectRef{Kind: SubjectPreferenceAxis, Name: "gold"},
		ObjectKind: "ComputeClass",
		Rule:       RankRuleTierUnused.String(),
		Focus:      &tier0,
	}
	unused1 := unused0
	unused1.Focus = &tier1
	unused2 := unused0
	unused2.Focus = &tier2

	key0 := RankFindingKey("p1/us-central1/c1", unused0)
	key1 := RankFindingKey("p1/us-central1/c1", unused1)
	key2 := RankFindingKey("p1/us-central1/c1", unused2)
	if want0 := "p1/us-central1/c1/_cluster/ComputeClass/gold/rule:tier-unused/0/leeway.rank_tier_unused"; key0 != want0 {
		t.Errorf("RankFindingKey(focus=0) = %q, want %q", key0, want0)
	}
	if want1 := "p1/us-central1/c1/_cluster/ComputeClass/gold/rule:tier-unused/1/leeway.rank_tier_unused"; key1 != want1 {
		t.Errorf("RankFindingKey(focus=1) = %q, want %q", key1, want1)
	}
	if want2 := "p1/us-central1/c1/_cluster/ComputeClass/gold/rule:tier-unused/2/leeway.rank_tier_unused"; key2 != want2 {
		t.Errorf("RankFindingKey(focus=2) = %q, want %q", key2, want2)
	}
	if key0 == key1 || key1 == key2 || key0 == key2 {
		t.Errorf("distinct unused tiers collided: %q / %q / %q", key0, key1, key2)
	}

	// Defensive fallback when fields are missing.
	if got, want := RankFindingKey("p1/us-central1/c1", RankFinding{}), "p1/us-central1/c1/_cluster/ComputeClass/unknown/compute-class/unknown"; got != want {
		t.Errorf("RankFindingKey(zero) = %q, want %q", got, want)
	}
}
