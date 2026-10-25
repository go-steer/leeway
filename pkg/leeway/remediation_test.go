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
	"strings"
	"testing"
	"time"
)

func TestSuggestRemediation_ManifestPatchWhenMissingExplicitSpreadConstraint(t *testing.T) {
	t.Parallel()

	sub := SubjectRef{Kind: SubjectDeployment, Namespace: "payments", Name: "checkout"}
	f := Finding{
		Kind:           KindPlacementDrift,
		Subject:        sub,
		TopologyKey:    "topology.kubernetes.io/zone",
		Tier:           TierB.String(),
		Severity:       "warning",
		SuspectedCause: CauseConstraintIgnored,
		Intent: &FindingIntent{
			Source:     SourceClusterDefaultAssumed.String(),
			Confidence: ConfidenceAssumed.String(),
		},
		Domains: []FindingDomain{
			{Domain: "us-central1-a", Actual: 6, Expected: 2, Delta: 4, ReadyNodes: 3},
			{Domain: "us-central1-b", Actual: 0, Expected: 2, Delta: -2, ReadyNodes: 3},
			{Domain: "us-central1-c", Actual: 0, Expected: 2, Delta: -2, ReadyNodes: 3},
		},
	}

	got := SuggestRemediation(f, map[string]string{
		"app.kubernetes.io/name": "checkout",
		"tier":                   "backend",
	})
	if got == nil {
		t.Fatal("SuggestRemediation returned nil, want manifest_patch")
	}
	if got.Kind != RemediationManifestPatch {
		t.Errorf("Kind = %q, want %q", got.Kind, RemediationManifestPatch)
	}
	if got.Command != "kubectl -n payments rollout restart deployment/checkout" {
		t.Errorf("Command = %q, want rollout restart command", got.Command)
	}
	for _, want := range []string{
		"topologySpreadConstraints:",
		"maxSkew: 1",
		"topologyKey: topology.kubernetes.io/zone",
		"whenUnsatisfiable: ScheduleAnyway",
		"app.kubernetes.io/name: checkout",
		"tier: backend",
		"matchLabelKeys:",
		"- pod-template-hash",
	} {
		if !strings.Contains(got.YAML, want) {
			t.Errorf("YAML missing %q:\n%s", want, got.YAML)
		}
	}
}

func TestSuggestRemediation_RolloutRestartWhenSpreadConstraintAlreadyConfigured(t *testing.T) {
	t.Parallel()

	maxSkew := int32(1)
	f := Finding{
		Kind:           KindContractViolated,
		Subject:        SubjectRef{Kind: SubjectDeployment, Namespace: "prod", Name: "api"},
		TopologyKey:    "topology.kubernetes.io/zone",
		Tier:           TierA.String(),
		Severity:       "critical",
		SuspectedCause: CauseConstraintIgnored,
		Intent: &FindingIntent{
			Source:     SourceTopologySpreadConstraint.String(),
			Confidence: ConfidenceDeclared.String(),
			MaxSkew:    &maxSkew,
			Evidence: []string{
				"topologySpreadConstraints[0]: topologyKey=topology.kubernetes.io/zone maxSkew=1 whenUnsatisfiable=DoNotSchedule labelSelector=app=api; matchLabelKeys=pod-template-hash",
			},
		},
		Domains: []FindingDomain{
			{Domain: "us-central1-a", Actual: 5, Expected: 3, Delta: 2, ReadyNodes: 3},
			{Domain: "us-central1-b", Actual: 1, Expected: 3, Delta: -2, ReadyNodes: 3},
		},
	}

	got := SuggestRemediation(f, map[string]string{"app": "api"})
	if got == nil {
		t.Fatal("SuggestRemediation returned nil, want rollout_restart")
	}
	if got.Kind != RemediationRolloutRestart {
		t.Errorf("Kind = %q, want %q", got.Kind, RemediationRolloutRestart)
	}
	if got.YAML != "" {
		t.Errorf("YAML = %q, want empty when constraint is already configured", got.YAML)
	}
	if got.Command != "kubectl -n prod rollout restart deployment/api" {
		t.Errorf("Command = %q, want rollout restart command", got.Command)
	}
}

func TestSuggestRemediation_RolloutBiasWithoutMatchLabelKeysRecommendsManifestPatch(t *testing.T) {
	t.Parallel()

	maxSkew := int32(1)
	f := Finding{
		Kind:           KindContractViolated,
		Subject:        SubjectRef{Kind: SubjectDeployment, Namespace: "prod", Name: "api"},
		TopologyKey:    "topology.kubernetes.io/zone",
		Tier:           TierA.String(),
		Severity:       "critical",
		SuspectedCause: CauseRolloutBias,
		Intent: &FindingIntent{
			Source:     SourceTopologySpreadConstraint.String(),
			Confidence: ConfidenceDeclared.String(),
			MaxSkew:    &maxSkew,
			Evidence: []string{
				"topologySpreadConstraints[0]: topologyKey=topology.kubernetes.io/zone maxSkew=1 whenUnsatisfiable=DoNotSchedule labelSelector=app=api",
			},
		},
	}

	got := SuggestRemediation(f, map[string]string{"app": "api"})
	if got == nil {
		t.Fatal("SuggestRemediation returned nil, want manifest_patch")
	}
	if got.Kind != RemediationManifestPatch {
		t.Errorf("Kind = %q, want %q", got.Kind, RemediationManifestPatch)
	}
	if !strings.Contains(got.YAML, "whenUnsatisfiable: DoNotSchedule") || !strings.Contains(got.YAML, "matchLabelKeys:\n        - pod-template-hash") {
		t.Errorf("YAML did not preserve DoNotSchedule and add matchLabelKeys:\n%s", got.YAML)
	}
}

func TestSuggestRemediation_ConflictingNodeAffinityRecommendsManifestPatch(t *testing.T) {
	t.Parallel()

	maxSkew := int32(1)
	f := Finding{
		Kind:           KindPlacementDrift,
		Subject:        SubjectRef{Kind: SubjectDeployment, Namespace: "topology", Name: "affinity-pinned-api"},
		TopologyKey:    "topology.kubernetes.io/zone",
		Tier:           TierB.String(),
		Severity:       "warning",
		SuspectedCause: CauseConstraintIgnored,
		Intent: &FindingIntent{
			Source:     SourceTopologySpreadConstraint.String(),
			Confidence: ConfidenceDeclared.String(),
			MaxSkew:    &maxSkew,
			Evidence: []string{
				"topologySpreadConstraints[0]: topologyKey=topology.kubernetes.io/zone maxSkew=1 whenUnsatisfiable=ScheduleAnyway labelSelector=app=affinity-pinned-api",
				"conflicting nodeAffinity/nodeSelector on topology.kubernetes.io/zone restricts placement to 1 domain(s), contradicting the spread constraint on topology.kubernetes.io/zone",
			},
		},
	}

	got := SuggestRemediation(f, map[string]string{"app": "affinity-pinned-api"})
	if got == nil {
		t.Fatal("SuggestRemediation returned nil, want manifest_patch")
	}
	if got.Kind != RemediationManifestPatch {
		t.Errorf("Kind = %q, want %q", got.Kind, RemediationManifestPatch)
	}
	if !strings.Contains(got.Summary, "conflicting nodeAffinity/nodeSelector") {
		t.Errorf("Summary = %q, want mention of conflicting nodeAffinity/nodeSelector", got.Summary)
	}
	// The YAML cannot express the affinity removal, so the summary must say
	// that applying it alone is not the fix.
	if !strings.Contains(got.Summary, "applying it alone changes nothing") {
		t.Errorf("Summary = %q, want a statement that the yaml alone changes nothing", got.Summary)
	}
	if got.Command != "kubectl -n topology rollout restart deployment/affinity-pinned-api" {
		t.Errorf("Command = %q, want rollout restart command", got.Command)
	}

	// The YAML leads with a comment naming the step it cannot render, and is
	// still the spread-constraint patch underneath.
	if !strings.HasPrefix(got.YAML, "# Step 2 of 2.") {
		t.Errorf("YAML does not open with the two-step comment:\n%s", got.YAML)
	}
	for _, want := range []string{
		"remove (or widen to more than one",
		"nodeAffinity/nodeSelector on topology.kubernetes.io/zone",
		"applying this patch on its own changes nothing",
		"\nspec:\n  template:\n    spec:\n      topologySpreadConstraints:\n",
		"maxSkew: 1",
		"topologyKey: topology.kubernetes.io/zone",
		"whenUnsatisfiable: ScheduleAnyway",
		"app: affinity-pinned-api",
		"matchLabelKeys:\n        - pod-template-hash",
	} {
		if !strings.Contains(got.YAML, want) {
			t.Errorf("YAML missing %q:\n%s", want, got.YAML)
		}
	}
	// Every preamble line is a YAML comment, so the field stays a document a
	// consumer can parse rather than prose it has to strip first.
	body := strings.SplitN(got.YAML, "\nspec:\n", 2)
	if len(body) != 2 {
		t.Fatalf("YAML has no spec: block after the preamble:\n%s", got.YAML)
	}
	for _, line := range strings.Split(strings.TrimSpace(body[0]), "\n") {
		if !strings.HasPrefix(line, "# ") {
			t.Errorf("preamble line %q is not a YAML comment", line)
		}
	}
}

func TestSuggestRemediation_InfrastructureAndStorageCausesNeverPatchWorkloadYAML(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		cause     SuspectedCause
		wantKind  RemediationKind
		wantWords string
	}{
		{
			name:      "volume pinning",
			cause:     CauseVolumePinning,
			wantKind:  RemediationStorageTopology,
			wantWords: "PersistentVolume",
		},
		{
			name:      "capacity shortfall",
			cause:     CauseCapacityShortfall,
			wantKind:  RemediationClusterCapacity,
			wantWords: "allocatable capacity",
		},
		{
			name:      "consolidation",
			cause:     CauseConsolidation,
			wantKind:  RemediationClusterCapacity,
			wantWords: "consolidation",
		},
		{
			name:      "domain outage",
			cause:     CauseDomainOutage,
			wantKind:  RemediationClusterNodes,
			wantWords: "Ready nodes",
		},
		{
			name:      "taint exclusion",
			cause:     CauseTaintExclusion,
			wantKind:  RemediationClusterNodes,
			wantWords: "cordoned or carry untolerated taints",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := Finding{
				Kind:           KindContractViolated,
				Subject:        SubjectRef{Kind: SubjectStatefulSet, Namespace: "prod", Name: "db"},
				TopologyKey:    "topology.kubernetes.io/zone",
				Tier:           TierA.String(),
				Severity:       "critical",
				SuspectedCause: tc.cause,
				Domains: []FindingDomain{
					{Domain: "us-central1-a", Actual: 4, Expected: 2, Delta: 2, ReadyNodes: 3},
					{Domain: "us-central1-b", Actual: 0, Expected: 2, Delta: -2, ReadyNodes: 0},
				},
			}
			got := SuggestRemediation(f, map[string]string{"app": "db"})
			if got == nil {
				t.Fatal("SuggestRemediation returned nil")
			}
			if got.Kind != tc.wantKind {
				t.Errorf("Kind = %q, want %q", got.Kind, tc.wantKind)
			}
			if got.YAML != "" {
				t.Errorf("YAML = %q, want empty for cause %q", got.YAML, tc.cause)
			}
			if !strings.Contains(got.Summary, tc.wantWords) || !strings.Contains(got.Summary, "us-central1-b") {
				t.Errorf("Summary = %q, want %q and deficit domain us-central1-b", got.Summary, tc.wantWords)
			}
		})
	}
}

func TestSuggestRemediation_DomainUnavailableFinding(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	a := DomainAvailability{Domain: "us-central1-c", Nodes: 3, Ready: 3, Usable: 0, Known: true}
	f := NewDomainFinding(a, "topology.kubernetes.io/zone", AlertState{FirstSeenAt: first})
	if f.Remediation == nil {
		t.Fatal("NewDomainFinding did not populate Remediation")
	}
	if f.Remediation.Kind != RemediationClusterNodes {
		t.Errorf("Remediation.Kind = %q, want %q", f.Remediation.Kind, RemediationClusterNodes)
	}
	if !strings.Contains(f.Remediation.Summary, "us-central1-c") {
		t.Errorf("Remediation.Summary = %q, want domain name", f.Remediation.Summary)
	}
}
