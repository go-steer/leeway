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

package report_test

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-steer/leeway/pkg/leeway"
	"github.com/go-steer/leeway/pkg/report"
	"github.com/go-steer/leeway/pkg/sources/computeclass"
)

type jsonFieldSpec struct {
	kind      string // "string", "int", "float", "bool", "slice", "map", "struct"
	omitempty bool
	pointer   bool
}

var timeType = reflect.TypeFor[time.Time]()

func classifyJSONFieldType(t reflect.Type) string {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == timeType {
		return "string"
	}
	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "int"
	case reflect.Float32, reflect.Float64:
		return "float"
	case reflect.Bool:
		return "bool"
	case reflect.Slice:
		return "slice"
	case reflect.Map:
		return "map"
	case reflect.Struct:
		return "struct"
	default:
		return t.Kind().String()
	}
}

func inspectJSONFields(t *testing.T, typ reflect.Type) map[string]jsonFieldSpec {
	t.Helper()
	got := make(map[string]jsonFieldSpec, typ.NumField())
	for i := range typ.NumField() {
		sf := typ.Field(i)
		if !sf.IsExported() {
			continue
		}
		tag := sf.Tag.Get("json")
		if tag == "-" {
			continue
		}
		if tag == "" {
			t.Fatalf("%s.%s exported without explicit json tag", typ, sf.Name)
		}
		parts := strings.Split(tag, ",")
		name := parts[0]
		if name == "" {
			t.Fatalf("%s.%s has empty json field name in tag %q", typ, sf.Name, tag)
		}
		got[name] = jsonFieldSpec{
			kind:      classifyJSONFieldType(sf.Type),
			omitempty: slices.Contains(parts[1:], "omitempty"),
			pointer:   sf.Type.Kind() == reflect.Pointer,
		}
	}
	return got
}

// TestReportSchemaV1Alpha1_StructJSONFieldsFrozen enforces docs/stability-policy.md §1:
//   - SchemaVersion remains "leeway.report/v1alpha1" (StatusOK="ok", StatusError="error").
//   - Every promised snake_case envelope field and camelCase Finding / RankFinding /
//     Remediation / ClassSummary field keeps its exact JSON key, JSON type class, and
//     omitempty posture.
//   - Any new field added within v1alpha1 MUST carry omitempty so existing readers and
//     round-trip mergers remain forward-compatible; Update docs/stability-policy.md and
//     this table in the same PR when adding an optional field.
func TestReportSchemaV1Alpha1_StructJSONFieldsFrozen(t *testing.T) {
	t.Parallel()

	if report.SchemaVersion != "leeway.report/v1alpha1" {
		t.Fatalf("report.SchemaVersion = %q, want %q (promoting default schema requires dual-version window per docs/stability-policy.md)",
			report.SchemaVersion, "leeway.report/v1alpha1")
	}
	if report.StatusOK != "ok" || report.StatusError != "error" {
		t.Fatalf("ClusterReport status constants = (%q, %q), want (\"ok\", \"error\")", report.StatusOK, report.StatusError)
	}

	pinned := []struct {
		name   string
		typ    reflect.Type
		fields map[string]jsonFieldSpec
	}{
		{
			name: "report.Report",
			typ:  reflect.TypeFor[report.Report](),
			fields: map[string]jsonFieldSpec{
				"schema_version": {kind: "string", omitempty: false},
				"generated_at":   {kind: "string", omitempty: false},
				"clusters":       {kind: "slice", omitempty: false},
				"summary":        {kind: "struct", omitempty: false},
			},
		},
		{
			name: "report.Summary",
			typ:  reflect.TypeFor[report.Summary](),
			fields: map[string]jsonFieldSpec{
				"clusters_total":     {kind: "int", omitempty: false},
				"clusters_succeeded": {kind: "int", omitempty: false},
				"clusters_failed":    {kind: "int", omitempty: false},
				"topology_findings":  {kind: "int", omitempty: false},
				"rank_findings":      {kind: "int", omitempty: false},
				"by_severity":        {kind: "map", omitempty: true},
				"by_tier":            {kind: "map", omitempty: true},
			},
		},
		{
			name: "report.ClusterReport",
			typ:  reflect.TypeFor[report.ClusterReport](),
			fields: map[string]jsonFieldSpec{
				"cluster":           {kind: "string", omitempty: false},
				"source":            {kind: "string", omitempty: true},
				"status":            {kind: "string", omitempty: false},
				"error":             {kind: "string", omitempty: true},
				"domains":           {kind: "map", omitempty: true},
				"topology_findings": {kind: "slice", omitempty: true},
				"compute_classes":   {kind: "slice", omitempty: true},
				"rank_findings":     {kind: "slice", omitempty: true},
				"subjects":          {kind: "slice", omitempty: true},
			},
		},
		{
			name: "report.DomainSummary",
			typ:  reflect.TypeFor[report.DomainSummary](),
			fields: map[string]jsonFieldSpec{
				"nodes":                 {kind: "int", omitempty: false},
				"ready":                 {kind: "int", omitempty: false},
				"usable":                {kind: "int", omitempty: false},
				"allocatable_cpu_milli": {kind: "int", omitempty: true},
				"allocatable_mem_bytes": {kind: "int", omitempty: true},
			},
		},
		{
			name: "computeclass.ClassSummary",
			typ:  reflect.TypeFor[computeclass.ClassSummary](),
			fields: map[string]jsonFieldSpec{
				"name":                {kind: "string", omitempty: false},
				"scorable":            {kind: "bool", omitempty: false},
				"ordering":            {kind: "string", omitempty: true},
				"podsByRank":          {kind: "map", omitempty: true},
				"pendingPods":         {kind: "int", omitempty: false},
				"pendingProvisioning": {kind: "int", omitempty: false},
				"pendingDeclined":     {kind: "int", omitempty: false},
			},
		},
		{
			name: "leeway.Finding",
			typ:  reflect.TypeFor[leeway.Finding](),
			fields: map[string]jsonFieldSpec{
				"kind":                {kind: "string", omitempty: false},
				"subject":             {kind: "struct", omitempty: false},
				"topologyKey":         {kind: "string", omitempty: false},
				"tier":                {kind: "string", omitempty: false},
				"severity":            {kind: "string", omitempty: false},
				"score":               {kind: "struct", omitempty: false},
				"intent":              {kind: "struct", omitempty: true, pointer: true},
				"domains":             {kind: "slice", omitempty: false},
				"suspectedCause":      {kind: "string", omitempty: false},
				"contributingFactors": {kind: "slice", omitempty: true},
				"untestedCauses":      {kind: "slice", omitempty: true},
				"firstSeenAt":         {kind: "string", omitempty: false},
				"transient":           {kind: "string", omitempty: true},
				"relaxed":             {kind: "bool", omitempty: true},
				"severityFromPolicy":  {kind: "bool", omitempty: true},
				"remediation":         {kind: "struct", omitempty: true, pointer: true},
			},
		},
		{
			name: "leeway.SubjectRef",
			typ:  reflect.TypeFor[leeway.SubjectRef](),
			fields: map[string]jsonFieldSpec{
				"kind":      {kind: "string", omitempty: false},
				"namespace": {kind: "string", omitempty: true},
				"name":      {kind: "string", omitempty: false},
			},
		},
		{
			name: "leeway.FindingScore",
			typ:  reflect.TypeFor[leeway.FindingScore](),
			fields: map[string]jsonFieldSpec{
				"drift":              {kind: "float", omitempty: false},
				"observedSkew":       {kind: "int", omitempty: false},
				"minAchievableSkew":  {kind: "int", omitempty: false},
				"excessSkew":         {kind: "int", omitempty: false},
				"relocationDistance": {kind: "int", omitempty: false},
				"maxDomainShare":     {kind: "float", omitempty: false},
			},
		},
		{
			name: "leeway.FindingIntent",
			typ:  reflect.TypeFor[leeway.FindingIntent](),
			fields: map[string]jsonFieldSpec{
				"source":     {kind: "string", omitempty: false},
				"confidence": {kind: "string", omitempty: false},
				"weighting":  {kind: "string", omitempty: false},
				"maxSkew":    {kind: "int", omitempty: true, pointer: true},
				"evidence":   {kind: "slice", omitempty: true},
			},
		},
		{
			name: "leeway.FindingDomain",
			typ:  reflect.TypeFor[leeway.FindingDomain](),
			fields: map[string]jsonFieldSpec{
				"domain":     {kind: "string", omitempty: false},
				"actual":     {kind: "int", omitempty: false},
				"expected":   {kind: "int", omitempty: false},
				"delta":      {kind: "int", omitempty: false},
				"readyNodes": {kind: "int", omitempty: false},
				"note":       {kind: "string", omitempty: true},
			},
		},
		{
			name: "leeway.FindingRemediation",
			typ:  reflect.TypeFor[leeway.FindingRemediation](),
			fields: map[string]jsonFieldSpec{
				"kind":    {kind: "string", omitempty: false},
				"summary": {kind: "string", omitempty: false},
				"command": {kind: "string", omitempty: true},
				"yaml":    {kind: "string", omitempty: true},
			},
		},
		{
			name: "leeway.RankFinding",
			typ:  reflect.TypeFor[leeway.RankFinding](),
			fields: map[string]jsonFieldSpec{
				"kind":                 {kind: "string", omitempty: false},
				"subject":              {kind: "struct", omitempty: false},
				"provider":             {kind: "string", omitempty: false},
				"objectKind":           {kind: "string", omitempty: false},
				"specHash":             {kind: "string", omitempty: false},
				"tier":                 {kind: "string", omitempty: false},
				"severity":             {kind: "string", omitempty: false},
				"rule":                 {kind: "string", omitempty: false},
				"focus":                {kind: "int", omitempty: true, pointer: true},
				"observed":             {kind: "struct", omitempty: false},
				"ranks":                {kind: "slice", omitempty: false},
				"ordering":             {kind: "string", omitempty: false},
				"tiers":                {kind: "int", omitempty: false},
				"scaleUp":              {kind: "string", omitempty: false},
				"optimizeRulePriority": {kind: "bool", omitempty: false},
				"reason":               {kind: "string", omitempty: false},
				"firstSeenAt":          {kind: "string", omitempty: false},
			},
		},
		{
			name: "leeway.RankObservation",
			typ:  reflect.TypeFor[leeway.RankObservation](),
			fields: map[string]jsonFieldSpec{
				"windowSeconds":       {kind: "float", omitempty: false},
				"tierSeconds":         {kind: "float", omitempty: false},
				"rank0Share":          {kind: "float", omitempty: false},
				"lastRank":            {kind: "int", omitempty: false},
				"lastRankShare":       {kind: "float", omitempty: false},
				"meanRank":            {kind: "float", omitempty: false},
				"pendingPods":         {kind: "int", omitempty: false},
				"pendingProvisioning": {kind: "int", omitempty: false},
				"pendingDeclined":     {kind: "int", omitempty: false},
				"degradedPods":        {kind: "int", omitempty: false},
				"improving":           {kind: "int", omitempty: false},
				"worsening":           {kind: "int", omitempty: false},
				"lateral":             {kind: "int", omitempty: false},
				"baseline":            {kind: "struct", omitempty: true, pointer: true},
			},
		},
		{
			name: "leeway.RankBaseline",
			typ:  reflect.TypeFor[leeway.RankBaseline](),
			fields: map[string]jsonFieldSpec{
				"mean":      {kind: "float", omitempty: false},
				"band":      {kind: "float", omitempty: false},
				"samples":   {kind: "int", omitempty: false},
				"firstSeen": {kind: "string", omitempty: false},
			},
		},
		{
			name: "leeway.RankShare",
			typ:  reflect.TypeFor[leeway.RankShare](),
			fields: map[string]jsonFieldSpec{
				"rank":       {kind: "int", omitempty: false},
				"rules":      {kind: "string", omitempty: true},
				"podSeconds": {kind: "float", omitempty: false},
				"share":      {kind: "float", omitempty: false},
				"pods":       {kind: "int", omitempty: false},
			},
		},
	}

	for _, tc := range pinned {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := inspectJSONFields(t, tc.typ)
			for wantKey, wantSpec := range tc.fields {
				gotSpec, ok := got[wantKey]
				if !ok {
					t.Errorf("%s: stable JSON field %q removed or renamed (violates docs/stability-policy.md §1)", tc.name, wantKey)
					continue
				}
				if gotSpec != wantSpec {
					t.Errorf("%s.%s = %+v, want %+v", tc.name, wantKey, gotSpec, wantSpec)
				}
			}
			for gotKey, gotSpec := range got {
				if _, known := tc.fields[gotKey]; !known {
					if !gotSpec.omitempty {
						t.Errorf("%s: new JSON field %q lacks omitempty; additions within leeway.report/v1alpha1 must be optional", tc.name, gotKey)
					} else {
						t.Errorf("%s: unrecorded additive JSON field %q — document it in docs/stability-policy.md and pin it here", tc.name, gotKey)
					}
				}
			}
		})
	}
}

func TestReportSchemaV1Alpha1_PopulatedWireSerialization(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	maxSkew := int32(1)
	focus := leeway.Rank(1)

	doc := report.Report{
		SchemaVersion: report.SchemaVersion,
		GeneratedAt:   now,
		Clusters: []report.ClusterReport{
			{
				Cluster: "prod",
				Source:  "gke:acme-prod/us-central1/prod",
				Status:  report.StatusOK,
				Domains: map[string]map[string]report.DomainSummary{
					"topology.kubernetes.io/zone": {
						"us-central1-a": {
							Nodes:               2,
							Ready:               2,
							Usable:              2,
							AllocatableCPUMilli: 8000,
							AllocatableMemBytes: 34359738368,
						},
					},
				},
				TopologyFindings: []leeway.Finding{{
					Kind:        leeway.KindContractViolated,
					Subject:     leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "shop", Name: "checkout"},
					TopologyKey: "topology.kubernetes.io/zone",
					Tier:        "A",
					Severity:    "critical",
					Score: leeway.FindingScore{
						Drift:              0.5,
						ObservedSkew:       3,
						MinAchievableSkew:  0,
						ExcessSkew:         2,
						RelocationDistance: 2,
						MaxDomainShare:     1.0,
					},
					Intent: &leeway.FindingIntent{
						Source:     leeway.SourceTopologySpreadConstraint.String(),
						Confidence: leeway.ConfidenceDeclared.String(),
						Weighting:  leeway.WeightEqual.String(),
						MaxSkew:    &maxSkew,
						Evidence:   []string{"topologySpreadConstraints[0]: maxSkew=1"},
					},
					Domains: []leeway.FindingDomain{{
						Domain:     "us-central1-a",
						Actual:     3,
						Expected:   1,
						Delta:      2,
						ReadyNodes: 2,
						Note:       "over-allocated",
					}},
					SuspectedCause:      leeway.CauseConstraintIgnored,
					ContributingFactors: []string{"0 pods pinned by zonal volumes"},
					UntestedCauses:      []leeway.SuspectedCause{leeway.CauseRolloutBias},
					FirstSeenAt:         now,
					Transient:           leeway.TransientRollout.String(),
					Relaxed:             true,
					SeverityFromPolicy:  true,
					Remediation: &leeway.FindingRemediation{
						Kind:    leeway.RemediationManifestPatch,
						Summary: "Add topologySpreadConstraint",
						Command: "kubectl -n shop rollout restart deployment/checkout",
						YAML:    "spec:\n  template: {}",
					},
				}},
				ComputeClasses: []computeclass.ClassSummary{{
					Name:                "gold",
					Scorable:            true,
					Ordering:            leeway.ByListPosition.String(),
					PodsByRank:          map[string]int{"1": 4},
					PendingPods:         1,
					PendingProvisioning: 0,
					PendingDeclined:     1,
				}},
				RankFindings: []leeway.RankFinding{{
					Kind:       leeway.KindRankTierUnused,
					Subject:    leeway.SubjectRef{Kind: leeway.SubjectPreferenceAxis, Name: "gold"},
					Provider:   string(leeway.ProviderGKEComputeClass),
					ObjectKind: "ComputeClass",
					SpecHash:   "842f2f5b7a0deb21",
					Tier:       "C",
					Severity:   "info",
					Rule:       leeway.RankRuleTierUnused.String(),
					Focus:      &focus,
					Observed: leeway.RankObservation{
						WindowSeconds:       3600,
						TierSeconds:         14400,
						Rank0Share:          0,
						LastRank:            1,
						LastRankShare:       1,
						MeanRank:            1,
						PendingPods:         1,
						PendingProvisioning: 0,
						PendingDeclined:     1,
						DegradedPods:        4,
						Improving:           0,
						Worsening:           1,
						Lateral:             0,
						Baseline: &leeway.RankBaseline{
							Mean:      0.1,
							Band:      0.2,
							Samples:   48,
							FirstSeen: now.Add(-24 * time.Hour),
						},
					},
					Ranks: []leeway.RankShare{{
						Rank:       1,
						Rules:      "machineFamily=e2",
						PodSeconds: 14400,
						Share:      1,
						Pods:       4,
					}},
					Ordering:             leeway.ByListPosition.String(),
					Tiers:                2,
					ScaleUp:              leeway.ScaleUpPolicyAnyway.String(),
					OptimizeRulePriority: true,
					Reason:               "tier 1 unused",
					FirstSeenAt:          now,
				}},
				Subjects: []leeway.Finding{{
					Kind:           leeway.KindPlacementDrift,
					Subject:        leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "shop", Name: "healthy"},
					TopologyKey:    "topology.kubernetes.io/zone",
					Domains:        []leeway.FindingDomain{{Domain: "us-central1-a", Actual: 2, Expected: 2, ReadyNodes: 2}},
					SuspectedCause: leeway.CauseUnknown,
					FirstSeenAt:    now,
				}},
			},
			{
				Cluster: "unreachable",
				Source:  "kubeconfig:unreachable",
				Status:  report.StatusError,
				Error:   "context deadline exceeded",
			},
		},
		Summary: report.Summary{
			ClustersTotal:     2,
			ClustersSucceeded: 1,
			ClustersFailed:    1,
			TopologyFindings:  1,
			RankFindings:      1,
			BySeverity:        map[string]int{"critical": 1, "info": 1},
			ByTier:            map[string]int{"A": 1, "C": 1},
		},
	}

	rawBytes, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("json.Marshal(Report): %v", err)
	}
	var wire map[string]any
	if err := json.Unmarshal(rawBytes, &wire); err != nil {
		t.Fatalf("json.Unmarshal(Report): %v", err)
	}

	if got := wire["schema_version"]; got != "leeway.report/v1alpha1" {
		t.Errorf("wire schema_version = %v, want leeway.report/v1alpha1", got)
	}
	requireKeys(t, "Report", wire, "schema_version", "generated_at", "clusters", "summary")
	requireKeys(t, "Summary", wire["summary"].(map[string]any),
		"clusters_total", "clusters_succeeded", "clusters_failed", "topology_findings", "rank_findings", "by_severity", "by_tier")

	clusters := wire["clusters"].([]any)
	okCluster := clusters[0].(map[string]any)
	errCluster := clusters[1].(map[string]any)
	requireKeys(t, "ClusterReport[ok]", okCluster,
		"cluster", "source", "status", "domains", "topology_findings", "compute_classes", "rank_findings", "subjects")
	requireKeys(t, "ClusterReport[error]", errCluster, "cluster", "source", "status", "error")

	domSummary := okCluster["domains"].(map[string]any)["topology.kubernetes.io/zone"].(map[string]any)["us-central1-a"].(map[string]any)
	requireKeys(t, "DomainSummary", domSummary, "nodes", "ready", "usable", "allocatable_cpu_milli", "allocatable_mem_bytes")

	tf := okCluster["topology_findings"].([]any)[0].(map[string]any)
	requireKeys(t, "Finding", tf,
		"kind", "subject", "topologyKey", "tier", "severity", "score", "intent", "domains",
		"suspectedCause", "contributingFactors", "untestedCauses", "firstSeenAt",
		"transient", "relaxed", "severityFromPolicy", "remediation")
	requireKeys(t, "Finding.subject", tf["subject"].(map[string]any), "kind", "namespace", "name")
	requireKeys(t, "Finding.score", tf["score"].(map[string]any),
		"drift", "observedSkew", "minAchievableSkew", "excessSkew", "relocationDistance", "maxDomainShare")
	requireKeys(t, "Finding.intent", tf["intent"].(map[string]any),
		"source", "confidence", "weighting", "maxSkew", "evidence")
	requireKeys(t, "Finding.domains[0]", tf["domains"].([]any)[0].(map[string]any),
		"domain", "actual", "expected", "delta", "readyNodes", "note")
	requireKeys(t, "Finding.remediation", tf["remediation"].(map[string]any),
		"kind", "summary", "command", "yaml")

	cc := okCluster["compute_classes"].([]any)[0].(map[string]any)
	requireKeys(t, "ClassSummary", cc,
		"name", "scorable", "ordering", "podsByRank", "pendingPods", "pendingProvisioning", "pendingDeclined")

	rf := okCluster["rank_findings"].([]any)[0].(map[string]any)
	requireKeys(t, "RankFinding", rf,
		"kind", "subject", "provider", "objectKind", "specHash", "tier", "severity", "rule",
		"focus", "observed", "ranks", "ordering", "tiers", "scaleUp", "optimizeRulePriority", "reason", "firstSeenAt")
	obs := rf["observed"].(map[string]any)
	requireKeys(t, "RankObservation", obs,
		"windowSeconds", "tierSeconds", "rank0Share", "lastRank", "lastRankShare", "meanRank",
		"pendingPods", "pendingProvisioning", "pendingDeclined", "degradedPods",
		"improving", "worsening", "lateral", "baseline")
	requireKeys(t, "RankBaseline", obs["baseline"].(map[string]any), "mean", "band", "samples", "firstSeen")
	requireKeys(t, "RankShare", rf["ranks"].([]any)[0].(map[string]any), "rank", "rules", "podSeconds", "share", "pods")

	// Non-breaching subject in subjects[] must omit all optional omitempty fields
	// (intent, contributingFactors, untestedCauses, transient, relaxed,
	// severityFromPolicy, remediation).
	healthySub := okCluster["subjects"].([]any)[0].(map[string]any)
	requireKeys(t, "Finding[healthy-subject]", healthySub,
		"kind", "subject", "topologyKey", "tier", "severity", "score", "domains", "suspectedCause", "firstSeenAt")
}

func requireKeys(t *testing.T, label string, m map[string]any, want ...string) {
	t.Helper()
	for _, k := range want {
		if _, ok := m[k]; !ok {
			t.Errorf("%s missing expected JSON key %q (present: %v)", label, k, m)
		}
	}
	if len(m) != len(want) {
		t.Errorf("%s has %d keys (%v), want %d (%v)", label, len(m), m, len(want), want)
	}
}

func TestReportSchemaV1Alpha1_WireVocabularyFrozen(t *testing.T) {
	t.Parallel()

	wantKinds := []string{
		"leeway.contract_violated",
		"leeway.placement_drift",
		"leeway.baseline_breach",
		"leeway.domain_unavailable",
		"leeway.rank_wedged",
		"leeway.rank_degraded",
		"leeway.rank_no_migration",
		"leeway.rank_tier_unused",
	}
	gotKinds := []string{
		leeway.KindContractViolated,
		leeway.KindPlacementDrift,
		leeway.KindBaselineBreach,
		leeway.KindDomainUnavailable,
		leeway.KindRankWedged,
		leeway.KindRankDegraded,
		leeway.KindRankNoMigration,
		leeway.KindRankTierUnused,
	}
	if !slices.Equal(gotKinds, wantKinds) {
		t.Errorf("finding kinds = %v, want %v", gotKinds, wantKinds)
	}

	wantCauses := []leeway.SuspectedCause{
		"unknown",
		"domain_outage",
		"domain_capacity_shortfall",
		"taint_exclusion",
		"consolidation",
		"volume_pinning",
		"rollout_bias",
		"constraint_ignored",
		"selector_confined",
	}
	gotCauses := []leeway.SuspectedCause{
		leeway.CauseUnknown,
		leeway.CauseDomainOutage,
		leeway.CauseCapacityShortfall,
		leeway.CauseTaintExclusion,
		leeway.CauseConsolidation,
		leeway.CauseVolumePinning,
		leeway.CauseRolloutBias,
		leeway.CauseConstraintIgnored,
		leeway.CauseSelectorConfined,
	}
	if !slices.Equal(gotCauses, wantCauses) {
		t.Errorf("suspected causes = %v, want %v", gotCauses, wantCauses)
	}

	wantRemediation := []leeway.RemediationKind{
		"manifest_patch",
		"rollout_restart",
		"cluster_capacity",
		"cluster_nodes",
		"storage_topology",
		"platform_managed",
	}
	gotRemediation := []leeway.RemediationKind{
		leeway.RemediationManifestPatch,
		leeway.RemediationRolloutRestart,
		leeway.RemediationClusterCapacity,
		leeway.RemediationClusterNodes,
		leeway.RemediationStorageTopology,
		leeway.RemediationPlatformManaged,
	}
	if !slices.Equal(gotRemediation, wantRemediation) {
		t.Errorf("remediation kinds = %v, want %v", gotRemediation, wantRemediation)
	}

	var gotSources []string
	for _, s := range leeway.IntentSources() {
		gotSources = append(gotSources, s.String())
	}
	wantSources := []string{
		"policy-crd",
		"workload-annotation",
		"topology-spread-constraint",
		"pod-anti-affinity-required",
		"pod-affinity-required",
		"cluster-default-declared",
		"pod-anti-affinity-preferred",
		"pod-affinity-preferred",
		"learned-baseline",
		"cluster-default-assumed",
	}
	if !slices.Equal(gotSources, wantSources) {
		t.Errorf("intent sources = %v, want %v", gotSources, wantSources)
	}

	if got, want := leeway.Severities(), []string{"info", "warning", "critical"}; !slices.Equal(got, want) {
		t.Errorf("Severities() = %v, want %v", got, want)
	}

	gotTiers := []string{leeway.TierNone.String(), leeway.TierA.String(), leeway.TierB.String(), leeway.TierC.String()}
	if want := []string{"", "A", "B", "C"}; !slices.Equal(gotTiers, want) {
		t.Errorf("Tier.String() = %v, want %v", gotTiers, want)
	}

	gotConfidence := []string{
		leeway.ConfidenceDeclared.String(),
		leeway.ConfidenceInferred.String(),
		leeway.ConfidenceAssumed.String(),
		leeway.ConfidenceLearned.String(),
	}
	if want := []string{"declared", "inferred", "assumed", "learned"}; !slices.Equal(gotConfidence, want) {
		t.Errorf("Confidence.String() = %v, want %v", gotConfidence, want)
	}

	gotWeighting := []string{
		leeway.WeightAuto.String(),
		leeway.WeightEqual.String(),
		leeway.WeightNodeCount.String(),
		leeway.WeightAllocatableCPU.String(),
		leeway.WeightAllocatableMemory.String(),
	}
	if want := []string{"Auto", "Equal", "NodeCount", "AllocatableCPU", "AllocatableMemory"}; !slices.Equal(gotWeighting, want) {
		t.Errorf("Weighting.String() = %v, want %v", gotWeighting, want)
	}

	gotRankRules := []string{
		leeway.RankRuleNone.String(),
		leeway.RankRuleWedged.String(),
		leeway.RankRuleRank0Share.String(),
		leeway.RankRuleLastRank.String(),
		leeway.RankRuleNoMigration.String(),
		leeway.RankRuleTierUnused.String(),
		leeway.RankRuleBaseline.String(),
	}
	if want := []string{"", "wedged", "rank0-share", "last-rank", "no-migration", "tier-unused", "rank-baseline"}; !slices.Equal(gotRankRules, want) {
		t.Errorf("RankRule.String() = %v, want %v", gotRankRules, want)
	}

	gotOrdering := []string{
		leeway.OrderingInvalid.String(),
		leeway.ByListPosition.String(),
		leeway.ByPriorityScore.String(),
	}
	if want := []string{"invalid", "list-position", "priority-score"}; !slices.Equal(gotOrdering, want) {
		t.Errorf("OrderingMode.String() = %v, want %v", gotOrdering, want)
	}

	gotScaleUp := []string{
		leeway.ScaleUpPolicyUnset.String(),
		leeway.ScaleUpPolicyAnyway.String(),
		leeway.ScaleUpPolicyDoNotScaleUp.String(),
		leeway.ScaleUpPolicyUnrecognised.String(),
	}
	if want := []string{"unset", "scale-up-anyway", "do-not-scale-up", "unrecognised"}; !slices.Equal(gotScaleUp, want) {
		t.Errorf("ScaleUpPolicy.String() = %v, want %v", gotScaleUp, want)
	}

	gotSubjects := []leeway.SubjectKind{
		leeway.SubjectDeployment,
		leeway.SubjectStatefulSet,
		leeway.SubjectDaemonSet,
		leeway.SubjectJob,
		leeway.SubjectNodeGroup,
		leeway.SubjectCustom,
		leeway.SubjectDomain,
		leeway.SubjectPreferenceAxis,
	}
	wantSubjects := []leeway.SubjectKind{
		"Deployment", "StatefulSet", "DaemonSet", "Job", "NodeGroup", "Custom", "Domain", "PreferenceAxis",
	}
	if !slices.Equal(gotSubjects, wantSubjects) {
		t.Errorf("SubjectKind constants = %v, want %v", gotSubjects, wantSubjects)
	}
	if leeway.ProviderGKEComputeClass != "gke-computeclass" {
		t.Errorf("ProviderGKEComputeClass = %q, want %q", leeway.ProviderGKEComputeClass, "gke-computeclass")
	}
}

func TestClusterReport_FindingIdentityHelpers(t *testing.T) {
	t.Parallel()

	gkeCluster := report.ClusterReport{
		Cluster: "c1",
		Source:  "gke:p1/us-central1/c1",
		Status:  report.StatusOK,
	}
	if got, want := gkeCluster.CanonicalCluster(), "p1/us-central1/c1"; got != want {
		t.Fatalf("CanonicalCluster(gke) = %q, want %q", got, want)
	}

	kubeCluster := report.ClusterReport{
		Cluster: "gke-prod-east",
		Source:  "kubeconfig:gke-prod-east",
		Status:  report.StatusOK,
	}
	if got, want := kubeCluster.CanonicalCluster(), "gke-prod-east"; got != want {
		t.Fatalf("CanonicalCluster(kubeconfig) = %q, want %q", got, want)
	}

	malformedGKE := report.ClusterReport{
		Cluster: "fallback-name",
		Source:  "gke:incomplete/triple",
		Status:  report.StatusOK,
	}
	if got, want := malformedGKE.CanonicalCluster(), "fallback-name"; got != want {
		t.Fatalf("CanonicalCluster(malformed gke) = %q, want %q", got, want)
	}

	tf := leeway.Finding{
		Kind:        leeway.KindContractViolated,
		Subject:     leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "shop", Name: "batch-worker"},
		TopologyKey: "topology.kubernetes.io/zone",
	}
	if got, want := gkeCluster.TopologyFindingKey(tf), "p1/us-central1/c1/shop/Deployment/batch-worker/topology.kubernetes.io/zone/leeway.contract_violated"; got != want {
		t.Errorf("ClusterReport.TopologyFindingKey = %q, want %q", got, want)
	}

	rf := leeway.RankFinding{
		Kind:       leeway.KindRankDegraded,
		Subject:    leeway.SubjectRef{Kind: leeway.SubjectPreferenceAxis, Name: "gold"},
		ObjectKind: "ComputeClass",
		Rule:       "last-rank",
	}
	if got, want := gkeCluster.RankFindingKey(rf), "p1/us-central1/c1/_cluster/ComputeClass/gold/rule:last-rank/leeway.rank_degraded"; got != want {
		t.Errorf("ClusterReport.RankFindingKey = %q, want %q", got, want)
	}
}

func pinType[T any](v T) T { return v }

// TestPkgLeeway_ExternalGoAPISurfaceFrozen pins the exported pkg/leeway types,
// constants, struct fields, and function signatures guaranteed to external Go
// module callers in docs/stability-policy.md §3 (k8s-lookout audit computeclass,
// k8s-lookout pkg/sources/capacity, and pure scoring/remediation callers).
//
// Any breaking change to these signatures or struct fields fails compilation.
func TestPkgLeeway_ExternalGoAPISurfaceFrozen(t *testing.T) {
	t.Parallel()

	// 1. k8s-lookout `audit computeclass` surface.
	decodeCC := pinType[func(string, map[string]any) (*leeway.ComputeClass, error)](leeway.DecodeComputeClass)
	newPrefAxis := pinType[func(leeway.AxisKey, []leeway.PreferenceRule) *leeway.PreferenceAxis](leeway.NewPreferenceAxis)
	cc, err := decodeCC("gold", map[string]any{
		"priorities": []any{
			map[string]any{"machineFamily": "n2", "spot": true},
			map[string]any{"machineFamily": "n2", "spot": false},
		},
		"whenUnsatisfiable": "DoNotScaleUp",
		"activeMigration":   map[string]any{"optimizeRulePriority": true},
	})
	if err != nil {
		t.Fatalf("DecodeComputeClass: %v", err)
	}
	if !cc.Scorable() || !cc.Axis.Scorable() {
		t.Fatalf("2-tier ComputeClass should be Scorable")
	}
	axisKey := leeway.AxisKey{Provider: leeway.ProviderGKEComputeClass, Name: "gold"}
	if got := axisKey.String(); got != "gke-computeclass/gold" {
		t.Errorf("AxisKey.String() = %q, want gke-computeclass/gold", got)
	}
	manualAxis := newPrefAxis(axisKey, cc.Axis.Rules)
	if !manualAxis.Scorable() {
		t.Errorf("NewPreferenceAxis should produce a Scorable 2-tier axis")
	}
	pinType[leeway.AxisKey](cc.Axis.Key)
	pinType[leeway.Provider](cc.Axis.Key.Provider)
	pinType[string](cc.Axis.Key.Name)
	pinType[leeway.OrderingMode](cc.Axis.Ordering)
	pinType[[]leeway.PreferenceRule](cc.Axis.Rules)
	pinType[int](cc.Axis.Tiers)
	pinType[string](cc.Axis.SpecHash)
	pinType[leeway.ScaleUpPolicy](cc.ScaleUp)
	pinType[bool](cc.OptimizeRulePriority)
	if cc.ScaleUp != leeway.ScaleUpPolicyDoNotScaleUp || cc.ScaleUp.String() != "do-not-scale-up" {
		t.Errorf("unexpected ScaleUpPolicy surface: %v (%s)", cc.ScaleUp, cc.ScaleUp.String())
	}
	if cc.Axis.Ordering != leeway.ByListPosition || cc.Axis.Ordering.String() != "list-position" {
		t.Errorf("unexpected OrderingMode surface: %v (%s)", cc.Axis.Ordering, cc.Axis.Ordering.String())
	}
	rule0 := cc.Axis.Rules[0]
	pinType[int](rule0.Index)
	pinType[*int](rule0.Score)
	pinType[leeway.Rank](rule0.Rank)
	pinType[map[string]any](rule0.Raw)
	pinType[bool](rule0.Peer(rule0))
	pinType[string](rule0.Render())
	pinType[string](rule0.Rank.String())
	if !rule0.Rank.IsTier() || rule0.Rank.String() != "0" ||
		leeway.RankUnknown.IsTier() || leeway.RankUnknown.String() != "unknown" ||
		leeway.RankUnsatisfiable.IsTier() || leeway.RankUnsatisfiable.String() != "unsatisfiable" ||
		leeway.RankOffAxis.IsTier() || leeway.RankOffAxis.String() != "off-axis" {
		t.Errorf("unexpected Rank sentinel IsTier/String behaviour")
	}

	// 2. k8s-lookout `pkg/sources/capacity` per-pod scale-up verdict surface.
	isScaleUp := pinType[func(string) bool](leeway.IsScaleUpReason)
	latestScaleUp := pinType[func([]leeway.AutoscalerEvent, string, time.Time) leeway.ScaleUpVerdict](leeway.LatestScaleUp)
	for _, reason := range []string{
		leeway.ReasonTriggeredScaleUp,
		leeway.ReasonNotTriggerScaleUp,
		leeway.ReasonFailedScaleUp,
	} {
		if !isScaleUp(reason) {
			t.Errorf("IsScaleUpReason(%q) = false, want true", reason)
		}
	}
	if isScaleUp("Scheduled") {
		t.Errorf("IsScaleUpReason(Scheduled) = true, want false")
	}
	t0 := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	verdict := latestScaleUp([]leeway.AutoscalerEvent{
		{Reason: leeway.ReasonFailedScaleUp, At: t0, PodUID: "uid-1"},
		{Reason: leeway.ReasonTriggeredScaleUp, At: t0, PodUID: "uid-1"},
	}, "uid-1", t0.Add(-time.Minute))
	if verdict != leeway.ScaleUpProvisioning || verdict.String() != "provisioning" ||
		leeway.ScaleUpDeclined.String() != "declined" ||
		leeway.ScaleUpUnheard.String() != "unheard" {
		t.Errorf("LatestScaleUp same-second tie = %v (%s), want ScaleUpProvisioning", verdict, verdict)
	}

	// 3. Pure scoring, attribution, remediation, and finding-identity surface.
	pinType[leeway.Domain](leeway.DomainUnknown)
	pinType[func(leeway.Domain) leeway.Domain](leeway.NormaliseDomain)
	pinType[func(string) (leeway.SubjectRef, bool)](leeway.ParseSubjectRef)
	pinType[func([]leeway.Domain)](leeway.SortDomains)
	pinType[func() *leeway.Distribution](leeway.NewDistribution)
	pinType[leeway.IntentMode](leeway.ModeSpread)
	pinType[leeway.IntentMode](leeway.ModeColocate)
	pinType[leeway.IntentMode](leeway.ModeIgnore)
	pinType[func([]leeway.Intent) map[leeway.TopologyKey]*leeway.Intent](leeway.ResolveIntents)
	pinType[func() leeway.EligibilityOptions](leeway.DefaultEligibilityOptions)
	pinType[func([]leeway.NodeView, leeway.EligibilityOptions) leeway.Eligibility](leeway.EligibleDomains)
	pinType[int64](leeway.Uncapped)
	pinType[func(int64, []float64, []int64) leeway.Apportionment](leeway.Apportion)
	pinType[func(int64, []float64, []int64, []int64) leeway.Apportionment](leeway.ApportionAligned)
	pinType[func() leeway.Thresholds](leeway.DefaultThresholds)
	pinType[func() leeway.Thresholds](leeway.ReportThresholds)
	pinType[func([]leeway.Domain, []int64, leeway.Apportionment, *int32, leeway.Thresholds) leeway.Scores](leeway.Score)
	pinType[func(*leeway.Scores, *leeway.Intent, leeway.Thresholds) leeway.Verdict]((*leeway.Scores).Judge)
	pinType[func() leeway.CauseConfig](leeway.DefaultCauseConfig)
	pinType[func(*leeway.Scores, *leeway.Intent, leeway.Evidence, time.Time, leeway.CauseConfig) leeway.Attribution]((*leeway.Scores).Attribute)
	pinType[leeway.Evidence](leeway.Evidence{Domains: map[leeway.Domain]leeway.DomainFacts{}})
	pinType[func(leeway.FindingInput) leeway.Finding](leeway.NewFinding)
	pinType[func(leeway.DomainAvailability, leeway.TopologyKey, leeway.AlertState) leeway.Finding](leeway.NewDomainFinding)
	pinType[func(leeway.Finding, map[string]string) *leeway.FindingRemediation](leeway.SuggestRemediation)
	pinType[func(leeway.Finding, *leeway.FindingRemediation) *leeway.FindingRemediation](leeway.PlatformManagedRemediation)
	pinType[func(leeway.RankFindingInput) leeway.RankFinding](leeway.NewRankFinding)
	pinType[func(leeway.RankFinding) string](leeway.RankMessage)
	if leeway.ClusterWideNamespace != "_cluster" {
		t.Errorf("ClusterWideNamespace = %q, want _cluster", leeway.ClusterWideNamespace)
	}
	pinType[func(string, leeway.Finding) string](leeway.TopologyFindingKey)
	pinType[func(string, leeway.RankFinding) string](leeway.RankFindingKey)
}
