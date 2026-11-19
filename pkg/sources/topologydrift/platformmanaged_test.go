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

package topologydrift

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	appslisters "k8s.io/client-go/listers/apps/v1"

	"github.com/go-steer/leeway/pkg/leeway"
	"github.com/go-steer/leeway/pkg/sources"
)

// The tests in this file pin the platform_managed remediation: a workload
// whose own metadata carries addonmanager.kubernetes.io/mode: Reconcile (GKE's
// kube-system/kube-dns, for one) keeps its finding, but gets no manifest patch
// the platform could revert.

// addonSubject is the workload addonCluster builds, as a kind of workload.
func addonSubject(kind leeway.SubjectKind) leeway.SubjectRef {
	return leeway.SubjectRef{Kind: kind, Namespace: "payments", Name: "api"}
}

// addonObjects is what addonCluster builds.
type addonObjects struct {
	nodes []*corev1.Node
	pods  []*corev1.Pod
	rss   []*appsv1.ReplicaSet
	deps  []*appsv1.Deployment
	stss  []*appsv1.StatefulSet
}

// addonCluster is #543's 8/1/1 cluster (a Tier B zone finding whose
// remediation is a manifest_patch) with its pods owned by a workload of kind
// (a Deployment through its ReplicaSet, or a StatefulSet directly), and that
// workload's object carrying mode as its addon manager label ("" for none).
func addonCluster(kind leeway.SubjectKind, mode string) addonObjects {
	c := eightOneOne[0]
	labels := map[string]string{"app": "api"}
	if mode != "" {
		labels[addonManagerModeLabel] = mode
	}
	meta := metav1.ObjectMeta{Name: "api", Namespace: "payments", Labels: labels}
	o := addonObjects{nodes: hostSpreadZones(c.perZone)}
	ownerKind, ownerName := "ReplicaSet", "api-7f9"
	switch kind {
	case leeway.SubjectDeployment:
		o.rss = []*appsv1.ReplicaSet{replicaSet("api-7f9", "payments", "api")}
		o.deps = []*appsv1.Deployment{{ObjectMeta: meta}}
	case leeway.SubjectStatefulSet:
		ownerKind, ownerName = "StatefulSet", "api"
		o.stss = []*appsv1.StatefulSet{{ObjectMeta: meta}}
	default:
		panic("addonCluster: unsupported kind " + string(kind))
	}
	for i, p := range replicas(c.rep, c.nodes...) {
		p.Name = fmt.Sprintf("%s-%d", ownerName, i)
		p.UID = types.UID("payments/" + p.Name)
		p.Status.Phase = corev1.PodRunning
		ownedBy(ownerKind, ownerName)(p)
		o.pods = append(o.pods, p)
	}
	return o
}

// watchRemediation runs addonCluster through Run (the informer path) and
// returns the remediation on the one zone finding it emits.
func watchRemediation(t *testing.T, kind leeway.SubjectKind, mode string) *leeway.FindingRemediation {
	t.Helper()
	o := addonCluster(kind, mode)
	var objs []runtime.Object
	for _, n := range o.nodes {
		objs = append(objs, n)
	}
	for _, p := range o.pods {
		objs = append(objs, p)
	}
	for _, rs := range o.rss {
		objs = append(objs, rs)
	}
	for _, d := range o.deps {
		objs = append(objs, d)
	}
	for _, sts := range o.stss {
		objs = append(objs, sts)
	}

	cfg := quickAlerts()
	cfg.TopologyKeys = DefaultTopologyKeys
	s := New(fake.NewSimpleClientset(objs...), cfg)
	s.logf = func(string, ...any) {}

	var mu sync.Mutex
	var got []sources.Signal
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Run(ctx, func(sig sources.Signal) {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, sig)
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("Run did not return after cancellation")
		}
	})
	signals := func() []sources.Signal {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(got)
	}
	waitFor(t, "the finding to reach the wire", func() bool { return len(signals()) > 0 })
	time.Sleep(50 * time.Millisecond)

	sigs := signals()
	if len(sigs) != 1 || sigs[0].LeewayFinding == nil {
		t.Fatalf("%d signal(s), want exactly one zone finding with a LeewayFinding: %v", len(sigs), sigs)
	}
	f := sigs[0].LeewayFinding
	if want := addonSubject(kind); f.Subject != want || f.TopologyKey != zoneKey {
		t.Fatalf("finding is on %s %s, want %s %s", f.Subject, f.TopologyKey, want, zoneKey)
	}
	if f.Remediation == nil {
		t.Fatal("the finding has no remediation")
	}
	return f.Remediation
}

// snapshotRemediation runs addonCluster through EvaluateSnapshot (the
// `leeway report` path) and returns the zone finding's remediation.
func snapshotRemediation(t *testing.T, kind leeway.SubjectKind, mode string) *leeway.FindingRemediation {
	t.Helper()
	o := addonCluster(kind, mode)
	snap, err := EvaluateSnapshot(context.Background(), Config{TopologyKeys: DefaultTopologyKeys}, SnapshotInput{
		Nodes:        o.nodes,
		Pods:         o.pods,
		ReplicaSets:  o.rss,
		Deployments:  o.deps,
		StatefulSets: o.stss,
	})
	if err != nil {
		t.Fatalf("EvaluateSnapshot: %v", err)
	}
	want := addonSubject(kind)
	for _, f := range snap.Findings {
		if f.Subject == want && f.TopologyKey == zoneKey {
			if f.Remediation == nil {
				t.Fatal("the zone finding has no remediation")
			}
			return f.Remediation
		}
	}
	t.Fatalf("no zone finding for %s in %+v", want, snap.Findings)
	return nil
}

func assertPlatformManaged(t *testing.T, kind leeway.SubjectKind, rem *leeway.FindingRemediation) {
	t.Helper()
	if rem.Kind != leeway.RemediationPlatformManaged {
		t.Errorf("Kind = %q, want %q", rem.Kind, leeway.RemediationPlatformManaged)
	}
	if rem.YAML != "" {
		t.Errorf("YAML = %q, want empty: the platform can revert any patch", rem.YAML)
	}
	if rem.Command != "" {
		t.Errorf("Command = %q, want empty", rem.Command)
	}
	if sub := addonSubject(kind); !strings.Contains(rem.Summary, sub.String()) || !strings.Contains(rem.Summary, "can revert edits") {
		t.Errorf("Summary = %q, want it to name %s and say edits can be reverted", rem.Summary, sub)
	}
}

func assertManifestPatch(t *testing.T, rem *leeway.FindingRemediation) {
	t.Helper()
	if rem.Kind != leeway.RemediationManifestPatch {
		t.Errorf("Kind = %q, want %q", rem.Kind, leeway.RemediationManifestPatch)
	}
	if !strings.Contains(rem.YAML, "topologySpreadConstraints:") {
		t.Errorf("YAML = %q, want a topologySpreadConstraints patch", rem.YAML)
	}
}

// addonKinds are the workload kinds whose objects both paths hold, so both
// paths can read their labels.
var addonKinds = []leeway.SubjectKind{leeway.SubjectDeployment, leeway.SubjectStatefulSet}

func TestPlatformManaged_WatchPathReplacesTheManifestPatch(t *testing.T) {
	for _, kind := range addonKinds {
		t.Run(string(kind), func(t *testing.T) {
			assertPlatformManaged(t, kind, watchRemediation(t, kind, addonManagerReconcile))
		})
	}
}

func TestPlatformManaged_WatchPathKeepsTheManifestPatchWithoutTheLabel(t *testing.T) {
	for _, kind := range addonKinds {
		t.Run(string(kind), func(t *testing.T) {
			assertManifestPatch(t, watchRemediation(t, kind, ""))
		})
	}
}

func TestPlatformManaged_SnapshotPathReplacesTheManifestPatch(t *testing.T) {
	for _, kind := range addonKinds {
		t.Run(string(kind), func(t *testing.T) {
			assertPlatformManaged(t, kind, snapshotRemediation(t, kind, addonManagerReconcile))
		})
	}
}

func TestPlatformManaged_SnapshotPathKeepsTheManifestPatchWithoutTheLabel(t *testing.T) {
	for _, kind := range addonKinds {
		t.Run(string(kind), func(t *testing.T) {
			assertManifestPatch(t, snapshotRemediation(t, kind, ""))
		})
	}
}

// EnsureExists only creates a missing object, so an edit survives and the
// patch is still worth offering.
func TestPlatformManaged_EnsureExistsKeepsTheManifestPatch(t *testing.T) {
	assertManifestPatch(t, snapshotRemediation(t, leeway.SubjectDeployment, "EnsureExists"))
}

// TestPlatformManaged_ClusterAndStorageRemediationsAreUnchanged runs
// remediationFor — the call both paths make — on a reconciled StatefulSet for
// each cause whose remediation acts on nodes, node pools or volumes. Those
// are not the workload's reconciler's to revert, so the remediation must be the one
// an unlabelled workload gets.
func TestPlatformManaged_ClusterAndStorageRemediationsAreUnchanged(t *testing.T) {
	sub := leeway.SubjectRef{Kind: leeway.SubjectStatefulSet, Namespace: "kube-system", Name: "addon-store"}
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: sub.Name, Namespace: sub.Namespace,
		Labels: map[string]string{addonManagerModeLabel: addonManagerReconcile},
	}}
	idx := newNamespaceIndexer()
	if err := idx.Add(sts); err != nil {
		t.Fatal(err)
	}
	s := New(fake.NewSimpleClientset(), Config{})
	s.statefulSets = appslisters.NewStatefulSetLister(idx)

	finding := func(cause leeway.SuspectedCause) leeway.Finding {
		return leeway.Finding{
			Kind:           leeway.KindPlacementDrift,
			Subject:        sub,
			TopologyKey:    zoneKey,
			SuspectedCause: cause,
			Domains: []leeway.FindingDomain{
				{Domain: "zone-a", Actual: 3, Expected: 2, Delta: 1, ReadyNodes: 2},
				{Domain: "zone-b", Actual: 1, Expected: 2, Delta: -1, ReadyNodes: 2},
			},
		}
	}

	if !s.platformReconciled(sub) {
		t.Fatal("platformReconciled = false for the labelled StatefulSet; the cases below would pass vacuously")
	}
	// The same lister turns an unknown cause's patch into platform_managed,
	// so the cases below are a statement about the cause, not the lookup.
	if got := s.remediationFor(finding(leeway.CauseUnknown), sub, zoneKey); got == nil || got.Kind != leeway.RemediationPlatformManaged {
		t.Fatalf("unknown cause: remediation = %+v, want %q", got, leeway.RemediationPlatformManaged)
	}

	for _, tc := range []struct {
		cause leeway.SuspectedCause
		want  leeway.RemediationKind
	}{
		{leeway.CauseCapacityShortfall, leeway.RemediationClusterCapacity},
		{leeway.CauseConsolidation, leeway.RemediationClusterCapacity},
		{leeway.CauseDomainOutage, leeway.RemediationClusterNodes},
		{leeway.CauseTaintExclusion, leeway.RemediationClusterNodes},
		{leeway.CauseSelectorConfined, leeway.RemediationClusterNodes},
		{leeway.CauseVolumePinning, leeway.RemediationStorageTopology},
	} {
		t.Run(string(tc.cause), func(t *testing.T) {
			f := finding(tc.cause)
			got := s.remediationFor(f, sub, zoneKey)
			want := leeway.SuggestRemediation(f, nil)
			if got == nil || got.Kind != tc.want {
				t.Fatalf("remediation = %+v, want kind %q", got, tc.want)
			}
			if *got != *want {
				t.Errorf("remediation = %+v, want the unlabelled workload's %+v", *got, *want)
			}
		})
	}
}
