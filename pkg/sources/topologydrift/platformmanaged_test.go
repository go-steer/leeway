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
// the addon manager would revert.

var addonSubject = leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "payments", Name: "api"}

// addonCluster is #543's 8/1/1 cluster (a Tier B zone finding whose
// remediation is a manifest_patch) plus the Deployment object above its
// ReplicaSet, carrying mode as its addon manager label ("" for none).
func addonCluster(mode string) (nodes []*corev1.Node, pods []*corev1.Pod, rss []*appsv1.ReplicaSet, deps []*appsv1.Deployment) {
	c := eightOneOne[0]
	nodes = hostSpreadZones(c.perZone)
	for i, p := range replicas(c.rep, c.nodes...) {
		p.Name = fmt.Sprintf("api-7f9-%d", i)
		p.UID = types.UID("payments/" + p.Name)
		p.Status.Phase = corev1.PodRunning
		ownedBy("ReplicaSet", "api-7f9")(p)
		pods = append(pods, p)
	}
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: "api", Namespace: "payments", Labels: map[string]string{"app": "api"},
	}}
	if mode != "" {
		dep.Labels[addonManagerModeLabel] = mode
	}
	return nodes, pods, []*appsv1.ReplicaSet{replicaSet("api-7f9", "payments", "api")}, []*appsv1.Deployment{dep}
}

// watchRemediation runs addonCluster through Run (the informer path) and
// returns the remediation on the one zone finding it emits.
func watchRemediation(t *testing.T, mode string) *leeway.FindingRemediation {
	t.Helper()
	nodes, pods, rss, deps := addonCluster(mode)
	var objs []runtime.Object
	for _, n := range nodes {
		objs = append(objs, n)
	}
	for _, p := range pods {
		objs = append(objs, p)
	}
	for _, rs := range rss {
		objs = append(objs, rs)
	}
	for _, d := range deps {
		objs = append(objs, d)
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
	if f.Subject != addonSubject || f.TopologyKey != zoneKey {
		t.Fatalf("finding is on %s %s, want %s %s", f.Subject, f.TopologyKey, addonSubject, zoneKey)
	}
	if f.Remediation == nil {
		t.Fatal("the finding has no remediation")
	}
	return f.Remediation
}

// snapshotRemediation runs addonCluster through EvaluateSnapshot (the
// `leeway report` path) and returns the zone finding's remediation.
func snapshotRemediation(t *testing.T, mode string) *leeway.FindingRemediation {
	t.Helper()
	nodes, pods, rss, deps := addonCluster(mode)
	snap, err := EvaluateSnapshot(context.Background(), Config{TopologyKeys: DefaultTopologyKeys}, SnapshotInput{
		Nodes:       nodes,
		Pods:        pods,
		ReplicaSets: rss,
		Deployments: deps,
	})
	if err != nil {
		t.Fatalf("EvaluateSnapshot: %v", err)
	}
	for _, f := range snap.Findings {
		if f.Subject == addonSubject && f.TopologyKey == zoneKey {
			if f.Remediation == nil {
				t.Fatal("the zone finding has no remediation")
			}
			return f.Remediation
		}
	}
	t.Fatalf("no zone finding for %s in %+v", addonSubject, snap.Findings)
	return nil
}

func assertPlatformManaged(t *testing.T, rem *leeway.FindingRemediation) {
	t.Helper()
	if rem.Kind != leeway.RemediationPlatformManaged {
		t.Errorf("Kind = %q, want %q", rem.Kind, leeway.RemediationPlatformManaged)
	}
	if rem.YAML != "" {
		t.Errorf("YAML = %q, want empty: the addon manager reverts any patch", rem.YAML)
	}
	if rem.Command != "" {
		t.Errorf("Command = %q, want empty", rem.Command)
	}
	if !strings.Contains(rem.Summary, addonSubject.String()) || !strings.Contains(rem.Summary, "reverts edits") {
		t.Errorf("Summary = %q, want it to name %s and say edits are reverted", rem.Summary, addonSubject)
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

func TestPlatformManaged_WatchPathReplacesTheManifestPatch(t *testing.T) {
	assertPlatformManaged(t, watchRemediation(t, addonManagerReconcile))
}

func TestPlatformManaged_WatchPathKeepsTheManifestPatchWithoutTheLabel(t *testing.T) {
	assertManifestPatch(t, watchRemediation(t, ""))
}

func TestPlatformManaged_SnapshotPathReplacesTheManifestPatch(t *testing.T) {
	assertPlatformManaged(t, snapshotRemediation(t, addonManagerReconcile))
}

func TestPlatformManaged_SnapshotPathKeepsTheManifestPatchWithoutTheLabel(t *testing.T) {
	assertManifestPatch(t, snapshotRemediation(t, ""))
}

// EnsureExists only creates a missing object, so an edit survives and the
// patch is still worth offering.
func TestPlatformManaged_EnsureExistsKeepsTheManifestPatch(t *testing.T) {
	assertManifestPatch(t, snapshotRemediation(t, "EnsureExists"))
}

// TestPlatformManaged_ClusterAndStorageRemediationsAreUnchanged runs
// remediationFor — the call both paths make — on a reconciled StatefulSet for
// each cause whose remediation acts on nodes, node pools or volumes. Those
// are not the addon manager's to revert, so the remediation must be the one
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
