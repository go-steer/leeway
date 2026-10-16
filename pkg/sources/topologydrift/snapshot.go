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
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/go-steer/leeway/pkg/leeway"
)

// PolicyGVR returns the GroupVersionResource for namespaced LeewayPolicy objects.
func PolicyGVR() schema.GroupVersionResource { return policyGVR }

// ClusterPolicyGVR returns the GroupVersionResource for cluster-scoped ClusterLeewayPolicy objects.
func ClusterPolicyGVR() schema.GroupVersionResource { return clusterPolicyGVR }

// PoliciesServed reports which of the LeewayPolicy and ClusterLeewayPolicy CRDs
// the cluster's discovery API serves.
func PoliciesServed(client kubernetes.Interface) (namespaced, clusterScoped bool) {
	if client == nil {
		return false, false
	}
	s := &Source{client: client}
	return s.policyCRDsServed()
}

// SnapshotInput holds the point-in-time cluster resources for a one-shot
// topology-drift evaluation (`leeway report`).
type SnapshotInput struct {
	Nodes           []*corev1.Node
	Pods            []*corev1.Pod
	ReplicaSets     []*appsv1.ReplicaSet
	Deployments     []*appsv1.Deployment
	StatefulSets    []*appsv1.StatefulSet
	Claims          []*corev1.PersistentVolumeClaim
	Volumes         []*corev1.PersistentVolume
	Policies        []*unstructured.Unstructured
	ClusterPolicies []*unstructured.Unstructured
	Events          []*corev1.Event

	// CapacityOracle is optional; when nil, EvaluateSnapshot derives a pending-pod
	// capacity oracle from Pods and Events.
	CapacityOracle CapacityOracle

	// Now is the evaluation timestamp; defaults to time.Now().UTC() when zero.
	Now time.Time
}

// SubjectEvaluation holds the resolved evaluations and domain distributions for
// one workload or node-group subject in a point-in-time snapshot.
type SubjectEvaluation struct {
	Subject       leeway.SubjectRef
	Evaluations   []Evaluation
	Distributions map[leeway.TopologyKey]*leeway.Distribution
}

// SnapshotResult is the complete output of EvaluateSnapshot for one cluster.
type SnapshotResult struct {
	// Domains maps each tracked topology key to per-domain node statistics.
	Domains map[leeway.TopologyKey]map[leeway.Domain]DomainStats

	// DomainAvailabilities maps each failure-domain topology key to the
	// availability readings of its known domains.
	DomainAvailabilities map[leeway.TopologyKey][]leeway.DomainAvailability

	// Findings contains all breached placement and domain findings that pass
	// routing (Tier A, Tier B, and Tier C when Config.TierCSignals is set).
	Findings []leeway.Finding

	// Subjects contains all evaluated workload and node-group subjects in
	// deterministic order.
	Subjects []SubjectEvaluation

	// NodeGroupsDiscovered is the total number of node groups resolved from
	// node labels.
	NodeGroupsDiscovered int64

	// source is retained so callers (such as pkg/report with --all or --min-drift)
	// can build a Finding for a non-breaching or Tier C evaluation on demand.
	source *Source
	now    time.Time
}

// FindingFor constructs the §8.5 leeway.Finding payload for any subject and
// evaluation from this snapshot, regardless of whether it breached or was routed.
func (r *SnapshotResult) FindingFor(sub leeway.SubjectRef, ev *Evaluation, dist *leeway.Distribution) leeway.Finding {
	if r == nil || r.source == nil || ev == nil {
		return leeway.Finding{}
	}
	phase := leeway.PhaseOK
	if ev.Verdict.Breached {
		phase = leeway.PhaseFiring
	}
	f, _ := r.source.findingFor(sub, ev, leeway.AlertState{
		Phase:       phase,
		FirstSeenAt: r.now,
		Since:       r.now,
	}, dist, r.now)
	return f
}

// EvaluateSnapshot runs the topology-drift evaluation pipeline synchronously
// against a point-in-time resource snapshot without starting informers or waiting
// on alert dwell timers.
func EvaluateSnapshot(ctx context.Context, cfg Config, in SnapshotInput) (*SnapshotResult, error) {
	now := in.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cfg.Now = func() time.Time { return now }

	s := New(nil, cfg)

	rsIdx := newNamespaceIndexer()
	for _, rs := range in.ReplicaSets {
		if rs != nil {
			_ = rsIdx.Add(rs)
		}
	}
	podIdx := newNamespaceIndexer()
	for _, pod := range in.Pods {
		if pod != nil {
			_ = podIdx.Add(pod)
		}
	}
	pvcIdx := newNamespaceIndexer()
	for _, pvc := range in.Claims {
		if pvc != nil {
			_ = pvcIdx.Add(pvc)
		}
	}
	pvIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, pv := range in.Volumes {
		if pv != nil {
			_ = pvIdx.Add(pv)
		}
	}

	s.mu.Lock()
	s.replicaSets = appslisters.NewReplicaSetLister(rsIdx)
	s.pods = corelisters.NewPodLister(podIdx)
	s.claims = corelisters.NewPersistentVolumeClaimLister(pvcIdx)
	s.volumes = corelisters.NewPersistentVolumeLister(pvIdx)
	s.mu.Unlock()

	oracle := in.CapacityOracle
	if oracle == nil {
		oracle = BuildCapacityOracle(in.Pods, in.Events, now)
	}
	s.WithCapacityOracle(oracle)

	for _, p := range in.Policies {
		s.onPolicy(p, false)
	}
	for _, cp := range in.ClusterPolicies {
		s.onPolicy(cp, true)
	}
	for _, d := range in.Deployments {
		s.onScalable(d)
	}
	for _, sts := range in.StatefulSets {
		s.onScalable(sts)
	}
	for _, n := range in.Nodes {
		s.onNode(n)
	}

	s.mu.Lock()
	s.armed = true
	s.mu.Unlock()

	for _, p := range in.Pods {
		if p != nil {
			s.state.OnPodAdd(p)
		}
	}

	s.sampleCluster(now)

	subjects := s.state.Subjects()
	sortSubjects(subjects)
	for _, sub := range subjects {
		s.evaluate(ctx, sub)
	}
	s.evaluateNodeGroups(now)

	res := &SnapshotResult{
		Domains:              make(map[leeway.TopologyKey]map[leeway.Domain]DomainStats, len(s.inv.Keys())),
		DomainAvailabilities: make(map[leeway.TopologyKey][]leeway.DomainAvailability),
		NodeGroupsDiscovered: s.nodeGroupsDiscovered(),
		source:               s,
		now:                  now,
	}

	for _, key := range s.inv.Keys() {
		res.Domains[key] = s.inv.Stats(key)
	}

	s.eachKnownDomain(now, func(key leeway.TopologyKey, a leeway.DomainAvailability) {
		res.DomainAvailabilities[key] = append(res.DomainAvailabilities[key], a)
		if a.Unavailable() {
			f := leeway.NewDomainFinding(a, key, leeway.AlertState{
				Phase:       leeway.PhaseFiring,
				FirstSeenAt: now,
				Since:       now,
			})
			res.Findings = append(res.Findings, f)
		}
	})

	groups := s.state.Groups()
	sortSubjects(groups)
	allSubjects := make([]leeway.SubjectRef, 0, len(subjects)+len(groups))
	allSubjects = append(allSubjects, subjects...)
	allSubjects = append(allSubjects, groups...)

	res.Subjects = make([]SubjectEvaluation, 0, len(allSubjects))
	for _, sub := range allSubjects {
		evals := s.state.EvaluationsOf(sub)
		snap := s.state.Snapshot(sub)
		res.Subjects = append(res.Subjects, SubjectEvaluation{
			Subject:       sub,
			Evaluations:   evals,
			Distributions: snap,
		})
		for i := range evals {
			ev := &evals[i]
			if !ev.Verdict.Breached {
				continue
			}
			f, delivery := s.findingFor(sub, ev, leeway.AlertState{
				Phase:       leeway.PhaseFiring,
				FirstSeenAt: now,
				Since:       now,
			}, snap[ev.Key], now)
			if !delivery.Signal {
				continue
			}
			res.Findings = append(res.Findings, f)
		}
	}

	return res, nil
}

// BuildCapacityOracle constructs a CapacityOracle from a point-in-time list of
// pods and events. It inspects PodScheduled=False (Reason=Unschedulable) pod
// conditions and FailedScheduling events to identify pods the scheduler refused
// due to insufficient resources.
func BuildCapacityOracle(pods []*corev1.Pod, events []*corev1.Event, now time.Time) CapacityOracle {
	eventMsg := make(map[types.UID]string)
	for _, ev := range events {
		if ev == nil || ev.Reason != "FailedScheduling" || ev.InvolvedObject.UID == "" {
			continue
		}
		if ev.Message != "" {
			eventMsg[ev.InvolvedObject.UID] = ev.Message
		}
	}

	facts := make(map[types.UID]PendingFact)
	for _, pod := range pods {
		if pod == nil || pod.Spec.NodeName != "" || pod.DeletionTimestamp != nil {
			continue
		}
		if pod.Status.Phase != corev1.PodPending {
			continue
		}
		for _, cond := range pod.Status.Conditions {
			if cond.Type != corev1.PodScheduled || cond.Status != corev1.ConditionFalse || cond.Reason != corev1.PodReasonUnschedulable {
				continue
			}
			msg := cond.Message
			if msg == "" {
				msg = eventMsg[pod.UID]
			}
			since := cond.LastTransitionTime.Time
			if since.IsZero() {
				since = pod.CreationTimestamp.Time
			}
			if since.IsZero() {
				since = now
			}
			facts[pod.UID] = PendingFact{
				Since:                since,
				Message:              msg,
				InsufficientResource: isInsufficientResourceMessage(msg),
			}
			break
		}
	}
	return func() map[types.UID]PendingFact {
		return facts
	}
}

func isInsufficientResourceMessage(msg string) bool {
	lower := strings.ToLower(msg)
	return strings.Contains(lower, "insufficient ") ||
		strings.Contains(lower, "too many pods")
}

func newNamespaceIndexer() cache.Indexer {
	return cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
}

func sortSubjects(subs []leeway.SubjectRef) {
	sort.Slice(subs, func(i, j int) bool {
		return subs[i].String() < subs[j].String()
	})
}
