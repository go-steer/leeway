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
	if factor, ok := selectorConfinedFactor(r.source, sub, ev, dist); ok {
		return buildSelectorConfinedFinding(r.source, sub, ev, dist, factor, r.now, r.source.cfg.TierCSignals)
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
	if cfg.Thresholds == nil {
		t := leeway.ReportThresholds()
		cfg.Thresholds = &t
	}

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
				if s.cfg.TierCSignals {
					if factor, ok := selectorConfinedFactor(s, sub, ev, snap[ev.Key]); ok {
						res.Findings = append(res.Findings, buildSelectorConfinedFinding(s, sub, ev, snap[ev.Key], factor, now, true))
					}
				}
				continue
			}
			f, delivery := s.findingFor(sub, ev, leeway.AlertState{
				Phase:       leeway.PhaseFiring,
				FirstSeenAt: now,
				Since:       now,
			}, snap[ev.Key], now)
			if !delivery.Signal && (sub.Kind == leeway.SubjectNodeGroup || f.SuspectedCause == leeway.CauseUnknown) {
				continue
			}
			res.Findings = append(res.Findings, f)
		}
	}

	return res, nil
}

// selectorConfinedFactor reports whether a non-breaching workload evaluation on
// a failure-domain axis is confined to a single domain by its own nodeSelector
// or required nodeAffinity while the cluster has two or more usable domains.
func selectorConfinedFactor(s *Source, sub leeway.SubjectRef, ev *Evaluation, dist *leeway.Distribution) (string, bool) {
	if s == nil || ev == nil || ev.Verdict.Breached {
		return "", false
	}
	switch sub.Kind {
	case leeway.SubjectDaemonSet, leeway.SubjectNodeGroup, leeway.SubjectDomain, leeway.SubjectPreferenceAxis, "":
		return "", false
	}
	if !isSelectorConfinedAxis(ev.Key, s.cfg.DomainUnavailableKeys) {
		return "", false
	}
	if ev.Intent != nil && (ev.Intent.Mode == leeway.ModeColocate || ev.Intent.Mode == leeway.ModeIgnore) {
		return "", false
	}
	if s.cfg.Thresholds != nil && ev.Scores.Total < s.cfg.Thresholds.MinReplicasForScoring {
		return "", false
	}
	if realUsableDomains(ev.Eligible) != 1 || len(ev.Eligible.Domains) == 0 || ev.Eligible.Domains[0] == leeway.DomainUnknown {
		return "", false
	}
	if len(ev.Scores.Actual) == 0 || ev.Scores.Actual[0] != ev.Scores.Total {
		return "", false
	}
	if dist != nil && runningCount(dist) != ev.Scores.Actual[0] {
		return "", false
	}
	if ev.Intent.EligibilityPolicies().NodeAffinityPolicy != corev1.NodeInclusionPolicyHonor {
		return "", false
	}

	pod := s.subjectPod(sub)
	if pod == nil {
		return "", false
	}
	c := ConstraintsOf(pod)
	stripped, changed := c.withoutSelectorAndAffinity()
	if !changed {
		return "", false
	}
	opts := leeway.DefaultEligibilityOptions()
	opts.Policies = ev.Intent.EligibilityPolicies()
	opts.Policies.NodeTaintsPolicy = corev1.NodeInclusionPolicyHonor
	unconstrained := leeway.EligibleDomains(s.inv.NodeViews(ev.Key, stripped), opts)
	usable := realUsableDomains(unconstrained)
	if usable < 2 {
		return "", false
	}
	return fmt.Sprintf("%s confines placement to 1 of %d usable domains on %s", c.selectorDescription(), usable, ev.Key), true
}

func runningCount(dist *leeway.Distribution) int64 {
	if dist == nil {
		return 0
	}
	var total int64
	for _, c := range dist.ByDomain {
		if c != nil {
			total += c.Running
		}
	}
	return total
}

func isSelectorConfinedAxis(key leeway.TopologyKey, configured []leeway.TopologyKey) bool {
	if len(configured) > 0 {
		return slices.Contains(configured, key)
	}
	return key == corev1.LabelTopologyZone || key == corev1.LabelTopologyRegion
}

func realUsableDomains(el leeway.Eligibility) int {
	limit := len(el.Domains) - el.SyntheticDomains
	if limit < 0 {
		limit = 0
	}
	count := 0
	for _, d := range el.Domains[:limit] {
		if d != leeway.DomainUnknown {
			count++
		}
	}
	return count
}

func buildSelectorConfinedFinding(s *Source, sub leeway.SubjectRef, ev *Evaluation, dist *leeway.Distribution, factor string, now time.Time, signal bool) leeway.Finding {
	phase := leeway.PhaseOK
	if signal {
		phase = leeway.PhaseFiring
	}
	f, _ := s.findingFor(sub, ev, leeway.AlertState{
		Phase:       phase,
		FirstSeenAt: now,
		Since:       now,
	}, dist, now)
	f.SuspectedCause = leeway.CauseSelectorConfined
	f.UntestedCauses = nil
	f.ContributingFactors = append([]string{factor}, f.ContributingFactors...)
	if f.Intent != nil {
		f.Intent.Evidence = append(f.Intent.Evidence, factor)
	}
	if signal {
		f.Kind = leeway.KindPlacementDrift
		f.Tier = leeway.TierC.String()
		f.Severity = "info"
		if ev.Intent != nil && ev.Intent.Severity != "" {
			f.Severity = ev.Intent.Severity
			f.SeverityFromPolicy = true
		}
		f.Remediation = leeway.SuggestRemediation(f, s.selectorLabelsFor(sub, ev.Key))
	}
	return f
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
