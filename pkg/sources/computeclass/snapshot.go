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

package computeclass

import (
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/go-steer/leeway/pkg/leeway"
)

// ComputeClassGVR returns the GroupVersionResource for GKE ComputeClass objects.
func ComputeClassGVR() schema.GroupVersionResource { return computeClassGVR }

// SnapshotInput holds the point-in-time cluster resources for a one-shot
// compute-class evaluation (`leeway report`).
type SnapshotInput struct {
	Classes []*unstructured.Unstructured
	Nodes   []*corev1.Node
	Pods    []*corev1.Pod
	Events  []*corev1.Event
	Now     time.Time
}

// ClassSummary summarizes the current occupancy and pending state of one
// ComputeClass in a point-in-time snapshot.
type ClassSummary struct {
	Name                string         `json:"name"`
	Scorable            bool           `json:"scorable"`
	Ordering            string         `json:"ordering,omitempty"`
	PodsByRank          map[string]int `json:"podsByRank,omitempty"`
	PendingPods         int            `json:"pendingPods"`
	PendingProvisioning int            `json:"pendingProvisioning"`
	PendingDeclined     int            `json:"pendingDeclined"`
}

// SnapshotResult is the complete output of EvaluateSnapshot for one cluster.
type SnapshotResult struct {
	Classes  []ClassSummary       `json:"classes,omitempty"`
	Findings []leeway.RankFinding `json:"findings,omitempty"`
}

// EvaluateSnapshot runs the compute-class rank evaluation synchronously against
// a point-in-time resource snapshot without starting informers or waiting on
// alert dwell timers.
func EvaluateSnapshot(cfg Config, in SnapshotInput) (*SnapshotResult, error) {
	now := in.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}

	s, err := New(nil, nil, cfg)
	if err != nil {
		return nil, err
	}

	t0 := now.Add(-s.cfg.Window)
	s.now = func() time.Time { return t0 }
	for _, c := range in.Classes {
		if c != nil {
			s.onClass(c)
		}
	}
	for _, n := range in.Nodes {
		if n != nil {
			s.onNode(n)
		}
	}
	for _, ev := range in.Events {
		if ev != nil {
			s.onEvent(ev)
		}
	}
	for _, p := range in.Pods {
		if p != nil {
			s.onPod(p)
		}
	}

	s.mu.Lock()
	s.synced = true
	s.mu.Unlock()
	s.reconcile(t0)

	// Sample at t0 to anchor the start of the window, then flush and judge at
	// now so that current pod placements accrue a full window for share rules.
	_ = s.judgeAll(t0)
	s.now = func() time.Time { return now }
	judgements := s.judgeAll(now)

	sort.Slice(judgements, func(i, j int) bool {
		return judgements[i].axis.Name < judgements[j].axis.Name
	})

	res := &SnapshotResult{
		Classes: make([]ClassSummary, 0, len(judgements)),
	}
	for i := range judgements {
		j := &judgements[i]
		podsByRank := make(map[string]int, len(j.window.Pods))
		for rank, count := range j.window.Pods {
			if count > 0 {
				podsByRank[rank.String()] = count
			}
		}

		s.mu.Lock()
		pending := len(s.pendingByClass[j.axis.Name])
		provisioning, declined := s.scaleUpSplit(j.axis.Name)
		s.mu.Unlock()

		res.Classes = append(res.Classes, ClassSummary{
			Name:                j.axis.Name,
			Scorable:            j.class.Scorable(),
			Ordering:            j.class.Axis.Ordering.String(),
			PodsByRank:          podsByRank,
			PendingPods:         pending,
			PendingProvisioning: provisioning,
			PendingDeclined:     declined,
		})

		for _, v := range j.verdicts {
			f, delivery := s.findingFor(j, v, leeway.AlertState{
				Phase:       leeway.PhaseFiring,
				FirstSeenAt: now,
				Since:       now,
			})
			if !delivery.Signal {
				continue
			}
			res.Findings = append(res.Findings, f)
		}
	}

	return res, nil
}
