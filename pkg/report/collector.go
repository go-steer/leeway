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

package report

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/leeway/pkg/clusters"
	"github.com/go-steer/leeway/pkg/leeway"
	"github.com/go-steer/leeway/pkg/sources/computeclass"
	"github.com/go-steer/leeway/pkg/sources/topologydrift"
)

const (
	// DefaultParallelism is the default maximum number of clusters queried concurrently.
	DefaultParallelism = 8
	// DefaultClusterTimeout is the default per-cluster collection timeout.
	DefaultClusterTimeout = 60 * time.Second
)

// Options configures a multi-cluster report collection run.
type Options struct {
	// Parallelism bounds concurrent cluster evaluations (default 8).
	Parallelism int

	// Timeout bounds each individual cluster evaluation (default 60s).
	Timeout time.Duration

	// Namespaces restricts workload/pod listing to the named namespaces. Empty
	// queries all namespaces.
	Namespaces []string

	// Sources selects which leeway sources to evaluate (`topology-drift`,
	// `compute-class`). Empty evaluates both.
	Sources []string

	// TopologyKeys overrides the tracked topology axes. Empty uses
	// topologydrift.DefaultTopologyKeys.
	TopologyKeys []leeway.TopologyKey

	// ClusterDefaultConstraints configures kube-scheduler default spread
	// constraints (nil = assumed system defaults, &[] = none, non-empty = declared).
	ClusterDefaultConstraints *[]corev1.TopologySpreadConstraint

	// TopologyTierC includes Tier C topology-drift findings in TopologyFindings.
	TopologyTierC bool

	// ComputeClassTierC includes Tier C compute-class findings in RankFindings.
	ComputeClassTierC bool

	// All includes all evaluated subject-axes in ClusterReport.Subjects, even
	// when not breaching.
	All bool

	// MinDrift includes subject-axes with Score.Drift >= MinDrift in
	// ClusterReport.Subjects when > 0.
	MinDrift float64

	// Now overrides the evaluation timestamp (defaults to time.Now().UTC()).
	Now time.Time

	// ClientBuilder overrides client construction for testing.
	ClientBuilder func(clusters.Cluster) (kubernetes.Interface, dynamic.Interface, error)
}

// Collect queries all target clusters concurrently, evaluates topology drift and
// compute-class priority ranks on each snapshot, and returns a consolidated Report.
func Collect(ctx context.Context, targets []clusters.Cluster, opts Options) (*Report, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	parallelism := opts.Parallelism
	if parallelism <= 0 {
		parallelism = DefaultParallelism
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultClusterTimeout
	}

	results := make([]ClusterReport, len(targets))
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup

	for i, target := range targets {
		wg.Add(1)
		go func(idx int, c clusters.Cluster) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results[idx] = ClusterReport{
					Cluster: c.Name(),
					Source:  c.Profile,
					Status:  StatusError,
					Error:   ctx.Err().Error(),
				}
				return
			}

			cctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			results[idx] = collectCluster(cctx, c, opts, now)
		}(i, target)
	}
	wg.Wait()

	rep := &Report{
		SchemaVersion: SchemaVersion,
		GeneratedAt:   now,
		Clusters:      results,
		Summary: Summary{
			ClustersTotal: len(results),
			BySeverity:    map[string]int{},
			ByTier:        map[string]int{},
		},
	}

	for _, cr := range results {
		if cr.Status == StatusOK {
			rep.Summary.ClustersSucceeded++
		} else {
			rep.Summary.ClustersFailed++
		}
		rep.Summary.TopologyFindings += len(cr.TopologyFindings)
		rep.Summary.RankFindings += len(cr.RankFindings)
		for _, f := range cr.TopologyFindings {
			if f.Severity != "" {
				rep.Summary.BySeverity[f.Severity]++
			}
			if f.Tier != "" {
				rep.Summary.ByTier[f.Tier]++
			}
		}
		for _, rf := range cr.RankFindings {
			if rf.Severity != "" {
				rep.Summary.BySeverity[rf.Severity]++
			}
			if rf.Tier != "" {
				rep.Summary.ByTier[rf.Tier]++
			}
		}
	}
	if len(rep.Summary.BySeverity) == 0 {
		rep.Summary.BySeverity = nil
	}
	if len(rep.Summary.ByTier) == 0 {
		rep.Summary.ByTier = nil
	}
	return rep, nil
}

func collectCluster(ctx context.Context, target clusters.Cluster, opts Options, now time.Time) ClusterReport {
	cr := ClusterReport{
		Cluster: target.Name(),
		Source:  target.Profile,
		Status:  StatusOK,
	}

	client, dyn, err := buildClients(target, opts)
	if err != nil {
		cr.Status = StatusError
		cr.Error = fmt.Sprintf("build client: %v", err)
		return cr
	}

	runTopo := sourceEnabled(opts.Sources, topologydrift.Name)
	runCC := sourceEnabled(opts.Sources, computeclass.Name)

	nodes, err := listNodes(ctx, client)
	if err != nil {
		cr.Status = StatusError
		cr.Error = fmt.Sprintf("list nodes: %v", err)
		return cr
	}

	namespaces := opts.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{metav1.NamespaceAll}
	}

	pods, err := listPods(ctx, client, namespaces)
	if err != nil {
		cr.Status = StatusError
		cr.Error = fmt.Sprintf("list pods: %v", err)
		return cr
	}

	// Events are best-effort: a forbidden or failed event list must not fail the
	// cluster report.
	events, _ := listEvents(ctx, client, namespaces)

	if runTopo {
		rs, err := listReplicaSets(ctx, client, namespaces)
		if err != nil {
			cr.Status = StatusError
			cr.Error = fmt.Sprintf("list replicasets: %v", err)
			return cr
		}
		deps, _ := listDeployments(ctx, client, namespaces)
		stss, _ := listStatefulSets(ctx, client, namespaces)
		pvcs, _ := listPVCs(ctx, client, namespaces)
		pvs, _ := listPVs(ctx, client)
		policies, clusterPolicies := listPolicies(ctx, client, dyn, namespaces)

		keys := opts.TopologyKeys
		if len(keys) == 0 {
			keys = topologydrift.DefaultTopologyKeys
		}
		tcfg := topologydrift.Config{
			Cluster:                   target.Name(),
			TopologyKeys:              keys,
			ClusterDefaultConstraints: opts.ClusterDefaultConstraints,
			TierCSignals:              opts.TopologyTierC,
			Now:                       func() time.Time { return now },
		}
		snap, err := topologydrift.EvaluateSnapshot(ctx, tcfg, topologydrift.SnapshotInput{
			Nodes:           nodes,
			Pods:            pods,
			ReplicaSets:     rs,
			Deployments:     deps,
			StatefulSets:    stss,
			Claims:          pvcs,
			Volumes:         pvs,
			Policies:        policies,
			ClusterPolicies: clusterPolicies,
			Events:          events,
			Now:             now,
		})
		if err != nil {
			cr.Status = StatusError
			cr.Error = fmt.Sprintf("evaluate topology-drift: %v", err)
			return cr
		}

		if len(snap.Domains) > 0 {
			cr.Domains = make(map[string]map[string]DomainSummary, len(snap.Domains))
			for key, byDomain := range snap.Domains {
				if len(byDomain) == 0 {
					continue
				}
				dm := make(map[string]DomainSummary, len(byDomain))
				for d, st := range byDomain {
					dm[string(d)] = DomainSummary{
						Nodes:               st.Nodes,
						Ready:               st.Ready,
						Usable:              st.Usable,
						AllocatableCPUMilli: st.AllocatableCPUMilli,
						AllocatableMemBytes: st.AllocatableMemBytes,
					}
				}
				cr.Domains[string(key)] = dm
			}
		}
		cr.TopologyFindings = snap.Findings

		if opts.All || opts.MinDrift > 0 {
			for _, sub := range snap.Subjects {
				for i := range sub.Evaluations {
					ev := &sub.Evaluations[i]
					if !opts.All && ev.Scores.Drift < opts.MinDrift {
						continue
					}
					cr.Subjects = append(cr.Subjects, snap.FindingFor(sub.Subject, ev, sub.Distributions[ev.Key]))
				}
			}
		}
	}

	if runCC && dyn != nil && computeclass.ComputeClassesServed(client) {
		classes, err := listComputeClasses(ctx, dyn)
		if err == nil && len(classes) > 0 {
			ccfg := computeclass.DefaultConfig()
			ccfg.Cluster = target.Name()
			ccfg.TierCSignals = opts.ComputeClassTierC
			ccSnap, err := computeclass.EvaluateSnapshot(ccfg, computeclass.SnapshotInput{
				Classes: classes,
				Nodes:   nodes,
				Pods:    pods,
				Events:  events,
				Now:     now,
			})
			if err != nil {
				cr.Status = StatusError
				cr.Error = fmt.Sprintf("evaluate compute-class: %v", err)
				return cr
			}
			cr.ComputeClasses = ccSnap.Classes
			cr.RankFindings = ccSnap.Findings
		}
	}

	return cr
}

func buildClients(target clusters.Cluster, opts Options) (kubernetes.Interface, dynamic.Interface, error) {
	if opts.ClientBuilder != nil {
		return opts.ClientBuilder(target)
	}
	if target.Config == nil {
		return nil, nil, fmt.Errorf("cluster %q has no rest.Config", target.Name())
	}
	client, err := kubernetes.NewForConfig(target.Config)
	if err != nil {
		return nil, nil, err
	}
	dyn, err := dynamic.NewForConfig(target.Config)
	if err != nil {
		return nil, nil, err
	}
	return client, dyn, nil
}

func sourceEnabled(sources []string, name string) bool {
	if len(sources) == 0 {
		return true
	}
	return slices.Contains(sources, name)
}

func listNodes(ctx context.Context, client kubernetes.Interface) ([]*corev1.Node, error) {
	list, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]*corev1.Node, len(list.Items))
	for i := range list.Items {
		out[i] = &list.Items[i]
	}
	return out, nil
}

func listPods(ctx context.Context, client kubernetes.Interface, namespaces []string) ([]*corev1.Pod, error) {
	var out []*corev1.Pod
	for _, ns := range namespaces {
		list, err := client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		for i := range list.Items {
			out = append(out, &list.Items[i])
		}
	}
	return out, nil
}

func listReplicaSets(ctx context.Context, client kubernetes.Interface, namespaces []string) ([]*appsv1.ReplicaSet, error) {
	var out []*appsv1.ReplicaSet
	for _, ns := range namespaces {
		list, err := client.AppsV1().ReplicaSets(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		for i := range list.Items {
			out = append(out, &list.Items[i])
		}
	}
	return out, nil
}

func listDeployments(ctx context.Context, client kubernetes.Interface, namespaces []string) ([]*appsv1.Deployment, error) {
	var out []*appsv1.Deployment
	for _, ns := range namespaces {
		list, err := client.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		for i := range list.Items {
			out = append(out, &list.Items[i])
		}
	}
	return out, nil
}

func listStatefulSets(ctx context.Context, client kubernetes.Interface, namespaces []string) ([]*appsv1.StatefulSet, error) {
	var out []*appsv1.StatefulSet
	for _, ns := range namespaces {
		list, err := client.AppsV1().StatefulSets(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		for i := range list.Items {
			out = append(out, &list.Items[i])
		}
	}
	return out, nil
}

func listPVCs(ctx context.Context, client kubernetes.Interface, namespaces []string) ([]*corev1.PersistentVolumeClaim, error) {
	var out []*corev1.PersistentVolumeClaim
	for _, ns := range namespaces {
		list, err := client.CoreV1().PersistentVolumeClaims(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		for i := range list.Items {
			out = append(out, &list.Items[i])
		}
	}
	return out, nil
}

func listPVs(ctx context.Context, client kubernetes.Interface) ([]*corev1.PersistentVolume, error) {
	list, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]*corev1.PersistentVolume, len(list.Items))
	for i := range list.Items {
		out[i] = &list.Items[i]
	}
	return out, nil
}

func listEvents(ctx context.Context, client kubernetes.Interface, namespaces []string) ([]*corev1.Event, error) {
	var out []*corev1.Event
	for _, ns := range namespaces {
		list, err := client.CoreV1().Events(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		for i := range list.Items {
			out = append(out, &list.Items[i])
		}
	}
	return out, nil
}

func listPolicies(ctx context.Context, client kubernetes.Interface, dyn dynamic.Interface, namespaces []string) (policies, clusterPolicies []*unstructured.Unstructured) {
	if dyn == nil {
		return nil, nil
	}
	namespaced, clusterScoped := topologydrift.PoliciesServed(client)
	if namespaced {
		for _, ns := range namespaces {
			list, err := dyn.Resource(topologydrift.PolicyGVR()).Namespace(ns).List(ctx, metav1.ListOptions{})
			if err != nil {
				continue
			}
			for i := range list.Items {
				policies = append(policies, &list.Items[i])
			}
		}
	}
	if clusterScoped && slices.Contains(namespaces, metav1.NamespaceAll) {
		if list, err := dyn.Resource(topologydrift.ClusterPolicyGVR()).List(ctx, metav1.ListOptions{}); err == nil {
			for i := range list.Items {
				clusterPolicies = append(clusterPolicies, &list.Items[i])
			}
		}
	}
	return policies, clusterPolicies
}

func listComputeClasses(ctx context.Context, dyn dynamic.Interface) ([]*unstructured.Unstructured, error) {
	list, err := dyn.Resource(computeclass.ComputeClassGVR()).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]*unstructured.Unstructured, len(list.Items))
	for i := range list.Items {
		out[i] = &list.Items[i]
	}
	return out, nil
}
