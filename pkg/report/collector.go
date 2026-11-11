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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/go-steer/leeway/pkg/clusters"
	"github.com/go-steer/leeway/pkg/kube"
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

const (
	// rawPageLimit is the page size of a `kubectl get --raw` list read. A pod
	// runs to about 13-17 KB of JSON, so a page of 250 stays near 4 MiB, under
	// the credential proxy's 8 MiB output cap.
	rawPageLimit = 250
	// minRawPageLimit is the smallest page a truncated read is retried at.
	minRawPageLimit = 1

	coreV1Prefix = "/api/v1"
	appsV1Prefix = "/apis/apps/v1"

	podEventsFieldSelector = "involvedObject.kind=Pod"
	proxyTruncatedMarker   = "credential proxy output truncated"
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

	// DomainUnavailableKeys overrides the topology axes on which a domain with
	// no usable node raises leeway.domain_unavailable. Nil uses
	// topologydrift.DefaultDomainUnavailableKeys; a non-nil empty slice disables
	// the detector.
	DomainUnavailableKeys []leeway.TopologyKey

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

	runTopo := sourceEnabled(opts.Sources, topologydrift.Name)
	runCC := sourceEnabled(opts.Sources, computeclass.Name)

	namespaces := opts.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{metav1.NamespaceAll}
	}

	var (
		nodes           []*corev1.Node
		pods            []*corev1.Pod
		events          []*corev1.Event
		rs              []*appsv1.ReplicaSet
		deps            []*appsv1.Deployment
		stss            []*appsv1.StatefulSet
		pvcs            []*corev1.PersistentVolumeClaim
		pvs             []*corev1.PersistentVolume
		policies        []*unstructured.Unstructured
		clusterPolicies []*unstructured.Unstructured
		classes         []*unstructured.Unstructured
		ccServed        bool
	)

	if opts.ClientBuilder == nil && target.Config == nil {
		snapData, err := fetchViaKubectl(ctx, target, namespaces, runTopo, runCC)
		if err != nil {
			cr.Status = StatusError
			cr.Error = err.Error()
			return cr
		}
		nodes = snapData.nodes
		pods = snapData.pods
		events = snapData.events
		rs = snapData.rs
		deps = snapData.deps
		stss = snapData.stss
		pvcs = snapData.pvcs
		pvs = snapData.pvs
		policies = snapData.policies
		clusterPolicies = snapData.clusterPolicies
		classes = snapData.classes
		ccServed = snapData.ccServed
	} else {
		client, dyn, err := buildClients(target, opts)
		if err != nil {
			cr.Status = StatusError
			cr.Error = fmt.Sprintf("build client: %v", err)
			return cr
		}

		nodes, err = listNodes(ctx, client)
		if err != nil {
			cr.Status = StatusError
			cr.Error = fmt.Sprintf("list nodes: %v", err)
			return cr
		}

		pods, err = listPods(ctx, client, namespaces)
		if err != nil {
			cr.Status = StatusError
			cr.Error = fmt.Sprintf("list pods: %v", err)
			return cr
		}

		// Events are best-effort: a forbidden or failed event list must not fail the
		// cluster report.
		events, _ = listEvents(ctx, client, namespaces)

		if runTopo {
			rs, err = listReplicaSets(ctx, client, namespaces)
			if err != nil {
				cr.Status = StatusError
				cr.Error = fmt.Sprintf("list replicasets: %v", err)
				return cr
			}
			deps, _ = listDeployments(ctx, client, namespaces)
			stss, _ = listStatefulSets(ctx, client, namespaces)
			pvcs, _ = listPVCs(ctx, client, namespaces)
			pvs, _ = listPVs(ctx, client)
			policies, clusterPolicies = listPolicies(ctx, client, dyn, namespaces)
		}

		if runCC && dyn != nil && computeclass.ComputeClassesServed(client) {
			if list, err := listComputeClasses(ctx, dyn); err == nil && len(list) > 0 {
				classes = list
				ccServed = true
			}
		}
	}

	if runTopo {

		keys := opts.TopologyKeys
		if len(keys) == 0 {
			keys = topologydrift.DefaultTopologyKeys
		}
		tcfg := topologydrift.Config{
			Cluster:                   target.Name(),
			TopologyKeys:              keys,
			DomainUnavailableKeys:     opts.DomainUnavailableKeys,
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

	if runCC && ccServed {
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
		n := &list.Items[i]
		_, _ = kube.Transform(n)
		out[i] = n
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
			p := &list.Items[i]
			_, _ = kube.Transform(p)
			out = append(out, p)
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
			rs := &list.Items[i]
			_, _ = kube.Transform(rs)
			out = append(out, rs)
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
			d := &list.Items[i]
			_, _ = kube.Transform(d)
			out = append(out, d)
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
			sts := &list.Items[i]
			_, _ = kube.Transform(sts)
			out = append(out, sts)
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

type kubectlSnapshot struct {
	nodes           []*corev1.Node
	pods            []*corev1.Pod
	events          []*corev1.Event
	rs              []*appsv1.ReplicaSet
	deps            []*appsv1.Deployment
	stss            []*appsv1.StatefulSet
	pvcs            []*corev1.PersistentVolumeClaim
	pvs             []*corev1.PersistentVolume
	policies        []*unstructured.Unstructured
	clusterPolicies []*unstructured.Unstructured
	classes         []*unstructured.Unstructured
	ccServed        bool
}

// fetchViaKubectl reads a cluster snapshot through the kubectl binary. Every
// built-in collection is paged through `kubectl get --raw` (see
// kubectlListPaged), so neither kubectl nor leeway ever holds a whole
// cluster's list at once and each page is trimmed by kube.Transform before
// the next is read. The LeewayPolicy and ComputeClass CRDs, which are small,
// keep the typed `kubectl get -o json` read.
func fetchViaKubectl(ctx context.Context, target clusters.Cluster, namespaces []string, runTopo, runCC bool) (*kubectlSnapshot, error) {
	var baseArgs []string
	if strings.TrimSpace(target.Kubeconfig) != "" {
		baseArgs = append(baseArgs, "--kubeconfig="+strings.TrimSpace(target.Kubeconfig))
	}
	if strings.TrimSpace(target.Context) != "" {
		baseArgs = append(baseArgs, "--context="+strings.TrimSpace(target.Context))
	}

	var out kubectlSnapshot
	var fallbackNS namespaceFallbackCache

	nodes, err := kubectlListPaged(ctx, baseArgs, rawNodes, metav1.NamespaceAll, "", trimmed[corev1.Node])
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	out.nodes = nodes

	policyResource := topologydrift.PolicyGVR().Resource + "." + topologydrift.PolicyGroup
	clusterPolicyResource := topologydrift.ClusterPolicyGVR().Resource + "." + topologydrift.PolicyGroup

	for _, ns := range namespaces {
		pods, err := kubectlListPaged(ctx, baseArgs, rawPods, ns, "", trimmed[corev1.Pod])
		if err != nil {
			return nil, fmt.Errorf("list pods: %w", err)
		}
		out.pods = append(out.pods, pods...)

		// Best-effort lists: an error drops that list for this namespace and
		// keeps nothing from its earlier pages.
		if events, err := kubectlListPaged(ctx, baseArgs, rawEvents, ns, podEventsFieldSelector, kept[corev1.Event]); err == nil {
			out.events = append(out.events, events...)
		}

		if runTopo {
			rs, err := kubectlListPaged(ctx, baseArgs, rawReplicaSets, ns, "", trimmed[appsv1.ReplicaSet])
			if err != nil {
				return nil, fmt.Errorf("list replicasets: %w", err)
			}
			out.rs = append(out.rs, rs...)

			if deps, err := kubectlListPaged(ctx, baseArgs, rawDeployments, ns, "", trimmed[appsv1.Deployment]); err == nil {
				out.deps = append(out.deps, deps...)
			}
			if stss, err := kubectlListPaged(ctx, baseArgs, rawStatefulSets, ns, "", trimmed[appsv1.StatefulSet]); err == nil {
				out.stss = append(out.stss, stss...)
			}
			if pvcs, err := kubectlListPaged(ctx, baseArgs, rawPVCs, ns, "", kept[corev1.PersistentVolumeClaim]); err == nil {
				out.pvcs = append(out.pvcs, pvcs...)
			}

			_ = kubectlGetNamespacedJSON(ctx, baseArgs, policyResource, ns, nil, true, &fallbackNS, func(polList *unstructured.UnstructuredList) {
				for i := range polList.Items {
					out.policies = append(out.policies, &polList.Items[i])
				}
			})
		}
	}

	if runTopo {
		if pvs, err := kubectlListPaged(ctx, baseArgs, rawPVs, metav1.NamespaceAll, "", kept[corev1.PersistentVolume]); err == nil {
			out.pvs = pvs
		}

		if slices.Contains(namespaces, metav1.NamespaceAll) {
			var cpolList unstructured.UnstructuredList
			if err := kubectlGetJSON(ctx, baseArgs, clusterPolicyResource, "", false, nil, &cpolList); err == nil {
				for i := range cpolList.Items {
					out.clusterPolicies = append(out.clusterPolicies, &cpolList.Items[i])
				}
			}
		}
	}

	if runCC {
		var ccList unstructured.UnstructuredList
		if err := kubectlGetJSON(ctx, baseArgs, "computeclasses.cloud.google.com", "", false, nil, &ccList); err == nil {
			out.ccServed = true
			for i := range ccList.Items {
				out.classes = append(out.classes, &ccList.Items[i])
			}
		}
	}

	return &out, nil
}

// rawCollection is a built-in list endpoint `kubectl get --raw` can page.
type rawCollection struct {
	prefix     string
	resource   string
	namespaced bool
}

var (
	rawNodes        = rawCollection{coreV1Prefix, "nodes", false}
	rawPods         = rawCollection{coreV1Prefix, "pods", true}
	rawEvents       = rawCollection{coreV1Prefix, "events", true}
	rawPVCs         = rawCollection{coreV1Prefix, "persistentvolumeclaims", true}
	rawPVs          = rawCollection{coreV1Prefix, "persistentvolumes", false}
	rawReplicaSets  = rawCollection{appsV1Prefix, "replicasets", true}
	rawDeployments  = rawCollection{appsV1Prefix, "deployments", true}
	rawStatefulSets = rawCollection{appsV1Prefix, "statefulsets", true}
)

// path is the collection's URL path, scoped to ns when the kind is namespaced
// and ns is not NamespaceAll.
func (c rawCollection) path(ns string) string {
	if c.namespaced && ns != metav1.NamespaceAll {
		return c.prefix + "/namespaces/" + url.PathEscape(ns) + "/" + c.resource
	}
	return c.prefix + "/" + c.resource
}

// rawListPage is the part of a list response a paged read needs.
type rawListPage[T any] struct {
	Metadata metav1.ListMeta `json:"metadata"`
	Items    []T             `json:"items"`
}

// trimmed keeps an item after kube.Transform has dropped the fields no
// evaluation reads.
func trimmed[T any](item *T) *T {
	_, _ = kube.Transform(item)
	return item
}

// kept keeps an item as decoded, for kinds kube.Transform does not trim.
func kept[T any](item *T) *T {
	return item
}

// kubectlListPaged lists collection c one page at a time through
// `kubectl get --raw <path>?limit=N&continue=...` and returns every item
// after keep has seen it.
//
// A single `kubectl get -A -o json` makes kubectl decode and re-encode the
// whole list, and makes leeway buffer and decode all of it, before
// kube.Transform can trim one object: on a 23k-pod cluster that is several
// GiB, which takes down a 2 GiB sandbox. `--raw` passes each page's bytes
// through untouched, and each page is decoded and trimmed before the next is
// read, so peak memory is the trimmed snapshot plus one page.
//
// A page that comes back truncated (the credential proxy's marker, or JSON
// that ends early) is read again at half the size from the same continue
// token. Any other error is returned, with nothing kept from earlier pages.
func kubectlListPaged[T any](
	ctx context.Context,
	baseArgs []string,
	c rawCollection,
	ns string,
	fieldSelector string,
	keep func(*T) *T,
) ([]*T, error) {
	var out []*T
	limit := rawPageLimit
	continueToken := ""
	for {
		query := url.Values{}
		query.Set("limit", strconv.Itoa(limit))
		if continueToken != "" {
			query.Set("continue", continueToken)
		}
		if fieldSelector != "" {
			query.Set("fieldSelector", fieldSelector)
		}
		args := slices.Concat(baseArgs, []string{"get", "--raw", c.path(ns) + "?" + query.Encode()})

		stdout, err := runKubectl(ctx, args, c.resource)
		var page rawListPage[T]
		if err == nil {
			if decodeErr := json.Unmarshal(stdout, &page); decodeErr != nil {
				err = fmt.Errorf("decode kubectl get --raw %s: %w", c.resource, decodeErr)
			}
		}
		if err != nil {
			if isKubectlOutputTruncated(err) && limit > minRawPageLimit {
				limit = max(limit/2, minRawPageLimit)
				continue
			}
			return nil, err
		}

		for i := range page.Items {
			out = append(out, keep(&page.Items[i]))
		}
		continueToken = page.Metadata.Continue
		if continueToken == "" {
			return out, nil
		}
	}
}

type namespaceFallbackCache struct {
	loaded bool
	names  []string
}

var errKubectlOutputTruncated = errors.New("kubectl output truncated")

// kubectlGetNamespacedJSON fetches a namespaced resource list via `kubectl get`,
// using `-A` when ns is NamespaceAll and automatically falling back to
// per-namespace queries if an intermediary (such as credential-proxy's 8 MiB
// output cap) truncates the all-namespaces JSON payload.
func kubectlGetNamespacedJSON[L any](
	ctx context.Context,
	baseArgs []string,
	resource string,
	ns string,
	extraArgs []string,
	bestEffort bool,
	cachedNamespaces *namespaceFallbackCache,
	consume func(*L),
) error {
	allNS := ns == metav1.NamespaceAll
	var list L
	err := kubectlGetJSON(ctx, baseArgs, resource, ns, allNS, extraArgs, &list)
	if err == nil {
		consume(&list)
		return nil
	}
	if !allNS || !isKubectlOutputTruncated(err) {
		return err
	}
	if !cachedNamespaces.loaded {
		var nsList corev1.NamespaceList
		if nsErr := kubectlGetJSON(ctx, baseArgs, "namespaces", "", false, nil, &nsList); nsErr != nil {
			return fmt.Errorf("%w (namespace fallback failed: %v)", err, nsErr)
		}
		names := make([]string, 0, len(nsList.Items))
		for i := range nsList.Items {
			if name := strings.TrimSpace(nsList.Items[i].Name); name != "" {
				names = append(names, name)
			}
		}
		slices.Sort(names)
		cachedNamespaces.names = names
		cachedNamespaces.loaded = true
	}
	for _, singleNS := range cachedNamespaces.names {
		var nsItems L
		if nsErr := kubectlGetJSON(ctx, baseArgs, resource, singleNS, false, extraArgs, &nsItems); nsErr != nil {
			if isKubectlNotFound(nsErr) || bestEffort {
				continue
			}
			return nsErr
		}
		consume(&nsItems)
	}
	return nil
}

func isKubectlNotFound(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "(notfound)") || strings.Contains(lower, "not found")
}

func isKubectlOutputTruncated(err error) bool {
	if err == nil {
		return false
	}
	var syntaxErr *json.SyntaxError
	return errors.Is(err, errKubectlOutputTruncated) ||
		errors.As(err, &syntaxErr) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}

func kubectlGetJSON(ctx context.Context, baseArgs []string, resource, ns string, allNamespaces bool, extraArgs []string, into any) error {
	args := make([]string, 0, len(baseArgs)+len(extraArgs)+5)
	args = append(args, baseArgs...)
	args = append(args, "get", resource)
	if allNamespaces {
		args = append(args, "-A")
	} else if ns != "" {
		args = append(args, "-n", ns)
	}
	args = append(args, extraArgs...)
	args = append(args, "-o", "json")

	stdout, err := runKubectl(ctx, args, resource)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(stdout, into); err != nil {
		return fmt.Errorf("decode kubectl get %s: %w", resource, err)
	}
	return nil
}

// runKubectl runs kubectl with args and returns its stdout, or an error
// wrapping errKubectlOutputTruncated when the credential proxy reports that
// it cut the output.
func runKubectl(ctx context.Context, args []string, resource string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "kubectl", args...) // #nosec G204 -- args are fixed resource nouns or collection paths and validated kubeconfig/context flags
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if strings.Contains(stderr.String(), proxyTruncatedMarker) {
			return nil, fmt.Errorf("kubectl get %s: %w", resource, errKubectlOutputTruncated)
		}
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, fmt.Errorf("kubectl get %s: %s", resource, msg)
		}
		return nil, fmt.Errorf("kubectl get %s: %w", resource, err)
	}
	if strings.Contains(stderr.String(), proxyTruncatedMarker) {
		return nil, fmt.Errorf("kubectl get %s: %w", resource, errKubectlOutputTruncated)
	}
	return stdout.Bytes(), nil
}
