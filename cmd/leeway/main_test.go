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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/metric/noop"
	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/go-steer/leeway/pkg/engine"
	"github.com/go-steer/leeway/pkg/inject"
	"github.com/go-steer/leeway/pkg/leeway"
	"github.com/go-steer/leeway/pkg/report"
	"github.com/go-steer/leeway/pkg/sources"
)

func TestBuildSourcesForClients_SharesSingleTransformingFactory(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "api-1", Namespace: "prod"},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{{
				Name: "app",
				Env:  []corev1.EnvVar{{Name: "DB_PASS", Value: "super-secret"}},
			}},
		},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-1",
			Labels: map[string]string{"topology.kubernetes.io/zone": "us-central1-a"},
		},
	}
	client := fake.NewSimpleClientset(pod, node)
	client.Resources = []*metav1.APIResourceList{{
		GroupVersion: "cloud.google.com/v1",
		APIResources: []metav1.APIResource{{
			Name:       "computeclasses",
			Kind:       "ComputeClass",
			Namespaced: false,
		}},
	}}

	var podWatches, nodeWatches atomic.Int64
	client.PrependWatchReactor("*", func(action ktesting.Action) (bool, watch.Interface, error) {
		switch action.GetResource().Resource {
		case "pods":
			podWatches.Add(1)
		case "nodes":
			nodeWatches.Add(1)
		}
		return false, nil, nil
	})

	scheme := runtime.NewScheme()
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		{Group: "cloud.google.com", Version: "v1", Resource: "computeclasses"}: "ComputeClassList",
	})

	var stderr bytes.Buffer
	srcs, stopFactory, err := buildSourcesForClients(options{
		sources:      strings.Join(defaultSources, ","),
		topologyKeys: strings.Join(defaultTopologyKeys(), ","),
	}, client, dyn, noop.NewMeterProvider(), &stderr)
	if err != nil {
		t.Fatalf("buildSourcesForClients: %v", err)
	}
	defer stopFactory()

	if len(srcs) != 2 {
		t.Fatalf("built %d sources, want 2 (stderr: %s)", len(srcs), stderr.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = sources.RunAll(ctx, srcs, func(sources.Signal) {})

	if got := podWatches.Load(); got != 1 {
		t.Errorf("pod watch streams opened = %d, want 1 (both sources must share one transforming factory)", got)
	}
	if got := nodeWatches.Load(); got != 1 {
		t.Errorf("node watch streams opened = %d, want 1 (both sources must share one transforming factory)", got)
	}
}

func TestProbeSources_FailsLoudlyOnMissingRBAC(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "selfsubjectaccessreviews", func(action ktesting.Action) (bool, runtime.Object, error) {
		create := action.(ktesting.CreateAction)
		ssar := create.GetObject().(*authorizationv1.SelfSubjectAccessReview)
		attr := ssar.Spec.ResourceAttributes
		allowed := attr.Resource != "pods" || attr.Verb != "watch"
		ssar.Status = authorizationv1.SubjectAccessReviewStatus{
			Allowed: allowed,
			Reason:  "RBAC denied in test",
		}
		return true, ssar, nil
	})

	scheme := runtime.NewScheme()
	dyn := dynamicfake.NewSimpleDynamicClient(scheme)

	var stderr bytes.Buffer
	srcs, stopFactory, err := buildSourcesForClients(options{
		sources:      "topology-drift",
		topologyKeys: strings.Join(defaultTopologyKeys(), ","),
	}, client, dyn, noop.NewMeterProvider(), &stderr)
	if err != nil {
		t.Fatalf("buildSourcesForClients: %v", err)
	}
	defer stopFactory()

	err = probeSources(context.Background(), client, srcs, &stderr)
	if !errors.Is(err, sources.ErrAccessDenied) {
		t.Fatalf("probeSources error = %v, want errors.Is(_, sources.ErrAccessDenied)", err)
	}
}

func TestRealMain_FlagValidation(t *testing.T) {
	dir := t.TempDir()
	outFile, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	defer outFile.Close()
	errFile, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	defer errFile.Close()

	if code := realMain([]string{"--version"}, outFile, errFile); code != 0 {
		t.Errorf("--version exit code = %d, want 0", code)
	}
	if code := realMain([]string{"version"}, outFile, errFile); code != 0 {
		t.Errorf("version subcommand exit code = %d, want 0", code)
	}
	for _, args := range [][]string{
		{"--kube-qps=0"},
		{"--kube-burst=-1"},
		{"--access-recheck=-1s"},
		{"watch", "--kube-qps=0"},
		{"report", "--parallelism=0"},
		{"report", "--timeout=0s"},
	} {
		if code := realMain(args, outFile, errFile); code != 2 {
			t.Errorf("realMain(%v) exit code = %d, want 2", args, code)
		}
	}
}

type fakeClearanceObserver struct {
	mu      sync.Mutex
	cleared bool
}

func (f *fakeClearanceObserver) Clearance(inc engine.Incident) (engine.Clearance, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.cleared {
		return engine.Clearance{Cleared: false}, true
	}
	return engine.Clearance{
		Cleared:    true,
		Resolution: engine.ResolutionRecovered,
	}, true
}

func TestEmitter_WebhookIncidentAndResolution(t *testing.T) {
	type webhookReq struct {
		path string
		body []byte
	}
	var (
		mu   sync.Mutex
		reqs []webhookReq
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		reqs = append(reqs, webhookReq{path: r.URL.Path, body: body})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"inc-42"}`))
	}))
	defer srv.Close()

	sink, err := inject.NewWebhookSink(inject.WebhookConfig{
		URL:        srv.URL,
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatalf("NewWebhookSink: %v", err)
	}

	var stderr bytes.Buffer
	em, err := newEmitter(prometheus.NewRegistry(), &stderr, emitterConfig{
		Cluster:           "gke-prod-us-east1",
		Project:           "acme-prod",
		Region:            "us-east1",
		Sink:              sink,
		DedupWindow:       10 * time.Minute,
		RecoveryStableFor: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("newEmitter: %v", err)
	}

	obs := &fakeClearanceObserver{}
	em.recovery.AddObserver(obs)

	now := time.Now().Add(-time.Minute).UTC()
	finding := &leeway.Finding{
		Kind:           leeway.KindContractViolated,
		Tier:           leeway.TierA.String(),
		Severity:       "critical",
		Subject:        leeway.SubjectRef{Kind: leeway.SubjectDeployment, Namespace: "prod", Name: "checkout"},
		TopologyKey:    "topology.kubernetes.io/zone",
		SuspectedCause: leeway.CauseCapacityShortfall,
	}

	sig := sources.Signal{
		Kind:          leeway.KindContractViolated,
		Source:        engine.SourceSentinel,
		Severity:      engine.SeverityCritical,
		LeewayFinding: finding,
		TriageEvent: engine.TriageEvent{
			Key:          engine.EventKey{UID: "leeway:Deployment/prod/checkout|topology.kubernetes.io/zone", Reason: string(leeway.CauseCapacityShortfall)},
			Namespace:    "prod",
			KindOfObject: "Deployment",
			Name:         "checkout",
			Message:      "topology contract violated on topology.kubernetes.io/zone",
			FirstSeen:    now,
			LastSeen:     now,
			Count:        1,
		},
	}

	// 1. Emit initial incident -> should POST /incidents with leeway_finding
	em.emit(sig)
	// 2. Emit duplicate within window -> should be deduplicated (no second POST)
	em.emit(sig)

	mu.Lock()
	if len(reqs) != 1 {
		t.Fatalf("webhook requests = %d, want 1", len(reqs))
	}
	openReq := reqs[0]
	mu.Unlock()

	if openReq.path != "/incidents" {
		t.Errorf("open path = %q, want /incidents", openReq.path)
	}
	var openPayload inject.Payload
	if err := json.Unmarshal(openReq.body, &openPayload); err != nil {
		t.Fatalf("unmarshal open payload: %v", err)
	}
	if openPayload.Cluster != "gke-prod-us-east1" {
		t.Errorf("openPayload.Cluster = %q, want gke-prod-us-east1", openPayload.Cluster)
	}
	if openPayload.LeewayFinding == nil {
		t.Fatal("openPayload.LeewayFinding is nil, want populated finding")
	}
	if openPayload.LeewayFinding.Kind != leeway.KindContractViolated {
		t.Errorf("openPayload.LeewayFinding.Kind = %q, want %q", openPayload.LeewayFinding.Kind, leeway.KindContractViolated)
	}

	// 3. Mark symptom cleared and advance recovery tracker -> should POST /incidents/inc-42/events
	obs.mu.Lock()
	obs.cleared = true
	obs.mu.Unlock()

	em.recovery.Tick() // transitions symptomatic -> clearing
	time.Sleep(20 * time.Millisecond)
	em.recovery.Tick() // transitions clearing -> resolved and dispatches ResolvedPayload

	mu.Lock()
	defer mu.Unlock()
	if len(reqs) != 2 {
		t.Fatalf("webhook requests after resolution = %d, want 2", len(reqs))
	}
	resReq := reqs[1]
	if resReq.path != "/incidents/inc-42/events" {
		t.Errorf("resolved path = %q, want /incidents/inc-42/events", resReq.path)
	}
	var resPayload inject.ResolvedPayload
	if err := json.Unmarshal(resReq.body, &resPayload); err != nil {
		t.Fatalf("unmarshal resolved payload: %v", err)
	}
	if resPayload.Kind != inject.KindResolved {
		t.Errorf("resPayload.Kind = %q, want %q", resPayload.Kind, inject.KindResolved)
	}
	if resPayload.Resolution != string(engine.ResolutionRecovered) {
		t.Errorf("resPayload.Resolution = %q, want %q", resPayload.Resolution, engine.ResolutionRecovered)
	}
}

func TestReportCmd_EndToEnd(t *testing.T) {
	srv := newFakeKubeAPIServer(t)
	defer srv.Close()

	dir := t.TempDir()
	outPath := filepath.Join(dir, "report.json")
	outFile, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	defer outFile.Close()
	errFile, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	defer errFile.Close()

	code := realMain([]string{
		"report",
		"--clusters=test-cluster=" + srv.URL,
		"--output=" + outPath,
	}, outFile, errFile)
	if code != 0 {
		errBytes, _ := os.ReadFile(errFile.Name())
		t.Fatalf("leeway report exit code = %d, want 0 (stderr: %s)", code, string(errBytes))
	}

	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read report output: %v", err)
	}
	var rep report.Report
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatalf("unmarshal report JSON: %v", err)
	}
	if rep.Summary.ClustersTotal != 1 || rep.Summary.ClustersSucceeded != 1 {
		t.Fatalf("unexpected summary: %+v", rep.Summary)
	}
	if len(rep.Clusters[0].TopologyFindings) == 0 {
		t.Fatalf("expected topology findings in report, got 0 (cluster report: %+v)", rep.Clusters[0])
	}
}

func newFakeKubeAPIServer(t *testing.T) *httptest.Server {
	t.Helper()
	isController := true
	nodeList := corev1.NodeList{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "NodeList"},
		Items: []corev1.Node{
			makeAPINode("node-a", "us-central1-a"),
			makeAPINode("node-b", "us-central1-b"),
			makeAPINode("node-c", "us-central1-c"),
		},
	}
	rsList := appsv1.ReplicaSetList{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "ReplicaSetList"},
		Items: []appsv1.ReplicaSet{{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "web-rs",
				Namespace: "default",
				UID:       types.UID("rs-1"),
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "web",
					UID:        types.UID("dep-1"),
					Controller: &isController,
				}},
			},
		}},
	}
	var podItems []corev1.Pod
	for i := 0; i < 6; i++ {
		podItems = append(podItems, corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "web-" + string(rune('a'+i)),
				Namespace: "default",
				UID:       types.UID("pod-" + string(rune('a'+i))),
				Labels:    map[string]string{"app": "web"},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "web-rs",
					UID:        types.UID("rs-1"),
					Controller: &isController,
				}},
			},
			Spec: corev1.PodSpec{
				NodeName: "node-a",
				TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
					MaxSkew:           1,
					TopologyKey:       corev1.LabelTopologyZone,
					WhenUnsatisfiable: corev1.DoNotSchedule,
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "web"},
					},
				}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		})
	}
	podList := corev1.PodList{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"},
		Items:    podItems,
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/nodes":
			_ = json.NewEncoder(w).Encode(nodeList)
		case "/api/v1/pods":
			_ = json.NewEncoder(w).Encode(podList)
		case "/apis/apps/v1/replicasets":
			_ = json.NewEncoder(w).Encode(rsList)
		case "/apis/apps/v1/deployments":
			_ = json.NewEncoder(w).Encode(appsv1.DeploymentList{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "DeploymentList"}})
		case "/apis/apps/v1/statefulsets":
			_ = json.NewEncoder(w).Encode(appsv1.StatefulSetList{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSetList"}})
		case "/api/v1/persistentvolumeclaims":
			_ = json.NewEncoder(w).Encode(corev1.PersistentVolumeClaimList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaimList"}})
		case "/api/v1/persistentvolumes":
			_ = json.NewEncoder(w).Encode(corev1.PersistentVolumeList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeList"}})
		case "/api/v1/events":
			_ = json.NewEncoder(w).Encode(corev1.EventList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "EventList"}})
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","code":404}`))
		}
	}))
}

func makeAPINode(name, zone string) corev1.Node {
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				corev1.LabelTopologyZone:   zone,
				corev1.LabelTopologyRegion: "us-central1",
				corev1.LabelHostname:       name,
			},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{
				Type:   corev1.NodeReady,
				Status: corev1.ConditionTrue,
			}},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("16Gi"),
			},
		},
	}
}
