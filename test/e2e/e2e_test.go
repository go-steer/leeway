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

// Package e2e exercises the compiled `leeway` binary (`leeway report` and
// `leeway watch`) end-to-end as an OS subprocess against live HTTP Kubernetes
// API servers, a live HTTP webhook receiver, and SQLite state persistence.
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	authv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/go-steer/leeway/pkg/report"
)

var (
	buildOnce sync.Once
	builtBin  string
	buildErr  error
)

func leewayBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "leeway-e2e-bin-*")
		if err != nil {
			buildErr = err
			return
		}
		builtBin = filepath.Join(dir, "leeway")
		cmd := exec.Command("go", "build", "-o", builtBin, "github.com/go-steer/leeway/cmd/leeway")
		out, err := cmd.CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("go build cmd/leeway: %w\n%s", err, string(out))
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return builtBin
}

// webhookCapture is a live HTTP webhook receiver implementing
// POST /incidents and POST /incidents/<id>/events.
type webhookCapture struct {
	srv     *httptest.Server
	mu      sync.Mutex
	counter int
	opens   []webhookEvent
	appends []webhookEvent
}

type webhookEvent struct {
	ID   string
	Kind string
	Raw  string
	Body map[string]any
}

func newWebhookCapture(t *testing.T) *webhookCapture {
	t.Helper()
	w := &webhookCapture{}
	w.srv = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rawBytes, _ := io.ReadAll(req.Body)
		_ = req.Body.Close()
		var body map[string]any
		_ = json.Unmarshal(rawBytes, &body)
		kind, _ := body["kind"].(string)

		w.mu.Lock()
		defer w.mu.Unlock()

		if req.Method == http.MethodPost && req.URL.Path == "/incidents" {
			w.counter++
			id := fmt.Sprintf("inc-%04d", w.counter)
			w.opens = append(w.opens, webhookEvent{
				ID:   id,
				Kind: kind,
				Raw:  string(rawBytes),
				Body: body,
			})
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(rw, `{"id":%q}`, id)
			return
		}
		if req.Method == http.MethodPost && strings.HasPrefix(req.URL.Path, "/incidents/") && strings.HasSuffix(req.URL.Path, "/events") {
			parts := strings.Split(strings.TrimPrefix(req.URL.Path, "/incidents/"), "/")
			id := parts[0]
			w.appends = append(w.appends, webhookEvent{
				ID:   id,
				Kind: kind,
				Raw:  string(rawBytes),
				Body: body,
			})
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte(`{}`))
			return
		}
		http.NotFound(rw, req)
	}))
	t.Cleanup(w.srv.Close)
	return w
}

func (w *webhookCapture) findOpen(pred func(webhookEvent) bool) (webhookEvent, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, ev := range w.opens {
		if pred(ev) {
			return ev, true
		}
	}
	return webhookEvent{}, false
}

func (w *webhookCapture) countOpens(pred func(webhookEvent) bool) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, ev := range w.opens {
		if pred(ev) {
			n++
		}
	}
	return n
}

func (w *webhookCapture) findAppend(pred func(webhookEvent) bool) (webhookEvent, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, ev := range w.appends {
		if pred(ev) {
			return ev, true
		}
	}
	return webhookEvent{}, false
}

// mockKubeCluster serves a fake Kubernetes API over HTTP supporting discovery,
// SelfSubjectAccessReview, resource LISTs, and live chunked WATCH streams.
type mockKubeCluster struct {
	srv *httptest.Server

	mu              sync.Mutex
	rv              int64
	nodes           []corev1.Node
	pods            []corev1.Pod
	deployments     []appsv1.Deployment
	replicaSets     []appsv1.ReplicaSet
	events          []corev1.Event
	policies        []unstructured.Unstructured
	clusterPolicies []unstructured.Unstructured
	computeClasses  []unstructured.Unstructured
	servePolicies   bool
	serveClasses    bool

	podWatchers map[chan watchFrame]struct{}
}

type watchFrame struct {
	Type   string `json:"type"`
	Object any    `json:"object"`
}

func newMockKubeCluster(t *testing.T) *mockKubeCluster {
	t.Helper()
	c := &mockKubeCluster{
		rv:          100,
		podWatchers: make(map[chan watchFrame]struct{}),
	}
	c.srv = httptest.NewServer(http.HandlerFunc(c.handle))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *mockKubeCluster) nextRVLocked() string {
	c.rv++
	return fmt.Sprintf("%d", c.rv)
}

func (c *mockKubeCluster) deletePod(namespace, name string) {
	c.mu.Lock()
	var deleted *corev1.Pod
	kept := c.pods[:0]
	for i := range c.pods {
		p := c.pods[i]
		if p.Namespace == namespace && p.Name == name {
			cp := p
			cp.ResourceVersion = c.nextRVLocked()
			deleted = &cp
			continue
		}
		kept = append(kept, p)
	}
	c.pods = kept
	var chans []chan watchFrame
	for ch := range c.podWatchers {
		chans = append(chans, ch)
	}
	c.mu.Unlock()

	if deleted != nil {
		frame := watchFrame{Type: "DELETED", Object: deleted}
		for _, ch := range chans {
			select {
			case ch <- frame:
			default:
			}
		}
	}
}

func (c *mockKubeCluster) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := r.URL.Path

	// SelfSubjectAccessReview: grant all read checks.
	if r.Method == http.MethodPost && path == "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews" {
		var sar authv1.SelfSubjectAccessReview
		_ = json.NewDecoder(r.Body).Decode(&sar)
		sar.Status.Allowed = true
		_ = json.NewEncoder(w).Encode(&sar)
		return
	}

	// API resource discovery for optional CRDs.
	if path == "/apis/leeway.lookout.go-steer.io/v1alpha1" {
		c.mu.Lock()
		serve := c.servePolicies
		c.mu.Unlock()
		if !serve {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(&metav1.APIResourceList{
			TypeMeta:     metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
			GroupVersion: "leeway.lookout.go-steer.io/v1alpha1",
			APIResources: []metav1.APIResource{
				{Name: "leewaypolicies", Kind: "LeewayPolicy", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
				{Name: "clusterleewaypolicies", Kind: "ClusterLeewayPolicy", Namespaced: false, Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		})
		return
	}
	if path == "/apis/cloud.google.com/v1" {
		c.mu.Lock()
		serve := c.serveClasses
		c.mu.Unlock()
		if !serve {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(&metav1.APIResourceList{
			TypeMeta:     metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
			GroupVersion: "cloud.google.com/v1",
			APIResources: []metav1.APIResource{
				{Name: "computeclasses", Kind: "ComputeClass", Namespaced: false, Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		})
		return
	}

	// Watch streams.
	if r.URL.Query().Get("watch") == "true" || r.URL.Query().Get("watch") == "1" {
		if r.URL.Query().Get("sendInitialEvents") == "true" {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_ = json.NewEncoder(w).Encode(&metav1.Status{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
				Status:   metav1.StatusFailure,
				Code:     http.StatusUnprocessableEntity,
				Reason:   metav1.StatusReasonInvalid,
				Message:  "sendInitialEvents is not supported by mock apiserver",
			})
			return
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		if strings.HasSuffix(path, "/pods") {
			ch := make(chan watchFrame, 16)
			c.mu.Lock()
			c.podWatchers[ch] = struct{}{}
			c.mu.Unlock()
			defer func() {
				c.mu.Lock()
				delete(c.podWatchers, ch)
				c.mu.Unlock()
			}()
			enc := json.NewEncoder(w)
			for {
				select {
				case <-r.Context().Done():
					return
				case frame := <-ch:
					_ = enc.Encode(frame)
					flusher.Flush()
				}
			}
		}

		<-r.Context().Done()
		return
	}

	c.mu.Lock()
	rv := fmt.Sprintf("%d", c.rv)
	nodes := append([]corev1.Node(nil), c.nodes...)
	pods := append([]corev1.Pod(nil), c.pods...)
	deps := append([]appsv1.Deployment(nil), c.deployments...)
	rss := append([]appsv1.ReplicaSet(nil), c.replicaSets...)
	evs := append([]corev1.Event(nil), c.events...)
	pols := append([]unstructured.Unstructured(nil), c.policies...)
	cpols := append([]unstructured.Unstructured(nil), c.clusterPolicies...)
	ccs := append([]unstructured.Unstructured(nil), c.computeClasses...)
	c.mu.Unlock()

	for i := range nodes {
		nodes[i].TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Node"}
		if nodes[i].ResourceVersion == "" {
			nodes[i].ResourceVersion = "1"
		}
	}
	for i := range pods {
		pods[i].TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
		if pods[i].ResourceVersion == "" {
			pods[i].ResourceVersion = "1"
		}
	}
	for i := range deps {
		deps[i].TypeMeta = metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}
		if deps[i].ResourceVersion == "" {
			deps[i].ResourceVersion = "1"
		}
	}
	for i := range rss {
		rss[i].TypeMeta = metav1.TypeMeta{APIVersion: "apps/v1", Kind: "ReplicaSet"}
		if rss[i].ResourceVersion == "" {
			rss[i].ResourceVersion = "1"
		}
	}
	for i := range evs {
		evs[i].TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Event"}
		if evs[i].ResourceVersion == "" {
			evs[i].ResourceVersion = "1"
		}
	}

	switch {
	case path == "/api/v1/nodes":
		_ = json.NewEncoder(w).Encode(&corev1.NodeList{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "NodeList"},
			ListMeta: metav1.ListMeta{ResourceVersion: rv},
			Items:    nodes,
		})
	case strings.HasSuffix(path, "/pods"):
		_ = json.NewEncoder(w).Encode(&corev1.PodList{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"},
			ListMeta: metav1.ListMeta{ResourceVersion: rv},
			Items:    pods,
		})
	case strings.HasSuffix(path, "/deployments"):
		_ = json.NewEncoder(w).Encode(&appsv1.DeploymentList{
			TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "DeploymentList"},
			ListMeta: metav1.ListMeta{ResourceVersion: rv},
			Items:    deps,
		})
	case strings.HasSuffix(path, "/replicasets"):
		_ = json.NewEncoder(w).Encode(&appsv1.ReplicaSetList{
			TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "ReplicaSetList"},
			ListMeta: metav1.ListMeta{ResourceVersion: rv},
			Items:    rss,
		})
	case strings.HasSuffix(path, "/statefulsets"):
		_ = json.NewEncoder(w).Encode(&appsv1.StatefulSetList{
			TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSetList"},
			ListMeta: metav1.ListMeta{ResourceVersion: rv},
		})
	case strings.HasSuffix(path, "/persistentvolumeclaims"):
		_ = json.NewEncoder(w).Encode(&corev1.PersistentVolumeClaimList{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaimList"},
			ListMeta: metav1.ListMeta{ResourceVersion: rv},
		})
	case path == "/api/v1/persistentvolumes":
		_ = json.NewEncoder(w).Encode(&corev1.PersistentVolumeList{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeList"},
			ListMeta: metav1.ListMeta{ResourceVersion: rv},
		})
	case strings.HasSuffix(path, "/events"):
		_ = json.NewEncoder(w).Encode(&corev1.EventList{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "EventList"},
			ListMeta: metav1.ListMeta{ResourceVersion: rv},
			Items:    evs,
		})
	case strings.HasSuffix(path, "/leewaypolicies"):
		_ = json.NewEncoder(w).Encode(&unstructured.UnstructuredList{
			Object: map[string]any{
				"apiVersion": "leeway.lookout.go-steer.io/v1alpha1",
				"kind":       "LeewayPolicyList",
				"metadata":   map[string]any{"resourceVersion": rv},
			},
			Items: pols,
		})
	case path == "/apis/leeway.lookout.go-steer.io/v1alpha1/clusterleewaypolicies":
		_ = json.NewEncoder(w).Encode(&unstructured.UnstructuredList{
			Object: map[string]any{
				"apiVersion": "leeway.lookout.go-steer.io/v1alpha1",
				"kind":       "ClusterLeewayPolicyList",
				"metadata":   map[string]any{"resourceVersion": rv},
			},
			Items: cpols,
		})
	case path == "/apis/cloud.google.com/v1/computeclasses":
		_ = json.NewEncoder(w).Encode(&unstructured.UnstructuredList{
			Object: map[string]any{
				"apiVersion": "cloud.google.com/v1",
				"kind":       "ComputeClassList",
				"metadata":   map[string]any{"resourceVersion": rv},
			},
			Items: ccs,
		})
	default:
		http.NotFound(w, r)
	}
}

func writeKubeconfig(t *testing.T, clusters map[string]string, currentContext string) string {
	t.Helper()
	var sb strings.Builder
	sb.WriteString("apiVersion: v1\nkind: Config\nclusters:\n")
	for name, url := range clusters {
		fmt.Fprintf(&sb, "- name: %s\n  cluster:\n    server: %s\n", name, url)
	}
	sb.WriteString("contexts:\n")
	for name := range clusters {
		fmt.Fprintf(&sb, "- name: %s\n  context:\n    cluster: %s\n    user: test-user\n", name, name)
	}
	fmt.Fprintf(&sb, "current-context: %s\nusers:\n- name: test-user\n  user:\n    token: test-token\n", currentContext)

	p := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(p, []byte(sb.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func makeZoneNode(name, zoneKey, zone string, unschedulable bool) corev1.Node {
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			UID:    types.UID("uid-" + name),
			Labels: map[string]string{zoneKey: zone, "kubernetes.io/hostname": name},
		},
		Spec: corev1.NodeSpec{
			Unschedulable: unschedulable,
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

func makeSpreadWorkload(ns, name, zoneKey string, when corev1.UnsatisfiableConstraintAction, replicas int32, podNodes []string, createdAt time.Time) (appsv1.Deployment, appsv1.ReplicaSet, []corev1.Pod) {
	isController := true
	depUID := types.UID("uid-dep-" + name)
	rsUID := types.UID("uid-rs-" + name)
	rsName := name + "-7f8d9c"

	dep := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         ns,
			Name:              name,
			UID:               depUID,
			CreationTimestamp: metav1.NewTime(createdAt),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": name}},
		},
	}
	rs := appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         ns,
			Name:              rsName,
			UID:               rsUID,
			CreationTimestamp: metav1.NewTime(createdAt),
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "apps/v1", Kind: "Deployment", Name: name, UID: depUID, Controller: &isController},
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
		},
	}

	var pods []corev1.Pod
	for i, nodeName := range podNodes {
		podName := fmt.Sprintf("%s-%d", rsName, i)
		pods = append(pods, corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         ns,
				Name:              podName,
				UID:               types.UID("uid-pod-" + podName),
				CreationTimestamp: metav1.NewTime(createdAt),
				Labels:            map[string]string{"app.kubernetes.io/name": name},
				OwnerReferences: []metav1.OwnerReference{
					{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rsName, UID: rsUID, Controller: &isController},
				},
			},
			Spec: corev1.PodSpec{
				NodeName: nodeName,
				TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
					{
						TopologyKey:       zoneKey,
						MaxSkew:           1,
						WhenUnsatisfiable: when,
						LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": name}},
					},
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		})
	}
	return dep, rs, pods
}

type runningWatch struct {
	cmd         *exec.Cmd
	metricsAddr string
	logs        *safeBuffer
	cancel      context.CancelFunc
	done        chan error
}

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func startLeewayWatch(t *testing.T, args ...string) *runningWatch {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	bin := leewayBinary(t)
	fullArgs := append([]string{"watch", "--metrics-addr=127.0.0.1:0"}, args...)
	cmd := exec.CommandContext(ctx, bin, fullArgs...)

	logs := &safeBuffer{}
	pr, pw := io.Pipe()
	cmd.Stderr = io.MultiWriter(logs, pw)
	cmd.Stdout = logs

	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start leeway watch: %v", err)
	}

	addrCh := make(chan string, 1)
	var addrSent atomic.Bool
	go func() {
		sc := bufio.NewScanner(pr)
		for sc.Scan() {
			line := sc.Text()
			if idx := strings.Index(line, "leeway: serving /metrics /healthz /readyz on "); idx >= 0 {
				if addrSent.CompareAndSwap(false, true) {
					addrCh <- strings.TrimSpace(line[idx+len("leeway: serving /metrics /healthz /readyz on "):])
				}
			}
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
		_ = pw.Close()
	}()

	rw := &runningWatch{
		cmd:    cmd,
		logs:   logs,
		cancel: cancel,
		done:   done,
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	})

	select {
	case addr := <-addrCh:
		rw.metricsAddr = addr
	case err := <-done:
		t.Fatalf("leeway watch exited before listening: %v\nlogs:\n%s", err, logs.String())
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for leeway watch listener\nlogs:\n%s", logs.String())
	}

	// Wait for /readyz 200.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + rw.metricsAddr + "/readyz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return rw
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("/readyz did not return 200 within 10s\nlogs:\n%s", logs.String())
	return rw
}

func scrapeMetrics(t *testing.T, addr string) string {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /metrics: %v", err)
	}
	return string(b)
}

func awaitCondition(t *testing.T, timeout time.Duration, desc string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, desc)
}

// TestE2E_PlacementDriftAndPolicy exercises Scenario 1:
//   - Two placement domains (`zone-a`, `zone-b`) on `lookout-examples/zone`
//   - `spread-hard` (DoNotSchedule, 4 replicas all in zone-a) -> Tier A `leeway.contract_violated`
//   - `spread-soft` (ScheduleAnyway, 4 replicas all in zone-a) + `LeewayPolicy` overriding severity to `critical` -> Tier B `leeway.placement_drift`
//   - Verifies both `leeway report` (JSON output) and `leeway watch` (/metrics + webhook POST /incidents + SQLite --store).
func TestE2E_PlacementDriftAndPolicy(t *testing.T) {
	const zoneKey = "lookout-examples/zone"
	past := time.Now().Add(-30 * time.Minute).UTC()

	cluster := newMockKubeCluster(t)
	cluster.servePolicies = true
	cluster.nodes = []corev1.Node{
		makeZoneNode("worker-a", zoneKey, "zone-a", false),
		makeZoneNode("worker-b", zoneKey, "zone-b", false),
	}

	depHard, rsHard, podsHard := makeSpreadWorkload(
		"leeway-drift", "spread-hard", zoneKey,
		corev1.DoNotSchedule, 4,
		[]string{"worker-a", "worker-a", "worker-a", "worker-a"},
		past,
	)
	depSoft, rsSoft, podsSoft := makeSpreadWorkload(
		"leeway-drift", "spread-soft", zoneKey,
		corev1.ScheduleAnyway, 4,
		[]string{"worker-a", "worker-a", "worker-a", "worker-a"},
		past,
	)
	cluster.deployments = []appsv1.Deployment{depHard, depSoft}
	cluster.replicaSets = []appsv1.ReplicaSet{rsHard, rsSoft}
	cluster.pods = append(cluster.pods, podsHard...)
	cluster.pods = append(cluster.pods, podsSoft...)
	cluster.policies = []unstructured.Unstructured{
		{
			Object: map[string]any{
				"apiVersion": "leeway.lookout.go-steer.io/v1alpha1",
				"kind":       "LeewayPolicy",
				"metadata": map[string]any{
					"name":            "spread-soft-pages",
					"namespace":       "leeway-drift",
					"resourceVersion": "1",
				},
				"spec": map[string]any{
					"selector": map[string]any{
						"matchLabels": map[string]any{"app.kubernetes.io/name": "spread-soft"},
					},
					"topologyKeys": []any{
						map[string]any{
							"key":        zoneKey,
							"mode":       "Spread",
							"thresholds": map[string]any{"severity": "critical"},
						},
					},
				},
			},
		},
	}

	kubeconfig := writeKubeconfig(t, map[string]string{"e2e-drift": cluster.srv.URL}, "e2e-drift")
	bin := leewayBinary(t)

	// 1. Verify `leeway report` one-shot CLI.
	out, err := exec.Command(bin, "report",
		"--kubeconfig="+kubeconfig,
		"--sources=topology-drift",
		"--topology-keys="+zoneKey,
		"--topology-domain-unavailable-keys="+zoneKey,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("leeway report failed: %v\n%s", err, string(out))
	}

	var rep report.Report
	if err := json.Unmarshal(out, &rep); err != nil {
		t.Fatalf("unmarshal report JSON: %v\n%s", err, string(out))
	}
	if rep.Summary.ClustersSucceeded != 1 || len(rep.Clusters) != 1 {
		t.Fatalf("unexpected report summary: %+v", rep.Summary)
	}
	cr := rep.Clusters[0]
	var gotHard, gotSoft, gotDomainUnavailable bool
	for _, f := range cr.TopologyFindings {
		switch {
		case f.Kind == "leeway.contract_violated" && f.Subject.Name == "spread-hard":
			gotHard = true
			if f.Tier != "A" || f.Severity != "critical" {
				t.Errorf("spread-hard tier/severity = %s/%s, want A/critical", f.Tier, f.Severity)
			}
			if f.Score.ObservedSkew != 4 || f.Score.Drift != 0.5 || f.Score.RelocationDistance != 2 {
				t.Errorf("spread-hard score = %+v, want observedSkew=4 drift=0.5 relocationDistance=2", f.Score)
			}
		case f.Kind == "leeway.placement_drift" && f.Subject.Name == "spread-soft":
			gotSoft = true
			if f.Tier != "B" || f.Severity != "critical" || !f.SeverityFromPolicy {
				t.Errorf("spread-soft = tier=%s severity=%s fromPolicy=%v, want B/critical/true",
					f.Tier, f.Severity, f.SeverityFromPolicy)
			}
		case f.Kind == "leeway.domain_unavailable":
			gotDomainUnavailable = true
		}
	}
	if !gotHard || !gotSoft {
		t.Fatalf("report findings missing spread-hard (%v) or spread-soft (%v): %+v", gotHard, gotSoft, cr.TopologyFindings)
	}
	if gotDomainUnavailable {
		t.Fatalf("unexpected leeway.domain_unavailable in placement-drift scenario: %+v", cr.TopologyFindings)
	}

	// 2. Verify `leeway watch` daemon + webhook + SQLite store + Prometheus metrics.
	webhook := newWebhookCapture(t)
	storeFile := filepath.Join(t.TempDir(), "leeway.db")
	rw := startLeewayWatch(t,
		"--kubeconfig="+kubeconfig,
		"--cluster-name=e2e-drift",
		"--sources=topology-drift",
		"--topology-keys="+zoneKey,
		"--topology-domain-unavailable-keys="+zoneKey,
		"--topology-dwell=300ms",
		"--webhook-url="+webhook.srv.URL,
		"--store="+storeFile,
	)

	awaitCondition(t, 8*time.Second, "webhook receives spread-hard and spread-soft findings", func() bool {
		_, okHard := webhook.findOpen(func(ev webhookEvent) bool {
			return strings.Contains(ev.Raw, `"leeway.contract_violated"`) && strings.Contains(ev.Raw, `"spread-hard"`)
		})
		_, okSoft := webhook.findOpen(func(ev webhookEvent) bool {
			return strings.Contains(ev.Raw, `"leeway.placement_drift"`) &&
				strings.Contains(ev.Raw, `"spread-soft"`) &&
				strings.Contains(ev.Raw, `"severityFromPolicy":true`)
		})
		return okHard && okSoft
	})

	if n := webhook.countOpens(func(ev webhookEvent) bool {
		return strings.Contains(ev.Raw, `"leeway.domain_unavailable"`)
	}); n > 0 {
		t.Fatalf("webhook received %d leeway.domain_unavailable events, want 0", n)
	}

	metrics := scrapeMetrics(t, rw.metricsAddr)
	for _, wantMetric := range []string{
		"lookout_leeway_drift",
		"lookout_leeway_observed_skew",
		"lookout_leeway_alert_state",
		"lookout_leeway_standalone_signals_total",
	} {
		if !strings.Contains(metrics, wantMetric) {
			t.Errorf("/metrics missing %q", wantMetric)
		}
	}
}

// TestE2E_ZoneUnavailableAndSuppression exercises Scenario 2:
//   - Workloads `zone-hard` and `zone-soft` are spread across `zone-a` and `zone-b`
//   - `zone-b`'s only node (`worker-b`) is cordoned (`spec.unschedulable: true`)
//   - Verifies both `leeway report` and `leeway watch` emit ONE `leeway.domain_unavailable`
//     finding (`suspected_cause="taint_exclusion"`) for `zone-b` and zero per-workload drift findings.
func TestE2E_ZoneUnavailableAndSuppression(t *testing.T) {
	const zoneKey = "lookout-examples/zone"
	past := time.Now().Add(-30 * time.Minute).UTC()

	cluster := newMockKubeCluster(t)
	cluster.nodes = []corev1.Node{
		makeZoneNode("worker-a", zoneKey, "zone-a", false),
		makeZoneNode("worker-b", zoneKey, "zone-b", true), // cordoned
	}

	depHard, rsHard, podsHard := makeSpreadWorkload(
		"leeway-zone", "zone-hard", zoneKey,
		corev1.DoNotSchedule, 2,
		[]string{"worker-a", "worker-b"},
		past,
	)
	depSoft, rsSoft, podsSoft := makeSpreadWorkload(
		"leeway-zone", "zone-soft", zoneKey,
		corev1.ScheduleAnyway, 4,
		[]string{"worker-a", "worker-b", "worker-a", "worker-b"},
		past,
	)
	cluster.deployments = []appsv1.Deployment{depHard, depSoft}
	cluster.replicaSets = []appsv1.ReplicaSet{rsHard, rsSoft}
	cluster.pods = append(cluster.pods, podsHard...)
	cluster.pods = append(cluster.pods, podsSoft...)

	kubeconfig := writeKubeconfig(t, map[string]string{"e2e-zone": cluster.srv.URL}, "e2e-zone")
	bin := leewayBinary(t)

	// 1. Verify `leeway report`.
	out, err := exec.Command(bin, "report",
		"--kubeconfig="+kubeconfig,
		"--sources=topology-drift",
		"--topology-keys="+zoneKey,
		"--topology-domain-unavailable-keys="+zoneKey,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("leeway report failed: %v\n%s", err, string(out))
	}

	var rep report.Report
	if err := json.Unmarshal(out, &rep); err != nil {
		t.Fatalf("unmarshal report JSON: %v\n%s", err, string(out))
	}
	cr := rep.Clusters[0]
	domainCount := 0
	workloadCount := 0
	for _, f := range cr.TopologyFindings {
		switch f.Kind {
		case "leeway.domain_unavailable":
			domainCount++
			if f.Subject.Name != "zone-b" || string(f.SuspectedCause) != "taint_exclusion" {
				t.Errorf("domain finding = subject=%s cause=%s, want zone-b / taint_exclusion", f.Subject.Name, f.SuspectedCause)
			}
		case "leeway.contract_violated", "leeway.placement_drift":
			if f.Subject.Name == "zone-hard" || f.Subject.Name == "zone-soft" {
				workloadCount++
			}
		}
	}
	if domainCount != 1 {
		t.Fatalf("report domain_unavailable count = %d, want 1: %+v", domainCount, cr.TopologyFindings)
	}
	if workloadCount != 0 {
		t.Fatalf("report per-workload findings = %d, want 0: %+v", workloadCount, cr.TopologyFindings)
	}

	// 2. Verify `leeway watch`.
	webhook := newWebhookCapture(t)
	rw := startLeewayWatch(t,
		"--kubeconfig="+kubeconfig,
		"--cluster-name=e2e-zone",
		"--sources=topology-drift",
		"--topology-keys="+zoneKey,
		"--topology-domain-unavailable-keys="+zoneKey,
		"--topology-dwell=300ms",
		"--webhook-url="+webhook.srv.URL,
	)

	awaitCondition(t, 8*time.Second, "webhook receives leeway.domain_unavailable for zone-b", func() bool {
		_, ok := webhook.findOpen(func(ev webhookEvent) bool {
			return strings.Contains(ev.Raw, `"leeway.domain_unavailable"`) &&
				strings.Contains(ev.Raw, `"zone-b"`) &&
				strings.Contains(ev.Raw, `"taint_exclusion"`)
		})
		return ok
	})

	// Hold for two additional dwell cycles to confirm no per-workload drift fires.
	time.Sleep(700 * time.Millisecond)
	if n := webhook.countOpens(func(ev webhookEvent) bool {
		return strings.Contains(ev.Raw, `"leeway.domain_unavailable"`)
	}); n != 1 {
		t.Fatalf("webhook domain_unavailable count = %d, want 1", n)
	}
	if n := webhook.countOpens(func(ev webhookEvent) bool {
		return strings.Contains(ev.Raw, `"zone-hard"`) || strings.Contains(ev.Raw, `"zone-soft"`)
	}); n != 0 {
		t.Fatalf("webhook per-workload findings = %d, want 0", n)
	}

	metrics := scrapeMetrics(t, rw.metricsAddr)
	if !strings.Contains(metrics, "lookout_leeway_domains_unavailable") {
		t.Errorf("/metrics missing lookout_leeway_domains_unavailable")
	}
}

// TestE2E_ComputeClassFallbackWedgedAndRecovery exercises Scenario 3:
//   - GKE ComputeClass `gold-ccc` with `whenUnsatisfiable: DoNotScaleUp` and two priorities
//   - Running pod on fallback rank 1 -> `leeway.rank_degraded` (`last_rank_ceiling`)
//   - Pending pod with `NotTriggerScaleUp` Event + control Pending pod with `TriggeredScaleUp` Event -> `leeway.rank_wedged` (`pendingDeclined=1`, `pendingProvisioning=1`)
//   - Deleting the wedged pod over the live watch stream triggers `POST /incidents/<id>/events` with `kind=resolved` (`resolution=recovered`).
func TestE2E_ComputeClassFallbackWedgedAndRecovery(t *testing.T) {
	now := time.Now().UTC()

	cluster := newMockKubeCluster(t)
	cluster.serveClasses = true
	cluster.computeClasses = []unstructured.Unstructured{
		{
			Object: map[string]any{
				"apiVersion": "cloud.google.com/v1",
				"kind":       "ComputeClass",
				"metadata": map[string]any{
					"name":            "gold-ccc",
					"resourceVersion": "1",
				},
				"spec": map[string]any{
					"whenUnsatisfiable": "DoNotScaleUp",
					"priorities": []any{
						map[string]any{"machineFamily": "n4"},
						map[string]any{"machineFamily": "n2"},
					},
				},
			},
		},
	}
	cluster.nodes = []corev1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-rank1",
				UID:  "uid-node-rank1",
				Labels: map[string]string{
					"cloud.google.com/compute-class":  "gold-ccc",
					"cloud.google.com/machine-family": "n2",
				},
				Annotations: map[string]string{
					"ccc_priority_index": "1",
				},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		},
	}
	cluster.pods = []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "pod-running-rank1",
				UID:       "uid-pod-running-rank1",
			},
			Spec: corev1.PodSpec{
				NodeName:     "node-rank1",
				NodeSelector: map[string]string{"cloud.google.com/compute-class": "gold-ccc"},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "pod-wedged",
				UID:       "uid-pod-wedged",
			},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{"cloud.google.com/compute-class": "gold-ccc"},
			},
			Status: corev1.PodStatus{Phase: corev1.PodPending},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "pod-provisioning",
				UID:       "uid-pod-provisioning",
			},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{"cloud.google.com/compute-class": "gold-ccc"},
			},
			Status: corev1.PodStatus{Phase: corev1.PodPending},
		},
	}
	cluster.events = []corev1.Event{
		{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "ev-declined",
				UID:       "uid-ev-declined",
			},
			InvolvedObject: corev1.ObjectReference{
				Kind:      "Pod",
				Namespace: "default",
				Name:      "pod-wedged",
				UID:       "uid-pod-wedged",
			},
			Reason:        "NotTriggerScaleUp",
			Message:       "pod didn't trigger scale-up: 1 max node group size reached",
			Source:        corev1.EventSource{Component: "cluster-autoscaler"},
			LastTimestamp: metav1.NewTime(now),
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "ev-provisioning",
				UID:       "uid-ev-provisioning",
			},
			InvolvedObject: corev1.ObjectReference{
				Kind:      "Pod",
				Namespace: "default",
				Name:      "pod-provisioning",
				UID:       "uid-pod-provisioning",
			},
			Reason:        "TriggeredScaleUp",
			Message:       "pod triggered scale-up",
			Source:        corev1.EventSource{Component: "cluster-autoscaler"},
			LastTimestamp: metav1.NewTime(now),
		},
	}

	kubeconfig := writeKubeconfig(t, map[string]string{"e2e-cc": cluster.srv.URL}, "e2e-cc")
	bin := leewayBinary(t)

	// 1. Verify `leeway report`.
	out, err := exec.Command(bin, "report",
		"--kubeconfig="+kubeconfig,
		"--sources=compute-class",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("leeway report failed: %v\n%s", err, string(out))
	}

	var rep report.Report
	if err := json.Unmarshal(out, &rep); err != nil {
		t.Fatalf("unmarshal report JSON: %v\n%s", err, string(out))
	}
	cr := rep.Clusters[0]
	if len(cr.ComputeClasses) != 1 {
		t.Fatalf("expected 1 compute class summary, got %+v", cr.ComputeClasses)
	}
	cs := cr.ComputeClasses[0]
	if cs.PendingPods != 2 || cs.PendingProvisioning != 1 || cs.PendingDeclined != 1 {
		t.Errorf("class pending counts = %+v, want pending=2 provisioning=1 declined=1", cs)
	}

	var gotWedged, gotDegraded bool
	for _, rf := range cr.RankFindings {
		switch rf.Kind {
		case "leeway.rank_wedged":
			gotWedged = true
		case "leeway.rank_degraded":
			gotDegraded = true
			if rf.Rule != "last-rank" || rf.Observed.LastRank != 1 || rf.Observed.LastRankShare != 1 {
				t.Errorf("rank_degraded = %+v, want rule=last-rank lastRank=1 lastRankShare=1", rf)
			}
		}
	}
	if !gotWedged || !gotDegraded {
		t.Fatalf("report rankFindings missing wedged (%v) or degraded (%v): %+v", gotWedged, gotDegraded, cr.RankFindings)
	}

	// 2. Verify `leeway watch` + live resolution on pod deletion.
	webhook := newWebhookCapture(t)
	_ = startLeewayWatch(t,
		"--kubeconfig="+kubeconfig,
		"--cluster-name=e2e-cc",
		"--sources=compute-class",
		"--compute-class-window=400ms",
		"--compute-class-dwell=200ms",
		"--recovery-stable-for=300ms",
		"--webhook-url="+webhook.srv.URL,
	)

	var wedgedIncidentID string
	awaitCondition(t, 10*time.Second, "webhook receives leeway.rank_wedged and leeway.rank_degraded", func() bool {
		wedgedEv, okW := webhook.findOpen(func(ev webhookEvent) bool {
			return strings.Contains(ev.Raw, `"leeway.rank_wedged"`) && strings.Contains(ev.Raw, `"gold-ccc"`)
		})
		_, okD := webhook.findOpen(func(ev webhookEvent) bool {
			return strings.Contains(ev.Raw, `"leeway.rank_degraded"`) && strings.Contains(ev.Raw, `"last-rank"`)
		})
		if okW {
			wedgedIncidentID = wedgedEv.ID
		}
		return okW && okD
	})

	// Delete the wedged pod (`pod-wedged`). Note that `pod-provisioning` (which
	// has TriggeredScaleUp) remains Pending, proving that a provisioning pod
	// does NOT keep `rank_wedged` open!
	cluster.deletePod("default", "pod-wedged")

	awaitCondition(t, 10*time.Second, "webhook receives kind=resolved for leeway.rank_wedged", func() bool {
		_, ok := webhook.findAppend(func(ev webhookEvent) bool {
			return ev.ID == wedgedIncidentID &&
				ev.Kind == "resolved" &&
				strings.Contains(ev.Raw, `"resolution":"recovered"`)
		})
		return ok
	})
}

// TestE2E_MultiClusterReportAndWatch exercises Scenario 4:
//   - Multi-cluster evaluation across `cluster-a` (topology drift), `cluster-b`
//     (compute-class wedged), and `cluster-dead` (unreachable endpoint).
//   - Verifies fault isolation in `leeway report` and concurrent multi-cluster
//     watching in `leeway watch --clusters=cluster-a,cluster-b`.
func TestE2E_MultiClusterReportAndWatch(t *testing.T) {
	const zoneKey = "lookout-examples/zone"
	past := time.Now().Add(-30 * time.Minute).UTC()

	clusterA := newMockKubeCluster(t)
	clusterA.nodes = []corev1.Node{
		makeZoneNode("a-worker-1", zoneKey, "zone-a", false),
		makeZoneNode("a-worker-2", zoneKey, "zone-b", false),
	}
	depA, rsA, podsA := makeSpreadWorkload(
		"prod", "api-gateway", zoneKey,
		corev1.DoNotSchedule, 4,
		[]string{"a-worker-1", "a-worker-1", "a-worker-1", "a-worker-1"},
		past,
	)
	clusterA.deployments = []appsv1.Deployment{depA}
	clusterA.replicaSets = []appsv1.ReplicaSet{rsA}
	clusterA.pods = podsA

	clusterB := newMockKubeCluster(t)
	clusterB.serveClasses = true
	clusterB.computeClasses = []unstructured.Unstructured{
		{
			Object: map[string]any{
				"apiVersion": "cloud.google.com/v1",
				"kind":       "ComputeClass",
				"metadata":   map[string]any{"name": "gpu-ccc", "resourceVersion": "1"},
				"spec": map[string]any{
					"whenUnsatisfiable": "DoNotScaleUp",
					"priorities": []any{
						map[string]any{"machineFamily": "g2"},
						map[string]any{"machineFamily": "n2"},
					},
				},
			},
		},
	}
	clusterB.nodes = []corev1.Node{
		makeZoneNode("b-worker-1", zoneKey, "zone-a", false),
		makeZoneNode("b-worker-2", zoneKey, "zone-b", false),
	}
	clusterB.pods = []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "trainer-0", UID: "uid-trainer-0"},
			Spec:       corev1.PodSpec{NodeSelector: map[string]string{"cloud.google.com/compute-class": "gpu-ccc"}},
			Status:     corev1.PodStatus{Phase: corev1.PodPending},
		},
	}

	// Unreachable cluster server (immediately closed).
	deadSrv := httptest.NewServer(http.NotFoundHandler())
	deadURL := deadSrv.URL
	deadSrv.Close()

	kubeconfig := writeKubeconfig(t, map[string]string{
		"cluster-a":    clusterA.srv.URL,
		"cluster-b":    clusterB.srv.URL,
		"cluster-dead": deadURL,
	}, "cluster-a")
	bin := leewayBinary(t)

	// 1. Multi-cluster `leeway report`.
	out, err := exec.Command(bin, "report",
		"--kubeconfig="+kubeconfig,
		"--clusters=cluster-a,cluster-b,cluster-dead",
		"--topology-keys="+zoneKey,
		"--timeout=5s",
	).CombinedOutput()
	if err != nil {
		t.Fatalf("leeway report multi-cluster failed: %v\n%s", err, string(out))
	}

	var rep report.Report
	if err := json.Unmarshal(out, &rep); err != nil {
		t.Fatalf("unmarshal multi-cluster report: %v\n%s", err, string(out))
	}
	if rep.Summary.ClustersTotal != 3 || rep.Summary.ClustersSucceeded != 2 || rep.Summary.ClustersFailed != 1 {
		t.Fatalf("unexpected multi-cluster summary: %+v", rep.Summary)
	}
	if rep.Summary.TopologyFindings != 1 || rep.Summary.RankFindings != 1 {
		t.Fatalf("expected 1 topology finding and 1 rank finding across fleet, got %+v", rep.Summary)
	}

	// 2. Multi-cluster `leeway watch`.
	webhook := newWebhookCapture(t)
	rw := startLeewayWatch(t,
		"--kubeconfig="+kubeconfig,
		"--clusters=cluster-a,cluster-b",
		"--topology-keys="+zoneKey,
		"--topology-dwell=300ms",
		"--compute-class-dwell=200ms",
		"--webhook-url="+webhook.srv.URL,
	)

	awaitCondition(t, 10*time.Second, "webhook receives findings from both cluster-a and cluster-b", func() bool {
		_, okA := webhook.findOpen(func(ev webhookEvent) bool {
			return ev.Body["cluster"] == "cluster-a" && strings.Contains(ev.Raw, `"api-gateway"`)
		})
		_, okB := webhook.findOpen(func(ev webhookEvent) bool {
			return ev.Body["cluster"] == "cluster-b" && strings.Contains(ev.Raw, `"gpu-ccc"`)
		})
		return okA && okB
	})

	metrics := scrapeMetrics(t, rw.metricsAddr)
	if !strings.Contains(metrics, `cluster="cluster-a"`) || !strings.Contains(metrics, `cluster="cluster-b"`) {
		t.Errorf("/metrics missing per-cluster labels for cluster-a and cluster-b")
	}
}
