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

package mcpserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	"github.com/go-steer/leeway/pkg/clusters"
	"github.com/go-steer/leeway/pkg/leeway"
	"github.com/go-steer/leeway/pkg/report"
	"github.com/go-steer/leeway/pkg/sources/computeclass"
	"github.com/go-steer/leeway/pkg/sources/topologydrift"
)

func TestListTools_OfflineAndSchemaAligned(t *testing.T) {
	t.Parallel()

	resolveCalled := false
	srv := New(Config{
		Version:    "v0.2.0-dev",
		Kubeconfig: "/nonexistent/kubeconfig-must-not-be-read-at-startup",
		ResolveClusters: func(context.Context, clusters.Options) ([]clusters.Cluster, error) {
			resolveCalled = true
			return nil, errors.New("ResolveClusters must not run during New or ListTools")
		},
	})

	var out bytes.Buffer
	in := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26"}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"ping"}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/list"}`,
	}, "\n") + "\n"

	if err := ServeStdio(context.Background(), strings.NewReader(in), &out, srv); err != nil {
		t.Fatalf("ServeStdio: %v", err)
	}
	if resolveCalled {
		t.Fatal("ResolveClusters was invoked during offline initialize / tools/list")
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d JSON-RPC responses, want 3:\n%s", len(lines), out.String())
	}

	var initResp struct {
		Result struct {
			ProtocolVersion string `json:"protocolVersion"`
			ServerInfo      struct {
				Name    string `json:"name"`
				Version string `json:"version"`
			} `json:"serverInfo"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &initResp); err != nil {
		t.Fatalf("decode initialize response: %v", err)
	}
	if initResp.Result.ServerInfo.Name != "leeway" || initResp.Result.ServerInfo.Version != "v0.2.0-dev" {
		t.Errorf("serverInfo = %+v, want leeway v0.2.0-dev", initResp.Result.ServerInfo)
	}
	if initResp.Result.ProtocolVersion != "2025-03-26" {
		t.Errorf("protocolVersion = %q, want 2025-03-26", initResp.Result.ProtocolVersion)
	}

	var listResp struct {
		Result struct {
			Tools []struct {
				Name        string `json:"name"`
				Annotations struct {
					ReadOnlyHint    bool `json:"readOnlyHint"`
					DestructiveHint bool `json:"destructiveHint"`
					IdempotentHint  bool `json:"idempotentHint"`
				} `json:"annotations"`
				InputSchema struct {
					Type                 string                    `json:"type"`
					AdditionalProperties bool                      `json:"additionalProperties"`
					Properties           map[string]map[string]any `json:"properties"`
				} `json:"inputSchema"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(lines[2]), &listResp); err != nil {
		t.Fatalf("decode tools/list response: %v", err)
	}
	if len(listResp.Result.Tools) != 1 {
		t.Fatalf("got %d tools, want 1", len(listResp.Result.Tools))
	}
	tool := listResp.Result.Tools[0]
	if tool.Name != "report" {
		t.Errorf("tool.Name = %q, want \"report\" (so core-agent qualifies it as leeway_report)", tool.Name)
	}
	if !tool.Annotations.ReadOnlyHint || tool.Annotations.DestructiveHint || !tool.Annotations.IdempotentHint {
		t.Errorf("unexpected tool annotations: %+v", tool.Annotations)
	}
	if tool.InputSchema.Type != "object" || tool.InputSchema.AdditionalProperties {
		t.Errorf("unexpected inputSchema header: type=%q additionalProperties=%v", tool.InputSchema.Type, tool.InputSchema.AdditionalProperties)
	}
	for _, wantProp := range []string{
		"clusters",
		"clusters_from",
		"namespace",
		"sources",
		"topology_keys",
		"topology_domain_unavailable_keys",
		"topology_cluster_defaults",
		"topology_tier_c_signals",
		"compute_class_tier_c_signals",
		"all",
		"min_drift",
	} {
		if _, ok := tool.InputSchema.Properties[wantProp]; !ok {
			t.Errorf("inputSchema.properties missing %q", wantProp)
		}
	}
}

func TestCallTool_ExecutesReportAndPreservesDomainUnavailableOverride(t *testing.T) {
	t.Parallel()

	// Two-zone cluster where us-east1-b has 0 usable nodes (cordoned), so
	// leeway.domain_unavailable fires by default unless topology_domain_unavailable_keys="".
	nodeA := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-a",
			Labels: map[string]string{
				"topology.kubernetes.io/zone":   "us-east1-a",
				"topology.kubernetes.io/region": "us-east1",
			},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("16Gi"),
			},
		},
	}
	nodeB := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-b",
			Labels: map[string]string{
				"topology.kubernetes.io/zone":   "us-east1-b",
				"topology.kubernetes.io/region": "us-east1",
			},
		},
		Spec: corev1.NodeSpec{Unschedulable: true},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("16Gi"),
			},
		},
	}
	ctrlTrue := true
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-rs",
			Namespace: "prod",
			UID:       types.UID("rs-1"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "web",
				UID:        types.UID("dep-1"),
				Controller: &ctrlTrue,
			}},
		},
	}
	pod0 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-0",
			Namespace: "prod",
			UID:       types.UID("p-0"),
			Labels:    map[string]string{"app": "web"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       "web-rs",
				UID:        types.UID("rs-1"),
				Controller: &ctrlTrue,
			}},
		},
		Spec:   corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	pod1 := pod0.DeepCopy()
	pod1.Name = "web-1"
	pod1.UID = types.UID("p-1")

	k8sClient := fake.NewSimpleClientset(nodeA, nodeB, rs, pod0, pod1)

	var lastClusterOpts clusters.Options
	srv := New(Config{
		Version:               "v0.2.0-dev",
		Sources:               []string{topologydrift.Name, computeclass.Name},
		TopologyKeys:          topologydrift.DefaultTopologyKeys,
		DomainUnavailableKeys: topologydrift.DefaultDomainUnavailableKeys,
		Getenv: func(k string) string {
			if k == "GOOGLE_CLOUD_PROJECT" {
				return "proj-default"
			}
			return ""
		},
		ResolveClusters: func(_ context.Context, opts clusters.Options) ([]clusters.Cluster, error) {
			lastClusterOpts = opts
			return []clusters.Cluster{{
				Identity: clusters.Identity{Project: "proj-default", Location: "us-east1", Cluster: "demo"},
				Profile:  "gke:proj-default/us-east1/demo",
				Config:   &rest.Config{Host: "https://demo.internal"},
			}}, nil
		},
		CollectReport: func(ctx context.Context, targets []clusters.Cluster, opts report.Options) (*report.Report, error) {
			opts.ClientBuilder = func(clusters.Cluster) (kubernetes.Interface, dynamic.Interface, error) {
				return k8sClient, nil, nil
			}
			return report.Collect(ctx, targets, opts)
		},
	})

	callReport := func(argsJSON string) report.Report {
		t.Helper()
		raw, err := srv.CallTool(context.Background(), "report", json.RawMessage(argsJSON))
		if err != nil {
			t.Fatalf("CallTool error: %v", err)
		}
		var res struct {
			IsError bool `json:"isError"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		}
		if err := json.Unmarshal(raw, &res); err != nil {
			t.Fatalf("unmarshal CallToolResult: %v", err)
		}
		if res.IsError || len(res.Content) != 1 {
			t.Fatalf("unexpected error tool result: %s", raw)
		}
		var rep report.Report
		if err := json.Unmarshal([]byte(res.Content[0].Text), &rep); err != nil {
			t.Fatalf("unmarshal Report JSON: %v (raw=%s)", err, res.Content[0].Text)
		}
		return rep
	}

	// 1. Bare cluster name "demo" with GOOGLE_CLOUD_PROJECT=proj-default uses default project scope;
	// default domain_unavailable_keys emits leeway.domain_unavailable.
	rep1 := callReport(`{"clusters":"demo","all":true}`)
	if lastClusterOpts.ClustersFrom != "proj-default" || lastClusterOpts.Clusters != "demo" || !lastClusterOpts.KeepFailedScopes {
		t.Errorf("cluster opts for bare name = %+v", lastClusterOpts)
	}
	if rep1.SchemaVersion != report.SchemaVersion || rep1.Summary.ClustersSucceeded != 1 {
		t.Fatalf("unexpected report summary: %+v", rep1.Summary)
	}
	hasDomainUnavailable := slices.ContainsFunc(rep1.Clusters[0].TopologyFindings, func(f leeway.Finding) bool {
		return f.Kind == leeway.KindDomainUnavailable
	})
	if !hasDomainUnavailable {
		t.Errorf("expected leeway.domain_unavailable with default domain keys, got %+v", rep1.Clusters[0].TopologyFindings)
	}
	if len(rep1.Clusters[0].Subjects) == 0 {
		t.Errorf("expected subjects populated when all=true")
	}

	// 2. Explicit cross-project GKE resource path does NOT restrict ClustersFrom to proj-default,
	// and topology_domain_unavailable_keys="" suppresses leeway.domain_unavailable.
	rep2 := callReport(`{"clusters":"projects/proj-other/locations/us-east1/clusters/other","topology_domain_unavailable_keys":"","sources":"topology-drift","namespace":"prod"}`)
	if lastClusterOpts.ClustersFrom != "" || lastClusterOpts.Clusters != "projects/proj-other/locations/us-east1/clusters/other" {
		t.Errorf("cross-project cluster opts = %+v", lastClusterOpts)
	}
	if slices.ContainsFunc(rep2.Clusters[0].TopologyFindings, func(f leeway.Finding) bool {
		return f.Kind == leeway.KindDomainUnavailable
	}) {
		t.Errorf("expected leeway.domain_unavailable suppressed when topology_domain_unavailable_keys=\"\", got %+v", rep2.Clusters[0].TopologyFindings)
	}

	// 3. Empty arguments {} with KUBECONFIG set defaults ClustersFrom to "kubeconfig".
	srvKC := New(Config{
		Getenv: func(k string) string {
			if k == "KUBECONFIG" {
				return "/tmp/kc.yaml"
			}
			return ""
		},
		ResolveClusters: func(_ context.Context, opts clusters.Options) ([]clusters.Cluster, error) {
			lastClusterOpts = opts
			return []clusters.Cluster{{
				Identity: clusters.Identity{Cluster: "ctx-1"},
				Profile:  "kubeconfig:ctx-1",
				Config:   &rest.Config{Host: "https://ctx-1.internal"},
			}}, nil
		},
		CollectReport: func(ctx context.Context, targets []clusters.Cluster, opts report.Options) (*report.Report, error) {
			opts.ClientBuilder = func(clusters.Cluster) (kubernetes.Interface, dynamic.Interface, error) {
				return k8sClient, nil, nil
			}
			return report.Collect(ctx, targets, opts)
		},
	})
	if _, err := srvKC.CallTool(context.Background(), "report", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("srvKC.CallTool: %v", err)
	}
	if lastClusterOpts.ClustersFrom != "kubeconfig" {
		t.Errorf("empty args with KUBECONFIG set: ClustersFrom = %q, want \"kubeconfig\"", lastClusterOpts.ClustersFrom)
	}
}

func TestServeStdio_ProtocolAndValidationNegativeCases(t *testing.T) {
	t.Parallel()

	srv := New(Config{
		ResolveClusters: func(context.Context, clusters.Options) ([]clusters.Cluster, error) {
			return nil, errors.New("simulated discovery failure")
		},
	})

	input := strings.Join([]string{
		`not-valid-json`,
		`{"jsonrpc":"2.0","id":1}`,
		`{"jsonrpc":"2.0","id":2,"method":"unknown/method"}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":"bad"}`,
		`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"wrong_tool","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"report","arguments":{"unknown_key":true}}}`,
		`{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"report","arguments":{"min_drift":-0.1}}}`,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"report","arguments":{"sources":"bogus"}}}`,
		`{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"report","arguments":{"topology_keys":","}}}`,
		`{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"report","arguments":{"topology_cluster_defaults":"malformed"}}}`,
		`{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"report","arguments":{"clusters":"c1"}}}`,
	}, "\n") + "\n"

	var out bytes.Buffer
	if err := ServeStdio(context.Background(), strings.NewReader(input), &out, srv); err != nil {
		t.Fatalf("ServeStdio: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 11 {
		t.Fatalf("got %d responses, want 11:\n%s", len(lines), out.String())
	}
	wantSubstrings := []string{
		"-32700", // 0: parse error
		"-32600", // 1: missing method
		"-32601", // 2: method not found
		"-32602", // 3: invalid tools/call params
		`"isError":true`,
		`unknown field`,
		`min_drift must be >= 0`,
		`unknown source`,
		`topology_keys`,
		`topology_cluster_defaults`,
		`simulated discovery failure`,
	}
	for i, want := range wantSubstrings {
		if !strings.Contains(lines[i], want) {
			t.Errorf("response[%d] = %s, want substring %q", i, lines[i], want)
		}
	}
}

func TestServeStdio_ReturnsPromptlyOnContextCancelWithOpenStdin(t *testing.T) {
	t.Parallel()

	inR, inW := io.Pipe()
	defer inW.Close() // stays open while ServeStdio shuts down

	outR, outW := io.Pipe()
	defer outR.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		defer outW.Close()
		done <- ServeStdio(ctx, inR, outW, New(Config{Version: "test"}))
	}()

	if _, err := io.WriteString(inW, `{"jsonrpc":"2.0","id":1,"method":"ping"}`+"\n"); err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(outR)
	if !sc.Scan() || !strings.Contains(sc.Text(), `"id":1`) {
		t.Fatalf("did not receive ping response before cancel; line=%q err=%v", sc.Text(), sc.Err())
	}

	// Cancel ctx while inW remains open (simulating SIGTERM before stdin EOF).
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeStdio returned error on context cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeStdio hung after context cancellation while stdin remained open")
	}
}

func TestCallTool_ZeroStringDefaultsMixedClustersAndGetenvKubeconfig(t *testing.T) {
	t.Parallel()

	k8sClient := fake.NewSimpleClientset(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "n1",
			Labels: map[string]string{"custom.io/rack": "r1"},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("16Gi"),
			},
		},
	})

	noneDefaults := []corev1.TopologySpreadConstraint{}
	var sawResolveCalls []clusters.Options
	var sawReportOpts report.Options

	srv := New(Config{
		Clusters:                  "projects/cfg-proj/locations/europe-west1/clusters/preconfigured",
		TopologyKeys:              []leeway.TopologyKey{"custom.io/rack"},
		DomainUnavailableKeys:     topologydrift.DefaultDomainUnavailableKeys,
		ClusterDefaultConstraints: &noneDefaults,
		Getenv: func(k string) string {
			if k == "GOOGLE_CLOUD_PROJECT" {
				return "proj-default"
			}
			return ""
		},
		ResolveClusters: func(_ context.Context, opts clusters.Options) ([]clusters.Cluster, error) {
			sawResolveCalls = append(sawResolveCalls, opts)
			if opts.ClustersFrom != "" {
				return []clusters.Cluster{{
					Identity: clusters.Identity{Project: opts.ClustersFrom, Location: "us-central1", Cluster: opts.Clusters},
					Profile:  "gke:" + opts.ClustersFrom + "/us-central1/" + opts.Clusters,
					Config:   &rest.Config{Host: "https://bare.internal"},
				}}, nil
			}
			var out []clusters.Cluster
			for _, tok := range strings.Split(opts.Clusters, ",") {
				out = append(out, clusters.Cluster{
					Identity: clusters.Identity{Cluster: tok},
					Profile:  "explicit:" + tok,
					Config:   &rest.Config{Host: "https://explicit.internal"},
				})
			}
			return out, nil
		},
		CollectReport: func(ctx context.Context, targets []clusters.Cluster, opts report.Options) (*report.Report, error) {
			sawReportOpts = opts
			opts.ClientBuilder = func(clusters.Cluster) (kubernetes.Interface, dynamic.Interface, error) {
				return k8sClient, nil, nil
			}
			return report.Collect(ctx, targets, opts)
		},
	})

	// 1. Zero-value "" on topology_keys and topology_cluster_defaults keeps server defaults,
	// while mixing a bare cluster ("demo"), a preconfigured bare name ("preconfigured"), a
	// cross-project GKE resource path, and a name=endpoint keeps all 4 targets.
	raw, err := srv.CallTool(context.Background(), "report", json.RawMessage(
		`{"clusters":"demo, preconfigured, projects/proj-other/locations/us-east1/clusters/prod, kind=https://10.0.0.1:6443","sources":"","topology_keys":"","topology_cluster_defaults":""}`,
	))
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if strings.Contains(string(raw), `"isError":true`) {
		t.Fatalf("unexpected error tool result: %s", raw)
	}
	if len(sawResolveCalls) != 2 {
		t.Fatalf("saw %d ResolveClusters calls, want 2 (1 scoped for bare 'demo' + 1 explicit for remaining 3): %+v", len(sawResolveCalls), sawResolveCalls)
	}
	if !slices.Equal(sawReportOpts.TopologyKeys, []leeway.TopologyKey{"custom.io/rack"}) {
		t.Errorf("TopologyKeys = %v, want [custom.io/rack]", sawReportOpts.TopologyKeys)
	}
	if sawReportOpts.ClusterDefaultConstraints == nil || len(*sawReportOpts.ClusterDefaultConstraints) != 0 {
		t.Errorf("ClusterDefaultConstraints = %v, want non-nil empty slice (--topology-cluster-defaults=none preserved)", sawReportOpts.ClusterDefaultConstraints)
	}
	var res struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatal(err)
	}
	var rep report.Report
	if err := json.Unmarshal([]byte(res.Content[0].Text), &rep); err != nil {
		t.Fatal(err)
	}
	if rep.Summary.ClustersTotal != 4 {
		t.Errorf("ClustersTotal = %d, want 4 (no cross-project or endpoint target dropped): %+v", rep.Summary.ClustersTotal, rep.Clusters)
	}

	// 2. Real clusters.Resolve with injected Config.Getenv containing both KUBECONFIG and CORE_AGENT_GOOGLE_AUTH=injected.
	kcFile := filepath.Join(t.TempDir(), "mcp.kubeconfig")
	kcYAML := "apiVersion: v1\nkind: Config\nclusters:\n- name: c1\n  cluster:\n    server: https://kc-mcp.internal:6443\ncontexts:\n- name: gke_mcp-proj_us-central1_c1\n  context:\n    cluster: c1\n    user: u\nusers:\n- name: u\n  user:\n    token: tok\n"
	if err := os.WriteFile(kcFile, []byte(kcYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	var resolvedHost string
	srvEnvKC := New(Config{
		Getenv: func(k string) string {
			switch k {
			case "KUBECONFIG":
				return kcFile
			case clusters.CoreAgentGoogleAuthEnv:
				return clusters.GoogleAuthInjected
			}
			return ""
		},
		CollectReport: func(ctx context.Context, targets []clusters.Cluster, opts report.Options) (*report.Report, error) {
			if len(targets) == 1 && targets[0].Config != nil {
				resolvedHost = targets[0].Config.Host
			}
			opts.ClientBuilder = func(clusters.Cluster) (kubernetes.Interface, dynamic.Interface, error) {
				return k8sClient, nil, nil
			}
			return report.Collect(ctx, targets, opts)
		},
	})
	rawKC, err := srvEnvKC.CallTool(context.Background(), "report", json.RawMessage(`{"clusters":"projects/mcp-proj/locations/us-central1/clusters/c1"}`))
	if err != nil || strings.Contains(string(rawKC), `"isError":true`) || resolvedHost != "https://kc-mcp.internal:6443" {
		t.Errorf("srvEnvKC CallTool = (%s, %v), resolvedHost=%q, want https://kc-mcp.internal:6443", rawKC, err, resolvedHost)
	}
}
