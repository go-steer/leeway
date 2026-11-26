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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
				Description string `json:"description"`
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
	for _, wantDesc := range []string{"clusters", "clusters_from", "KUBECONFIG", "GOOGLE_CLOUD_PROJECT"} {
		if !strings.Contains(tool.Description, wantDesc) {
			t.Errorf("tool.Description = %q, want mention of %q for omitted-clusters behavior", tool.Description, wantDesc)
		}
	}
	if !tool.Annotations.ReadOnlyHint || tool.Annotations.DestructiveHint || !tool.Annotations.IdempotentHint {
		t.Errorf("unexpected tool annotations: %+v", tool.Annotations)
	}
	if tool.InputSchema.Type != "object" || tool.InputSchema.AdditionalProperties {
		t.Errorf("unexpected inputSchema header: type=%q additionalProperties=%v", tool.InputSchema.Type, tool.InputSchema.AdditionalProperties)
	}
	if gotType, _ := tool.InputSchema.Properties["clusters"]["type"].(string); gotType != "string" {
		t.Errorf("clusters schema type = %q, want \"string\" (for Gemini/OpenAPI schema compatibility)", gotType)
	}
	if clustersDesc, _ := tool.InputSchema.Properties["clusters"]["description"].(string); !strings.Contains(clustersDesc, "array") {
		t.Errorf("clusters schema description = %q, want mention that JSON array is also accepted", clustersDesc)
	}

	// Keep toolsListJSON, acceptedReportFields, and ReportArgs JSON tags strictly aligned.
	var structTags []string
	rt := reflect.TypeOf(ReportArgs{})
	for i := range rt.NumField() {
		tag := strings.Split(rt.Field(i).Tag.Get("json"), ",")[0]
		if tag != "" && tag != "-" {
			structTags = append(structTags, tag)
		}
	}
	if !slices.Equal(structTags, acceptedReportFields) {
		t.Errorf("ReportArgs json tags %v != acceptedReportFields %v", structTags, acceptedReportFields)
	}
	if len(tool.InputSchema.Properties) != len(acceptedReportFields) {
		t.Errorf("inputSchema.properties has %d keys, want %d matching acceptedReportFields", len(tool.InputSchema.Properties), len(acceptedReportFields))
	}
	for _, wantProp := range acceptedReportFields {
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
	byID := make(map[string]string, len(lines))
	for _, line := range lines {
		var env struct {
			ID json.RawMessage `json:"id"`
		}
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			t.Fatalf("decode response line %q: %v", line, err)
		}
		byID[string(bytes.TrimSpace(env.ID))] = line
	}
	wantByID := map[string]string{
		"null": "-32700", // parse error
		"1":    "-32600", // missing method
		"2":    "-32601", // method not found
		"3":    "-32602", // invalid tools/call params
		"4":    `"isError":true`,
		"5":    `unknown field`,
		"6":    `min_drift must be >= 0`,
		"7":    `unknown source`,
		"8":    `topology_keys`,
		"9":    `topology_cluster_defaults`,
		"10":   `simulated discovery failure`,
	}
	for id, want := range wantByID {
		line, ok := byID[id]
		if !ok {
			t.Errorf("missing response for id=%s", id)
			continue
		}
		if !strings.Contains(line, want) {
			t.Errorf("response[id=%s] = %s, want substring %q", id, line, want)
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

func decodeToolText(t *testing.T, raw json.RawMessage) (text string, isErr bool) {
	t.Helper()
	var res struct {
		IsError bool `json:"isError"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("unmarshal CallToolResult: %v (raw=%s)", err, raw)
	}
	if len(res.Content) != 1 {
		t.Fatalf("CallToolResult content len = %d, want 1 (raw=%s)", len(res.Content), raw)
	}
	return res.Content[0].Text, res.IsError
}

func TestCallTool_TimeoutBoundsDiscoveryAndWholeCall(t *testing.T) {
	t.Parallel()

	t.Run("discovery_bounded_by_timeout", func(t *testing.T) {
		t.Parallel()

		var sawDeadline bool
		srv := New(Config{
			Timeout: 80 * time.Millisecond,
			ResolveClusters: func(ctx context.Context, _ clusters.Options) ([]clusters.Cluster, error) {
				_, sawDeadline = ctx.Deadline()
				<-ctx.Done()
				return nil, ctx.Err()
			},
		})

		// Outer guard keeps pre-fix failures fast if ResolveClusters gets an unbounded ctx.
		outerCtx, outerCancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
		defer outerCancel()

		start := time.Now()
		raw, err := srv.CallTool(outerCtx, "report", json.RawMessage(`{"clusters":"demo"}`))
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("CallTool returned RPC error: %v", err)
		}
		if !sawDeadline {
			t.Fatal("ResolveClusters ctx had no deadline; discovery is unbounded")
		}
		if elapsed >= 400*time.Millisecond {
			t.Fatalf("CallTool took %v with Timeout=80ms (hit outer guard instead of server Timeout)", elapsed)
		}
		text, isErr := decodeToolText(t, raw)
		if !isErr || !strings.Contains(text, "deadline exceeded") {
			t.Errorf("got (isErr=%v, text=%q), want deadline exceeded error", isErr, text)
		}
	})

	t.Run("whole_call_deadline_shared_across_discovery_and_waves", func(t *testing.T) {
		t.Parallel()

		var collectDeadline time.Time
		var hasCollectDeadline bool
		srv := New(Config{
			Timeout:     120 * time.Millisecond,
			Parallelism: 1,
			ResolveClusters: func(ctx context.Context, _ clusters.Options) ([]clusters.Cluster, error) {
				time.Sleep(40 * time.Millisecond)
				return []clusters.Cluster{
					{Identity: clusters.Identity{Cluster: "c1"}, Config: &rest.Config{Host: "https://c1.internal"}},
					{Identity: clusters.Identity{Cluster: "c2"}, Config: &rest.Config{Host: "https://c2.internal"}},
					{Identity: clusters.Identity{Cluster: "c3"}, Config: &rest.Config{Host: "https://c3.internal"}},
				}, nil
			},
			CollectReport: func(ctx context.Context, targets []clusters.Cluster, opts report.Options) (*report.Report, error) {
				collectDeadline, hasCollectDeadline = ctx.Deadline()
				<-ctx.Done()
				return nil, ctx.Err()
			},
		})

		outerCtx, outerCancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
		defer outerCancel()

		start := time.Now()
		_, err := srv.CallTool(outerCtx, "report", json.RawMessage(`{"clusters":"c1,c2,c3"}`))
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("CallTool returned RPC error: %v", err)
		}
		if !hasCollectDeadline {
			t.Fatal("CollectReport ctx had no whole-call deadline")
		}
		if collectDeadline.After(start.Add(200 * time.Millisecond)) {
			t.Errorf("CollectReport deadline %v exceeded whole-call Timeout from %v", collectDeadline.Sub(start), 120*time.Millisecond)
		}
		if elapsed >= 500*time.Millisecond {
			t.Errorf("whole CallTool took %v with Timeout=120ms", elapsed)
		}
	})
}

func TestCallTool_AmbientMultiClusterGuard(t *testing.T) {
	t.Parallel()

	okReport := func(_ context.Context, targets []clusters.Cluster, _ report.Options) (*report.Report, error) {
		rep := &report.Report{
			SchemaVersion: report.SchemaVersion,
			Summary:       report.Summary{ClustersTotal: len(targets), ClustersSucceeded: len(targets)},
		}
		for _, c := range targets {
			rep.Clusters = append(rep.Clusters, report.ClusterReport{
				Cluster: c.Name(),
				Source:  c.Profile,
				Status:  report.StatusOK,
			})
		}
		return rep, nil
	}

	gkeTwo := []clusters.Cluster{
		{
			Identity: clusters.Identity{Project: "gke-demos-345619", Location: "us-central1", Cluster: "prod-a"},
			Profile:  "gke:gke-demos-345619/us-central1/prod-a",
			Config:   &rest.Config{Host: "https://a.internal"},
		},
		{
			Identity: clusters.Identity{Project: "gke-demos-345619", Location: "us-east1", Cluster: "prod-b"},
			Profile:  "gke:gke-demos-345619/us-east1/prod-b",
			Config:   &rest.Config{Host: "https://b.internal"},
		},
	}

	kcTwo := []clusters.Cluster{
		{
			Identity: clusters.Identity{Project: "proj", Location: "us-central1", Cluster: "gke_proj_us-central1_c1"},
			Profile:  "kubeconfig:gke_proj_us-central1_c1",
			Config:   &rest.Config{Host: "https://c1.internal"},
		},
		{
			Identity: clusters.Identity{Cluster: "kind-dev"},
			Profile:  "kubeconfig:kind-dev",
			Config:   &rest.Config{Host: "https://c2.internal"},
		},
	}

	t.Run("refuses_ambient_GOOGLE_CLOUD_PROJECT_multi_cluster", func(t *testing.T) {
		t.Parallel()
		collectCalled := false
		srv := New(Config{
			Getenv: func(k string) string {
				if k == "GOOGLE_CLOUD_PROJECT" {
					return "gke-demos-345619"
				}
				return ""
			},
			ResolveClusters: func(context.Context, clusters.Options) ([]clusters.Cluster, error) {
				return gkeTwo, nil
			},
			CollectReport: func(ctx context.Context, targets []clusters.Cluster, opts report.Options) (*report.Report, error) {
				collectCalled = true
				return okReport(ctx, targets, opts)
			},
		})

		raw, err := srv.CallTool(context.Background(), "report", json.RawMessage(`{}`))
		if err != nil {
			t.Fatalf("CallTool: %v", err)
		}
		if collectCalled {
			t.Fatal("CollectReport was called despite ambient multi-cluster guard")
		}
		text, isErr := decodeToolText(t, raw)
		if !isErr {
			t.Fatalf("expected isError=true for ambient multi-cluster sweep, got: %s", text)
		}
		for _, want := range []string{
			"gke-demos-345619",
			"gke-demos-345619/us-central1/prod-a",
			"gke-demos-345619/us-east1/prod-b",
			"clusters",
			"clusters_from",
		} {
			if !strings.Contains(text, want) {
				t.Errorf("error text %q missing %q", text, want)
			}
		}
	})

	t.Run("refuses_ambient_KUBECONFIG_multi_cluster_with_context_names", func(t *testing.T) {
		t.Parallel()
		collectCalled := false
		srv := New(Config{
			Getenv: func(k string) string {
				if k == "KUBECONFIG" {
					return "/tmp/multi.kubeconfig"
				}
				return ""
			},
			ResolveClusters: func(context.Context, clusters.Options) ([]clusters.Cluster, error) {
				return kcTwo, nil
			},
			CollectReport: func(ctx context.Context, targets []clusters.Cluster, opts report.Options) (*report.Report, error) {
				collectCalled = true
				return okReport(ctx, targets, opts)
			},
		})

		raw, err := srv.CallTool(context.Background(), "report", json.RawMessage(`{}`))
		if err != nil {
			t.Fatalf("CallTool: %v", err)
		}
		if collectCalled {
			t.Fatal("CollectReport was called despite ambient KUBECONFIG multi-cluster guard")
		}
		text, isErr := decodeToolText(t, raw)
		if !isErr {
			t.Fatalf("expected isError=true for ambient KUBECONFIG multi-cluster sweep, got: %s", text)
		}
		for _, want := range []string{"kubeconfig", "gke_proj_us-central1_c1", "kind-dev", "clusters", "clusters_from"} {
			if !strings.Contains(text, want) {
				t.Errorf("error text %q missing %q", text, want)
			}
		}
		// Must not mangle kubeconfig context name into "proj/us-central1/gke_proj_us-central1_c1".
		if strings.Contains(text, "proj/us-central1/gke_proj_us-central1_c1") {
			t.Errorf("error text mangled kubeconfig context name: %q", text)
		}
	})

	t.Run("allows_ambient_single_cluster", func(t *testing.T) {
		t.Parallel()
		srv := New(Config{
			Getenv: func(k string) string {
				if k == "GOOGLE_CLOUD_PROJECT" {
					return "gke-demos-345619"
				}
				return ""
			},
			ResolveClusters: func(context.Context, clusters.Options) ([]clusters.Cluster, error) {
				return gkeTwo[:1], nil
			},
			CollectReport: okReport,
		})

		raw, err := srv.CallTool(context.Background(), "report", json.RawMessage(`{}`))
		if err != nil {
			t.Fatalf("CallTool: %v", err)
		}
		text, isErr := decodeToolText(t, raw)
		if isErr {
			t.Fatalf("single-cluster ambient auto-resolution failed: %s", text)
		}
	})

	t.Run("allows_explicit_LEEWAY_CLUSTERS_FROM_multi_cluster_even_with_KUBECONFIG", func(t *testing.T) {
		t.Parallel()
		var gotOpts clusters.Options
		srv := New(Config{
			Getenv: func(k string) string {
				switch k {
				case EnvClustersFrom:
					return "proj-a,proj-b"
				case "GOOGLE_CLOUD_PROJECT":
					return "proj-a"
				case "KUBECONFIG":
					return "/tmp/ambient.kubeconfig"
				}
				return ""
			},
			ResolveClusters: func(_ context.Context, opts clusters.Options) ([]clusters.Cluster, error) {
				gotOpts = opts
				return gkeTwo, nil
			},
			CollectReport: okReport,
		})

		raw, err := srv.CallTool(context.Background(), "report", json.RawMessage(`{}`))
		if err != nil {
			t.Fatalf("CallTool: %v", err)
		}
		text, isErr := decodeToolText(t, raw)
		if isErr {
			t.Fatalf("explicit LEEWAY_CLUSTERS_FROM must not be refused: %s", text)
		}
		if gotOpts.ClustersFrom != "proj-a,proj-b" {
			t.Errorf("opts.ClustersFrom = %q, want %q", gotOpts.ClustersFrom, "proj-a,proj-b")
		}

		// Bare cluster name filtering also honors LEEWAY_CLUSTERS_FROM over ambient KUBECONFIG.
		rawBare, err := srv.CallTool(context.Background(), "report", json.RawMessage(`{"clusters":"demo"}`))
		if err != nil {
			t.Fatalf("CallTool bare with LEEWAY_CLUSTERS_FROM: %v", err)
		}
		if txt, failed := decodeToolText(t, rawBare); failed {
			t.Fatalf("bare cluster with LEEWAY_CLUSTERS_FROM failed: %s", txt)
		}
		if gotOpts.ClustersFrom != "proj-a,proj-b" || gotOpts.Clusters != "demo" {
			t.Errorf("bare cluster opts = %+v, want ClustersFrom=proj-a,proj-b Clusters=demo", gotOpts)
		}
	})

	t.Run("comma_only_args_cannot_bypass_ambient_guard", func(t *testing.T) {
		t.Parallel()
		for _, args := range []string{`{"clusters":","}`, `{"clusters":[","," "]}`, `{"clusters_from":", ,"}`} {
			collectCalled := false
			srv := New(Config{
				Getenv: func(k string) string {
					if k == "GOOGLE_CLOUD_PROJECT" {
						return "gke-demos-345619"
					}
					return ""
				},
				ResolveClusters: func(_ context.Context, opts clusters.Options) ([]clusters.Cluster, error) {
					if opts.ClustersFrom != "gke-demos-345619" {
						t.Errorf("args=%s: ClustersFrom = %q, want gke-demos-345619", args, opts.ClustersFrom)
					}
					return gkeTwo, nil
				},
				CollectReport: func(ctx context.Context, targets []clusters.Cluster, opts report.Options) (*report.Report, error) {
					collectCalled = true
					return okReport(ctx, targets, opts)
				},
			})

			raw, err := srv.CallTool(context.Background(), "report", json.RawMessage(args))
			if err != nil {
				t.Fatalf("CallTool %s: %v", args, err)
			}
			if collectCalled {
				t.Errorf("args=%s bypassed ambient multi-cluster guard", args)
			}
			text, isErr := decodeToolText(t, raw)
			if !isErr || !strings.Contains(text, "gke-demos-345619") {
				t.Errorf("args=%s = (isErr=%v, %q), want ambient refusal mentioning gke-demos-345619", args, isErr, text)
			}
		}
	})

	t.Run("allows_explicit_server_flags_and_tool_args", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name string
			cfg  Config
			args string
		}{
			{
				name: "server_clusters_from",
				cfg:  Config{ClustersFrom: "gke-demos-345619"},
				args: `{}`,
			},
			{
				name: "server_clusters",
				cfg:  Config{Clusters: "gke-demos-345619/us-central1/prod-a,gke-demos-345619/us-east1/prod-b"},
				args: `{}`,
			},
			{
				name: "server_profiles_dir",
				cfg:  Config{ProfilesDir: "/opt/profiles"},
				args: `{}`,
			},
			{
				name: "tool_clusters_from_with_ambient_project",
				cfg: Config{
					Getenv: func(k string) string {
						if k == "GOOGLE_CLOUD_PROJECT" {
							return "gke-demos-345619"
						}
						return ""
					},
				},
				args: `{"clusters_from":"gke-demos-345619"}`,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				cfg := tc.cfg
				cfg.ResolveClusters = func(context.Context, clusters.Options) ([]clusters.Cluster, error) {
					return gkeTwo, nil
				}
				cfg.CollectReport = okReport
				raw, err := New(cfg).CallTool(context.Background(), "report", json.RawMessage(tc.args))
				if err != nil {
					t.Fatalf("CallTool: %v", err)
				}
				if text, isErr := decodeToolText(t, raw); isErr {
					t.Fatalf("explicit config %s was unexpectedly refused: %s", tc.name, text)
				}
			})
		}
	})
}

func TestCallTool_UnknownFieldHintsAndArrayClusters(t *testing.T) {
	t.Parallel()

	var lastOpts clusters.Options
	srv := New(Config{
		ResolveClusters: func(_ context.Context, opts clusters.Options) ([]clusters.Cluster, error) {
			lastOpts = opts
			return []clusters.Cluster{{
				Identity: clusters.Identity{Project: "p", Location: "us-east1", Cluster: "c1"},
				Profile:  "gke:p/us-east1/c1",
				Config:   &rest.Config{Host: "https://c1.internal"},
			}}, nil
		},
		CollectReport: func(_ context.Context, targets []clusters.Cluster, _ report.Options) (*report.Report, error) {
			return &report.Report{
				SchemaVersion: report.SchemaVersion,
				Summary:       report.Summary{ClustersTotal: len(targets), ClustersSucceeded: len(targets)},
			}, nil
		},
	})

	// 1. Unknown field typos produce nearest-property hints plus accepted field list.
	hintCases := []struct {
		name      string
		args      string
		wantSub   []string
		forbidSub []string
	}{
		{
			name:    "singular_cluster",
			args:    `{"cluster":"prod"}`,
			wantSub: []string{`unknown field "cluster"`, `did you mean "clusters"?`, "accepted fields:", "clusters_from", "namespace"},
		},
		{
			name:    "camelCase_clustersFrom",
			args:    `{"clustersFrom":"my-proj"}`,
			wantSub: []string{`unknown field "clustersFrom"`, `did you mean "clusters_from"?`, "accepted fields:"},
		},
		{
			name:    "kebab_clusters_from",
			args:    `{"clusters-from":"my-proj"}`,
			wantSub: []string{`unknown field "clusters-from"`, `did you mean "clusters_from"?`},
		},
		{
			name:    "plural_namespaces",
			args:    `{"namespaces":"prod"}`,
			wantSub: []string{`unknown field "namespaces"`, `did you mean "namespace"?`},
		},
		{
			name:    "case_mismatch_Clusters",
			args:    `{"Clusters":"prod"}`,
			wantSub: []string{`unknown field "Clusters"`, `did you mean "clusters"?`},
		},
		{
			name:    "prefix_topology_tier_c",
			args:    `{"topology_tier_c":true}`,
			wantSub: []string{`unknown field "topology_tier_c"`, `did you mean "topology_tier_c_signals"?`},
		},
		{
			name:      "all_namespaces_does_not_falsely_suggest_all",
			args:      `{"all_namespaces":true}`,
			wantSub:   []string{`unknown field "all_namespaces"`, "accepted fields:", "namespace"},
			forbidSub: []string{`did you mean "all"?`},
		},
		{
			name:      "unrelated_unknown_field_no_bogus_hint",
			args:      `{"format":"json"}`,
			wantSub:   []string{`unknown field "format"`, "accepted fields:", "clusters"},
			forbidSub: []string{"did you mean"},
		},
		{
			name:    "multiple_unknown_fields",
			args:    `{"cluster":"c1","namespaces":"prod"}`,
			wantSub: []string{`did you mean "clusters"?`, `did you mean "namespace"?`, "accepted fields:"},
		},
	}

	for _, tc := range hintCases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := srv.CallTool(context.Background(), "report", json.RawMessage(tc.args))
			if err != nil {
				t.Fatalf("CallTool: %v", err)
			}
			text, isErr := decodeToolText(t, raw)
			if !isErr {
				t.Fatalf("expected isError=true for %s, got %s", tc.args, text)
			}
			for _, sub := range tc.wantSub {
				if !strings.Contains(text, sub) {
					t.Errorf("error %q missing %q", text, sub)
				}
			}
			for _, bad := range tc.forbidSub {
				if strings.Contains(text, bad) {
					t.Errorf("error %q unexpectedly contained %q", text, bad)
				}
			}
		})
	}

	// 2. Array-typed clusters accepted alongside comma-separated strings.
	rawArr, err := srv.CallTool(context.Background(), "report", json.RawMessage(`{"clusters":["projects/p/locations/us-east1/clusters/c1"," projects/p/locations/us-west1/clusters/c2 ",""]}`))
	if err != nil {
		t.Fatalf("CallTool array clusters: %v", err)
	}
	if text, isErr := decodeToolText(t, rawArr); isErr {
		t.Fatalf("array clusters unexpectedly failed: %s", text)
	}
	wantClusters := "projects/p/locations/us-east1/clusters/c1,projects/p/locations/us-west1/clusters/c2"
	if lastOpts.Clusters != wantClusters {
		t.Errorf("ResolveClusters opts.Clusters = %q, want %q", lastOpts.Clusters, wantClusters)
	}

	// Empty array behaves like omitted clusters.
	rawEmptyArr, err := srv.CallTool(context.Background(), "report", json.RawMessage(`{"clusters":[]}`))
	if err != nil {
		t.Fatalf("CallTool empty array clusters: %v", err)
	}
	if text, isErr := decodeToolText(t, rawEmptyArr); isErr {
		t.Fatalf("empty array clusters unexpectedly failed: %s", text)
	}
	if lastOpts.Clusters != "" {
		t.Errorf("empty array clusters: opts.Clusters = %q, want empty", lastOpts.Clusters)
	}

	// Non-string / non-array clusters values rejected cleanly.
	for _, bad := range []string{
		`{"clusters":123}`,
		`{"clusters":true}`,
		`{"clusters":{"name":"c1"}}`,
		`{"clusters":["c1",123]}`,
		`{"clusters":["c1",null]}`,
	} {
		rawBad, err := srv.CallTool(context.Background(), "report", json.RawMessage(bad))
		if err != nil {
			t.Fatalf("CallTool %s: %v", bad, err)
		}
		text, isErr := decodeToolText(t, rawBad)
		if !isErr || !strings.Contains(text, "clusters") {
			t.Errorf("bad clusters %s = (isErr=%v, %q), want error mentioning clusters", bad, isErr, text)
		}
	}
}

func TestCallTool_RealCollectPartialResultsUnderWholeCallDeadline(t *testing.T) {
	t.Parallel()

	fastClient := fake.NewSimpleClientset(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "n1",
			Labels: map[string]string{"topology.kubernetes.io/zone": "us-central1-a"},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("16Gi"),
			},
		},
	})

	t.Run("preserves_fast_cluster_when_slow_cluster_hits_deadline", func(t *testing.T) {
		t.Parallel()
		var slowHits atomic.Int32
		slowSrv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			slowHits.Add(1)
			<-r.Context().Done()
		}))
		defer slowSrv.Close()

		srv := New(Config{
			Timeout:     150 * time.Millisecond,
			Parallelism: 2,
			ResolveClusters: func(context.Context, clusters.Options) ([]clusters.Cluster, error) {
				return []clusters.Cluster{
					{Identity: clusters.Identity{Cluster: "fast"}, Profile: "endpoint:fast", Config: &rest.Config{Host: "https://fast.internal"}},
					{Identity: clusters.Identity{Cluster: "slow"}, Profile: "endpoint:slow", Config: &rest.Config{Host: slowSrv.URL}},
				}, nil
			},
			CollectReport: func(ctx context.Context, targets []clusters.Cluster, opts report.Options) (*report.Report, error) {
				opts.ClientBuilder = func(c clusters.Cluster) (kubernetes.Interface, dynamic.Interface, error) {
					if c.Name() == "fast" {
						return fastClient, nil, nil
					}
					k8s, err := kubernetes.NewForConfig(c.Config)
					return k8s, nil, err
				}
				return report.Collect(ctx, targets, opts)
			},
		})

		start := time.Now()
		raw, err := srv.CallTool(context.Background(), "report", json.RawMessage(`{"clusters":"fast,slow"}`))
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("CallTool: %v", err)
		}
		if slowHits.Load() < 1 {
			t.Fatal("slowSrv never received a request (server closed prematurely?)")
		}
		if elapsed < 100*time.Millisecond {
			t.Errorf("CallTool returned in %v, too fast for a real 150ms deadline", elapsed)
		}
		text, isErr := decodeToolText(t, raw)
		if isErr {
			t.Fatalf("expected non-error partial report JSON, got error: %s", text)
		}
		var rep report.Report
		if err := json.Unmarshal([]byte(text), &rep); err != nil {
			t.Fatalf("unmarshal partial report: %v", err)
		}
		if rep.Summary.ClustersTotal != 2 || rep.Summary.ClustersSucceeded != 1 || rep.Summary.ClustersFailed != 1 {
			t.Errorf("summary = %+v, want total=2 succeeded=1 failed=1", rep.Summary)
		}
		for _, cr := range rep.Clusters {
			if cr.Cluster == "slow" && !strings.Contains(cr.Error, "deadline exceeded") {
				t.Errorf("slow cluster error = %q, want deadline exceeded", cr.Error)
			}
		}
	})

	t.Run("multi_wave_sweep_bounded_by_single_whole_call_timeout", func(t *testing.T) {
		t.Parallel()
		var slowHits atomic.Int32
		slowSrv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			slowHits.Add(1)
			<-r.Context().Done()
		}))
		defer slowSrv.Close()

		srv := New(Config{
			Timeout:     120 * time.Millisecond,
			Parallelism: 1,
			ResolveClusters: func(context.Context, clusters.Options) ([]clusters.Cluster, error) {
				return []clusters.Cluster{
					{Identity: clusters.Identity{Cluster: "slow-1"}, Profile: "endpoint:slow-1", Config: &rest.Config{Host: slowSrv.URL}},
					{Identity: clusters.Identity{Cluster: "slow-2"}, Profile: "endpoint:slow-2", Config: &rest.Config{Host: slowSrv.URL}},
					{Identity: clusters.Identity{Cluster: "slow-3"}, Profile: "endpoint:slow-3", Config: &rest.Config{Host: slowSrv.URL}},
				}, nil
			},
			CollectReport: func(ctx context.Context, targets []clusters.Cluster, opts report.Options) (*report.Report, error) {
				opts.ClientBuilder = func(c clusters.Cluster) (kubernetes.Interface, dynamic.Interface, error) {
					k8s, err := kubernetes.NewForConfig(c.Config)
					return k8s, nil, err
				}
				return report.Collect(ctx, targets, opts)
			},
		})

		start := time.Now()
		raw, err := srv.CallTool(context.Background(), "report", json.RawMessage(`{"clusters":"slow-1,slow-2,slow-3"}`))
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("CallTool: %v", err)
		}
		if slowHits.Load() < 1 {
			t.Fatal("slowSrv never received a request (server closed prematurely?)")
		}
		// Per-cluster timeout alone with Parallelism=1 would take ~360ms (3 x 120ms).
		if elapsed < 80*time.Millisecond || elapsed >= 300*time.Millisecond {
			t.Errorf("multi-wave CallTool took %v, expected bounded near whole-call 120ms timeout", elapsed)
		}
		text, isErr := decodeToolText(t, raw)
		if isErr {
			t.Fatalf("expected leeway.report/v1alpha1 JSON with status=error entries, got tool error: %s", text)
		}
		var rep report.Report
		if err := json.Unmarshal([]byte(text), &rep); err != nil {
			t.Fatalf("unmarshal report: %v", err)
		}
		if rep.Summary.ClustersTotal != 3 || rep.Summary.ClustersFailed != 3 {
			t.Errorf("summary = %+v, want total=3 failed=3", rep.Summary)
		}
		for _, cr := range rep.Clusters {
			if !strings.Contains(cr.Error, "deadline exceeded") {
				t.Errorf("cluster %s error = %q, want deadline exceeded", cr.Cluster, cr.Error)
			}
		}
	})
}

func TestCallTool_RealKubeconfigAmbientRefusalAndSelfCorrection(t *testing.T) {
	t.Parallel()

	kcFile := filepath.Join(t.TempDir(), "multi.kubeconfig")
	kcYAML := strings.Join([]string{
		"apiVersion: v1",
		"kind: Config",
		"clusters:",
		"- name: c1",
		"  cluster:",
		"    server: https://c1.internal:6443",
		"- name: c2",
		"  cluster:",
		"    server: https://c2.internal:6443",
		"contexts:",
		"- name: gke_proj_us-central1_c1",
		"  context:",
		"    cluster: c1",
		"    user: u",
		"- name: kind-dev",
		"  context:",
		"    cluster: c2",
		"    user: u",
		"users:",
		"- name: u",
		"  user:",
		"    token: tok",
	}, "\n") + "\n"
	if err := os.WriteFile(kcFile, []byte(kcYAML), 0o600); err != nil {
		t.Fatal(err)
	}

	var seenTargets []string
	srv := New(Config{
		Getenv: func(k string) string {
			if k == "KUBECONFIG" {
				return kcFile
			}
			return ""
		},
		CollectReport: func(_ context.Context, targets []clusters.Cluster, _ report.Options) (*report.Report, error) {
			seenTargets = nil
			for _, c := range targets {
				seenTargets = append(seenTargets, c.Name())
			}
			return &report.Report{
				SchemaVersion: report.SchemaVersion,
				Summary:       report.Summary{ClustersTotal: len(targets), ClustersSucceeded: len(targets)},
			}, nil
		},
	})

	// 1. Unscoped call refuses multi-context ambient kubeconfig and lists both contexts.
	raw, err := srv.CallTool(context.Background(), "report", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("CallTool unscoped: %v", err)
	}
	text, isErr := decodeToolText(t, raw)
	if !isErr {
		t.Fatalf("expected refusal for multi-context ambient KUBECONFIG, got: %s", text)
	}
	for _, want := range []string{"kubeconfig", "gke_proj_us-central1_c1", "kind-dev"} {
		if !strings.Contains(text, want) {
			t.Errorf("refusal %q missing %q", text, want)
		}
	}

	// 2. Self-correct with array clusters targeting one context.
	rawOne, err := srv.CallTool(context.Background(), "report", json.RawMessage(`{"clusters":["gke_proj_us-central1_c1"]}`))
	if err != nil {
		t.Fatalf("CallTool single context: %v", err)
	}
	if txt, failed := decodeToolText(t, rawOne); failed {
		t.Fatalf("single context failed: %s", txt)
	}
	if !slices.Equal(seenTargets, []string{"gke_proj_us-central1_c1"}) {
		t.Errorf("seenTargets = %v, want [gke_proj_us-central1_c1]", seenTargets)
	}

	// 3. Self-correct with explicit clusters_from="kubeconfig" sweeps both contexts.
	rawAll, err := srv.CallTool(context.Background(), "report", json.RawMessage(`{"clusters_from":"kubeconfig"}`))
	if err != nil {
		t.Fatalf("CallTool clusters_from=kubeconfig: %v", err)
	}
	if txt, failed := decodeToolText(t, rawAll); failed {
		t.Fatalf("clusters_from=kubeconfig failed: %s", txt)
	}
	if len(seenTargets) != 2 {
		t.Errorf("seenTargets = %v, want 2 contexts", seenTargets)
	}
}

func TestCallTool_ConcurrentStderrOnSkipIsRaceFree(t *testing.T) {
	t.Parallel()

	var stderrBuf bytes.Buffer
	srv := New(Config{
		Stderr: &stderrBuf,
		ResolveClusters: func(_ context.Context, opts clusters.Options) ([]clusters.Cluster, error) {
			if opts.OnSkip != nil {
				opts.OnSkip("bad-cluster", errors.New("skipped"))
			}
			return []clusters.Cluster{{
				Identity: clusters.Identity{Cluster: "ok"},
				Config:   &rest.Config{Host: "https://ok.internal"},
			}}, nil
		},
		CollectReport: func(_ context.Context, targets []clusters.Cluster, _ report.Options) (*report.Report, error) {
			return &report.Report{
				SchemaVersion: report.SchemaVersion,
				Summary:       report.Summary{ClustersTotal: len(targets), ClustersSucceeded: len(targets)},
			}, nil
		},
	})

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = srv.CallTool(context.Background(), "report", json.RawMessage(`{"clusters":"ok"}`))
		}()
	}
	wg.Wait()

	if count := strings.Count(stderrBuf.String(), "skipping cluster bad-cluster"); count != 8 {
		t.Errorf("skip log count = %d, want 8", count)
	}
}
