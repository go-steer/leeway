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

package clusters

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"golang.org/x/oauth2"
	container "google.golang.org/api/container/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func writeTestKubeconfig(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "kubeconfig")
	content := `apiVersion: v1
kind: Config
current-context: ctx-east
clusters:
- name: cluster-east
  cluster:
    server: https://east.example.com:6443
- name: cluster-west
  cluster:
    server: https://west.example.com:6443
contexts:
- name: ctx-east
  context:
    cluster: cluster-east
    user: user-east
- name: ctx-west
  context:
    cluster: cluster-west
    user: user-west
users:
- name: user-east
  user:
    token: tok-east
- name: user-west
  user:
    token: tok-west
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	return path
}

func TestResolve_KubeconfigContextsAndFleet(t *testing.T) {
	dir := t.TempDir()
	kc := writeTestKubeconfig(t, dir)

	// 1. Explicit --clusters=ctx-west,ctx-east
	got, err := Resolve(context.Background(), Options{
		Kubeconfig: kc,
		Clusters:   "ctx-west,ctx-east",
	})
	if err != nil {
		t.Fatalf("Resolve explicit contexts: %v", err)
	}
	if len(got) != 2 || got[0].Name() != "ctx-west" || got[1].Name() != "ctx-east" {
		t.Fatalf("got %+v, want [ctx-west, ctx-east]", got)
	}
	if got[0].Config.Host != "https://west.example.com:6443" {
		t.Errorf("host[0] = %q, want https://west.example.com:6443", got[0].Config.Host)
	}

	// 2. --clusters-from=kubeconfig:<path>
	all, err := Resolve(context.Background(), Options{
		ClustersFrom: "kubeconfig:" + kc,
	})
	if err != nil {
		t.Fatalf("Resolve kubeconfig fleet: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("got %d clusters, want 2", len(all))
	}
}

// TestResolve_ShellModeTargetsCarryNoRestConfig: inside the kube-agents
// agent-sandbox (CREDENTIAL_PROXY_URL + kubectl ShellMode) every target
// resolves to a kubeconfig context and no rest.Config, so leeway report reads
// it through kubectl rather than through client-go.
func TestResolve_ShellModeTargetsCarryNoRestConfig(t *testing.T) {
	t.Setenv(CredentialProxyURLEnv, "http://127.0.0.1:18080")
	t.Setenv(CredentialProxyModeEnv, "kubectl")
	t.Setenv(BrokerURLEnv, "")
	t.Setenv(BrokerTokenFileEnv, "")
	kc := writeTestKubeconfig(t, t.TempDir())

	cases := []struct {
		name        string
		opts        Options
		wantContext string
	}{
		{"GKE triple", Options{Clusters: "my-proj/us-central1/prod-a"}, "gke_my-proj_us-central1_prod-a"},
		{"GKE resource path", Options{Clusters: "projects/my-proj/locations/us-central1/clusters/prod-a"}, "gke_my-proj_us-central1_prod-a"},
		{"gke_ context name", Options{Kubeconfig: kc, Clusters: "gke_my-proj_us-central1_prod-a"}, "gke_my-proj_us-central1_prod-a"},
		{"kubeconfig context name", Options{Kubeconfig: kc, Clusters: "ctx-west"}, "ctx-west"},
		{"single cluster with --context", Options{Kubeconfig: kc, Context: "gke_my-proj_us-central1_prod-a"}, "gke_my-proj_us-central1_prod-a"},
		{"single cluster, no cluster argument", Options{}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Resolve(context.Background(), tc.opts)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("got %d clusters, want 1: %+v", len(got), got)
			}
			if got[0].Config != nil {
				t.Errorf("Config = %+v, want nil", got[0].Config)
			}
			if got[0].Context != tc.wantContext {
				t.Errorf("Context = %q, want %q", got[0].Context, tc.wantContext)
			}
		})
	}
}

func TestParseClusterRef(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"projects/my-proj/locations/us-central1/clusters/prod", "my-proj/us-central1/prod", true},
		{"projects/my-proj/zones/us-central1-a/clusters/zonal", "my-proj/us-central1-a/zonal", true},
		{"my-proj/us-central1/prod", "my-proj/us-central1/prod", true},
		{"example.com:my-proj/us-central1/prod", "example.com:my-proj/us-central1/prod", true},
		{"gke_my-proj_europe-west1_eu-prod", "my-proj/europe-west1/eu-prod", true},
		{"projects/my-proj/locations/us-central1", "", false},
		{"prod=https://10.0.0.1:6443", "", false},
		{"kubeconfig:/tmp/kc", "", false},
		{"../escape/us-central1/prod", "", false},
		{"", "", false},
	}
	for _, tc := range cases {
		got, ok := ParseClusterRef(tc.in)
		if ok != tc.ok || (ok && got.String() != tc.want) {
			t.Errorf("ParseClusterRef(%q) = (%v, %v), want (%q, %v)", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

func TestResolve_KubeconfigResolvesGKEResourcePathsAndTriplesWithoutGKEAPI(t *testing.T) {
	dir := t.TempDir()
	kc := filepath.Join(dir, "kubeconfig")
	content := `apiVersion: v1
kind: Config
current-context: gke_kc-proj_us-central1_prod
clusters:
- name: c-prod
  cluster:
    server: https://prod.kubeconfig.internal:6443
- name: c-stage
  cluster:
    server: https://stage.kubeconfig.internal:6443
contexts:
- name: gke_kc-proj_us-central1_prod
  context:
    cluster: c-prod
    user: u
- name: kc-proj/us-east1/stage
  context:
    cluster: c-stage
    user: u
users:
- name: u
  user:
    token: kc-tok
`
	if err := os.WriteFile(kc, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("KUBECONFIG", "")
	t.Setenv(CredentialProxyURLEnv, "")
	t.Setenv(BrokerURLEnv, "")

	forbidDescribe := func(_ context.Context, id Identity) (*container.Cluster, error) {
		t.Fatalf("GKE Describe called unexpectedly for %s when --kubeconfig was provided", id)
		return nil, nil
	}

	// 1. Explicit --kubeconfig resolves resource path, triple, and gke_ context without hitting GKE API.
	got, err := Resolve(context.Background(), Options{
		Kubeconfig: kc,
		Clusters:   "projects/kc-proj/locations/us-central1/clusters/prod,projects/kc-proj/locations/us-east1/clusters/stage,kc-proj/us-central1/prod",
		Describe:   forbidDescribe,
	})
	if err != nil {
		t.Fatalf("Resolve with --kubeconfig: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d clusters, want 2 (deduplicated): %+v", len(got), got)
	}
	if got[0].Config.Host != "https://prod.kubeconfig.internal:6443" || got[0].Profile != "kubeconfig:gke_kc-proj_us-central1_prod" {
		t.Errorf("got[0] = %+v", got[0])
	}
	if got[1].Config.Host != "https://stage.kubeconfig.internal:6443" || got[1].Profile != "kubeconfig:kc-proj/us-east1/stage" {
		t.Errorf("got[1] = %+v", got[1])
	}

	// 2. Missing cluster under explicit --kubeconfig returns a clear kubeconfig error without calling GKE API.
	if _, err := Resolve(context.Background(), Options{
		Kubeconfig: kc,
		Clusters:   "kc-proj/us-west1/missing",
		Describe:   forbidDescribe,
	}); err == nil || !strings.Contains(err.Error(), "no context matching") {
		t.Errorf("missing cluster in --kubeconfig: err = %v, want context-not-found error", err)
	}

	// 3. KUBECONFIG environment variable without --kubeconfig also resolves GKE resource paths locally.
	t.Setenv("KUBECONFIG", kc)
	gotEnv, err := Resolve(context.Background(), Options{
		Clusters: "projects/kc-proj/locations/us-central1/clusters/prod",
	})
	if err != nil {
		t.Fatalf("Resolve with KUBECONFIG env: %v", err)
	}
	if len(gotEnv) != 1 || gotEnv[0].Config.Host != "https://prod.kubeconfig.internal:6443" {
		t.Fatalf("gotEnv = %+v", gotEnv)
	}
}

func TestResolve_InjectedGoogleAuthWithoutMetadataOrADC(t *testing.T) {
	// Trap metadata server: any attempt to probe GCE metadata fails the test.
	metaTrap := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected GCE metadata request in injected auth mode: %s %s", r.Method, r.URL.Path)
		http.Error(w, "no metadata server in distroless Substrate task", http.StatusForbidden)
	}))
	defer metaTrap.Close()

	t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(metaTrap.URL, "http://"))
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing-adc.json"))
	t.Setenv("KUBECONFIG", "")
	t.Setenv(CredentialProxyURLEnv, "")
	t.Setenv(BrokerURLEnv, "")

	var (
		mu       sync.Mutex
		k8sAuths []string
		gkeAuths []string
	)
	k8sSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		k8sAuths = append(k8sAuths, r.Header.Get("Authorization"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"NodeList","apiVersion":"v1","metadata":{},"items":[]}`))
	}))
	defer k8sSrv.Close()

	// Encode the TLS server's self-signed cert as a PEM CA so ClientConfigForIdentity trusts it.
	caPEM := pemCertFromServer(k8sSrv)

	gkeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gkeAuths = append(gkeAuths, r.Header.Get("Authorization"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/projects/inj-proj/locations/-/clusters":
			_ = json.NewEncoder(w).Encode(&container.ListClustersResponse{
				Clusters: []*container.Cluster{{
					Name:       "c-list",
					Location:   "us-central1",
					Endpoint:   strings.TrimPrefix(k8sSrv.URL, "https://"),
					MasterAuth: &container.MasterAuth{ClusterCaCertificate: base64.StdEncoding.EncodeToString(caPEM)},
				}},
			})
		case "/v1/projects/inj-proj/locations/us-east1/clusters/c-get":
			_ = json.NewEncoder(w).Encode(&container.Cluster{
				Name:       "c-get",
				Location:   "us-east1",
				Endpoint:   strings.TrimPrefix(k8sSrv.URL, "https://"),
				MasterAuth: &container.MasterAuth{ClusterCaCertificate: base64.StdEncoding.EncodeToString(caPEM)},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer gkeSrv.Close()

	for _, envName := range []string{GoogleAuthEnv, CoreAgentGoogleAuthEnv, MastGoogleAuthEnv} {
		t.Run(envName, func(t *testing.T) {
			t.Setenv(GoogleAuthEnv, "")
			t.Setenv(CoreAgentGoogleAuthEnv, "")
			t.Setenv(MastGoogleAuthEnv, "")
			t.Setenv(envName, "injected")

			// Fleet list via --clusters-from
			listed, err := Resolve(context.Background(), Options{
				ClustersFrom: "projects/inj-proj/locations/-",
				GKEEndpoint:  gkeSrv.URL,
			})
			if err != nil {
				t.Fatalf("Resolve fleet (%s=injected): %v", envName, err)
			}
			if len(listed) != 1 || listed[0].Identity.String() != "inj-proj/us-central1/c-list" {
				t.Fatalf("listed = %+v", listed)
			}

			// Explicit resource path via --clusters
			explicit, err := Resolve(context.Background(), Options{
				Clusters:    "projects/inj-proj/locations/us-east1/clusters/c-get",
				GKEEndpoint: gkeSrv.URL,
			})
			if err != nil {
				t.Fatalf("Resolve explicit (%s=injected): %v", envName, err)
			}
			client, err := kubernetes.NewForConfig(explicit[0].Config)
			if err != nil {
				t.Fatalf("NewForConfig: %v", err)
			}
			if _, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{}); err != nil {
				t.Fatalf("Nodes().List: %v", err)
			}
		})
	}

	mu.Lock()
	defer mu.Unlock()
	if len(gkeAuths) == 0 || len(k8sAuths) == 0 {
		t.Fatalf("expected GKE and K8s requests; gke=%d k8s=%d", len(gkeAuths), len(k8sAuths))
	}
	for _, a := range append(gkeAuths, k8sAuths...) {
		if a != "Bearer placeholder" {
			t.Errorf("Authorization = %q, want \"Bearer placeholder\"", a)
		}
	}

	// Foreign non-"injected" values on CORE_AGENT_GOOGLE_AUTH are ignored cleanly,
	// whereas unknown values on LEEWAY_GOOGLE_AUTH or --google-auth fail fast.
	t.Setenv(GoogleAuthEnv, "")
	t.Setenv(CoreAgentGoogleAuthEnv, "adc")
	if mode, err := ResolveGoogleAuthMode(""); err != nil || mode != GoogleAuthADC {
		t.Errorf("CORE_AGENT_GOOGLE_AUTH=adc -> (%q, %v), want (adc, nil)", mode, err)
	}
	t.Setenv(GoogleAuthEnv, "typo")
	if _, err := ResolveGoogleAuthMode(""); err == nil {
		t.Error("expected error on invalid LEEWAY_GOOGLE_AUTH")
	}
}

func pemCertFromServer(srv *httptest.Server) []byte {
	raw := srv.Certificate().Raw
	var b strings.Builder
	b.WriteString("-----BEGIN CERTIFICATE-----\n")
	enc := base64.StdEncoding.EncodeToString(raw)
	for len(enc) > 64 {
		b.WriteString(enc[:64] + "\n")
		enc = enc[64:]
	}
	if len(enc) > 0 {
		b.WriteString(enc + "\n")
	}
	b.WriteString("-----END CERTIFICATE-----\n")
	return []byte(b.String())
}

func TestResolve_MultiScopeClustersFromPartialFailure(t *testing.T) {
	t.Setenv("KUBECONFIG", "")
	t.Setenv(CredentialProxyURLEnv, "")
	t.Setenv(BrokerURLEnv, "")

	fakeTS := func(context.Context) (oauth2.TokenSource, error) {
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "tok"}), nil
	}
	fakeList := func(_ context.Context, parent string) ([]*container.Cluster, error) {
		switch parent {
		case "projects/proj-ok/locations/-":
			return []*container.Cluster{{
				Name:     "c-ok",
				Location: "us-central1",
				ControlPlaneEndpointsConfig: &container.ControlPlaneEndpointsConfig{
					DnsEndpointConfig: &container.DNSEndpointConfig{
						AllowExternalTraffic: true,
						Endpoint:             "c-ok.gke.goog",
					},
				},
			}}, nil
		case "projects/proj-forbidden/locations/us-east1":
			return nil, errors.New("googleapi: Error 403: caller lacks container.clusters.list")
		default:
			t.Fatalf("unexpected parent %q", parent)
			return nil, nil
		}
	}

	// KeepFailedScopes=true retains failed scope as a ResolveError entry alongside healthy clusters.
	got, err := Resolve(context.Background(), Options{
		ClustersFrom:     "projects/proj-ok/locations/-, proj-forbidden/us-east1, proj-ok",
		KeepFailedScopes: true,
		ListGKE:          fakeList,
		TokenSource:      fakeTS,
	})
	if err != nil {
		t.Fatalf("Resolve partial multi-scope: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2 (1 ok + 1 failed scope): %+v", len(got), got)
	}
	if got[0].Name() != "c-ok" || got[0].ResolveError != nil {
		t.Errorf("got[0] = %+v, want healthy c-ok", got[0])
	}
	if got[1].Name() != "proj-forbidden/us-east1" || got[1].Profile != "gke:proj-forbidden/us-east1" || got[1].ResolveError == nil {
		t.Errorf("got[1] = %+v, want ResolveError for proj-forbidden/us-east1", got[1])
	}

	// KeepFailedScopes=false (watch mode) omits ResolveError entries so callers never see nil Config.
	watchTargets, err := Resolve(context.Background(), Options{
		ClustersFrom:     "proj-ok,proj-forbidden/us-east1",
		KeepFailedScopes: false,
		ListGKE:          fakeList,
		TokenSource:      fakeTS,
	})
	if err != nil {
		t.Fatalf("Resolve watch mode: %v", err)
	}
	if len(watchTargets) != 1 || watchTargets[0].Name() != "c-ok" {
		t.Errorf("watchTargets = %+v, want [c-ok]", watchTargets)
	}

	// All scopes failing still returns an error.
	if _, err := Resolve(context.Background(), Options{
		ClustersFrom:     "proj-forbidden/us-east1",
		KeepFailedScopes: true,
		ListGKE:          fakeList,
		TokenSource:      fakeTS,
	}); err == nil {
		t.Error("expected error when every scope in --clusters-from fails")
	}

	// Multi-scope with profiles:<dir> + GKE scope filtered by --clusters=c-ok must NOT
	// record a false status="error" entry on profiles:<dir> when dir simply lacks c-ok.
	emptyProfilesDir := t.TempDir()
	mixedScopes, err := Resolve(context.Background(), Options{
		ClustersFrom:     "profiles:" + emptyProfilesDir + ",proj-ok",
		Clusters:         "c-ok",
		KeepFailedScopes: true,
		ListGKE:          fakeList,
		TokenSource:      fakeTS,
	})
	if err != nil {
		t.Fatalf("Resolve profiles+GKE filtered multi-scope: %v", err)
	}
	if len(mixedScopes) != 1 || mixedScopes[0].Name() != "c-ok" || mixedScopes[0].ResolveError != nil {
		t.Errorf("mixedScopes = %+v, want only healthy [c-ok] with no false error on profiles dir", mixedScopes)
	}

	// KUBECONFIG env takes precedence over ambient CORE_AGENT_GOOGLE_AUTH=injected.
	kc := writeTestKubeconfig(t, t.TempDir())
	gotKCInj, err := Resolve(context.Background(), Options{
		Clusters: "ctx-west",
		Getenv: func(k string) string {
			switch k {
			case "KUBECONFIG":
				return kc
			case CoreAgentGoogleAuthEnv:
				return GoogleAuthInjected
			}
			return ""
		},
	})
	if err != nil || len(gotKCInj) != 1 || gotKCInj[0].Config.Host != "https://west.example.com:6443" {
		t.Errorf("KUBECONFIG with CORE_AGENT_GOOGLE_AUTH=injected = (%+v, %v)", gotKCInj, err)
	}

	// Also verify GKE resource path under KUBECONFIG + CORE_AGENT_GOOGLE_AUTH=injected never hits GKE API.
	gkeKCFile := filepath.Join(t.TempDir(), "gke.kubeconfig")
	gkeKCYAML := "apiVersion: v1\nkind: Config\nclusters:\n- name: c1\n  cluster:\n    server: https://kc-override.internal:6443\ncontexts:\n- name: gke_inj-proj_us-central1_c1\n  context:\n    cluster: c1\n    user: u\nusers:\n- name: u\n  user:\n    token: kc-tok\n"
	if err := os.WriteFile(gkeKCFile, []byte(gkeKCYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	gotGKEKCInj, err := Resolve(context.Background(), Options{
		Clusters:    "projects/inj-proj/locations/us-central1/clusters/c1",
		GKEEndpoint: "http://127.0.0.1:1", // unreachable: proves GKE API is not called
		Getenv: func(k string) string {
			switch k {
			case "KUBECONFIG":
				return gkeKCFile
			case CoreAgentGoogleAuthEnv:
				return GoogleAuthInjected
			}
			return ""
		},
	})
	if err != nil || len(gotGKEKCInj) != 1 || gotGKEKCInj[0].Config.Host != "https://kc-override.internal:6443" {
		t.Errorf("GKE resource path with KUBECONFIG + CORE_AGENT_GOOGLE_AUTH=injected = (%+v, %v)", gotGKEKCInj, err)
	}
}

func TestResolve_ExplicitGKETriplesAndEndpoints(t *testing.T) {
	fakeTS := func(context.Context) (oauth2.TokenSource, error) {
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "fake-adc"}), nil
	}
	fakeDescribe := func(_ context.Context, id Identity) (*container.Cluster, error) {
		return &container.Cluster{
			Name:     id.Cluster,
			Location: id.Location,
			ControlPlaneEndpointsConfig: &container.ControlPlaneEndpointsConfig{
				DnsEndpointConfig: &container.DNSEndpointConfig{
					AllowExternalTraffic: true,
					Endpoint:             "gke-" + id.Cluster + ".gke.goog",
				},
			},
			Autopilot: &container.Autopilot{Enabled: true},
		}, nil
	}

	got, err := Resolve(context.Background(), Options{
		Clusters:    "my-proj/us-central1/prod-a,local-kind=https://127.0.0.1:6443",
		Describe:    fakeDescribe,
		TokenSource: fakeTS,
	})
	if err != nil {
		t.Fatalf("Resolve mixed specs: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d clusters, want 2", len(got))
	}
	if got[0].Identity.String() != "my-proj/us-central1/prod-a" || !got[0].Autopilot {
		t.Errorf("cluster[0] = %+v", got[0])
	}
	if got[0].Config.Host != "https://gke-prod-a.gke.goog" {
		t.Errorf("cluster[0] host = %q", got[0].Config.Host)
	}
	reg, zone := got[0].RegionAndZone()
	if reg != "us-central1" || zone != "" {
		t.Errorf("RegionAndZone = (%q, %q), want (us-central1, '')", reg, zone)
	}
	if got[1].Name() != "local-kind" || got[1].Config.Host != "https://127.0.0.1:6443" {
		t.Errorf("cluster[1] = %+v", got[1])
	}
}

func TestResolve_GKEFleetFromProject(t *testing.T) {
	fakeTS := func(context.Context) (oauth2.TokenSource, error) {
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "fake-adc"}), nil
	}
	fakeList := func(_ context.Context, parent string) ([]*container.Cluster, error) {
		if parent != "projects/my-proj/locations/us-east1" {
			t.Fatalf("unexpected parent %q", parent)
		}
		return []*container.Cluster{
			{
				Name:     "c1",
				Location: "us-east1-b",
				ControlPlaneEndpointsConfig: &container.ControlPlaneEndpointsConfig{
					DnsEndpointConfig: &container.DNSEndpointConfig{
						AllowExternalTraffic: true,
						Endpoint:             "c1.gke.goog",
					},
				},
			},
			{
				Name:     "c2",
				Location: "us-east1-c",
				ControlPlaneEndpointsConfig: &container.ControlPlaneEndpointsConfig{
					DnsEndpointConfig: &container.DNSEndpointConfig{
						AllowExternalTraffic: true,
						Endpoint:             "c2.gke.goog",
					},
				},
			},
		}, nil
	}

	got, err := Resolve(context.Background(), Options{
		ClustersFrom: "my-proj/us-east1",
		Clusters:     "c2",
		ListGKE:      fakeList,
		TokenSource:  fakeTS,
	})
	if err != nil {
		t.Fatalf("Resolve GKE fleet: %v", err)
	}
	if len(got) != 1 || got[0].Name() != "c2" {
		t.Fatalf("got %+v, want [c2]", got)
	}
	reg, zone := got[0].RegionAndZone()
	if reg != "us-east1" || zone != "us-east1-c" {
		t.Errorf("RegionAndZone = (%q, %q), want (us-east1, us-east1-c)", reg, zone)
	}
}

func TestResolve_ProfilesDirAndDirectDeduplication(t *testing.T) {
	dir := t.TempDir()
	for name, cfg := range map[string]string{
		"cluster-mgmt": "cluster_identity:\n  project: proj-1\n  location: us-central1\n  cluster: mgmt\n",
		"cluster-leaf": "cluster_identity:\n  project: proj-1\n  location: europe-west1\n  cluster: leaf\n",
	} {
		pdir := filepath.Join(dir, name)
		if err := os.MkdirAll(pdir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(pdir, "config.yaml"), []byte(cfg), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	fakeTS := func(context.Context) (oauth2.TokenSource, error) {
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "fake-adc"}), nil
	}
	fakeDescribe := func(_ context.Context, id Identity) (*container.Cluster, error) {
		return &container.Cluster{
			Name:     id.Cluster,
			Location: id.Location,
			ControlPlaneEndpointsConfig: &container.ControlPlaneEndpointsConfig{
				DnsEndpointConfig: &container.DNSEndpointConfig{
					AllowExternalTraffic: true,
					Endpoint:             id.Cluster + ".gke.goog",
				},
			},
			Autopilot: &container.Autopilot{Enabled: id.Cluster == "mgmt"},
		}, nil
	}
	fakeDirect := func(string, string, bool) (*rest.Config, error) {
		return &rest.Config{Host: "https://kubernetes.default.svc"}, nil
	}

	got, err := Resolve(context.Background(), Options{
		ProfilesDir: dir,
		InCluster:   true,
		ClusterName: "mgmt",
		Describe:    fakeDescribe,
		TokenSource: fakeTS,
		BuildDirect: fakeDirect,
	})
	if err != nil {
		t.Fatalf("Resolve profiles + direct: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d clusters, want 2 (leaf via profile + mgmt via direct): %+v", len(got), got)
	}

	byName := map[string]Cluster{}
	for _, c := range got {
		byName[c.Name()] = c
	}
	mgmt := byName["mgmt"]
	if mgmt.Profile != DirectProfile || mgmt.Config.Host != "https://kubernetes.default.svc" {
		t.Errorf("mgmt should use direct client, got profile=%q host=%q", mgmt.Profile, mgmt.Config.Host)
	}
	if mgmt.Identity.Project != "proj-1" || mgmt.Identity.Location != "us-central1" || !mgmt.Autopilot {
		t.Errorf("mgmt should absorb profile metadata, got %+v (autopilot=%v)", mgmt.Identity, mgmt.Autopilot)
	}
}
