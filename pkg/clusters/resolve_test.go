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
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/oauth2"
	container "google.golang.org/api/container/v1"
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
