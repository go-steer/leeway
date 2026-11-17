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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"github.com/go-steer/leeway/pkg/clusters"
)

// TestBuildClients_TypedProtobufCRDsJSON: a target that carries a rest
// config gets a typed client that asks for protobuf and a dynamic client
// that asks for JSON, and the target's config is not modified.
func TestBuildClients_TypedProtobufCRDsJSON(t *testing.T) {
	var (
		mu     sync.Mutex
		accept = map[string]string{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		accept[r.URL.Path] = r.Header.Get("Accept")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"List","apiVersion":"v1","metadata":{},"items":[]}`))
	}))
	defer srv.Close()

	cfg := &rest.Config{Host: srv.URL}
	client, dyn, err := buildClients(clusters.Cluster{Config: cfg}, Options{})
	if err != nil {
		t.Fatalf("buildClients: %v", err)
	}
	if cfg.AcceptContentTypes != "" || cfg.ContentType != "" {
		t.Errorf("target config modified: AcceptContentTypes=%q ContentType=%q", cfg.AcceptContentTypes, cfg.ContentType)
	}

	ctx := context.Background()
	if _, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{}); err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	// A raw request through the typed client is decided by the config alone;
	// client-go's own protobuf preference covers only generated calls.
	if err := client.CoreV1().RESTClient().Get().AbsPath("/raw").Do(ctx).Error(); err != nil {
		t.Fatalf("raw GET: %v", err)
	}
	if _, err := listComputeClasses(ctx, dyn); err != nil {
		t.Fatalf("list compute classes: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if got, want := accept["/api/v1/nodes"], "application/vnd.kubernetes.protobuf,application/json"; got != want {
		t.Errorf("typed node list Accept = %q, want %q", got, want)
	}
	if got, want := accept["/raw"], "application/vnd.kubernetes.protobuf,application/json"; got != want {
		t.Errorf("raw GET Accept = %q, want %q", got, want)
	}
	crd, ok := accept["/apis/cloud.google.com/v1/computeclasses"]
	if !ok {
		t.Fatalf("no compute class list reached the server; saw %v", accept)
	}
	if strings.Contains(crd, "protobuf") || !strings.HasPrefix(crd, "application/json") {
		t.Errorf("CRD list Accept = %q, want JSON and no protobuf", crd)
	}
}
