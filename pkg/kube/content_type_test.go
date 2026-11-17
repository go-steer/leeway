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

package kube

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	authv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

const (
	wantAccept      = "application/vnd.kubernetes.protobuf,application/json"
	wantContentType = "application/vnd.kubernetes.protobuf"
	jsonType        = "application/json"
)

// headerRecorder is a fake API server that records the request headers it
// is sent, keyed by method and path. It answers a namespace list in
// protobuf when protobuf is acceptable and in JSON otherwise, and a node
// list always in JSON, so a test can tell which decoder the client used.
type headerRecorder struct {
	mu  sync.Mutex
	got map[string]http.Header
	srv *httptest.Server
}

func newHeaderRecorder(t *testing.T) *headerRecorder {
	t.Helper()
	h := &headerRecorder{got: map[string]http.Header{}}
	pb := protobuf.NewSerializer(scheme.Scheme, scheme.Scheme)
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		h.got[r.Method+" "+r.URL.Path] = r.Header.Clone()
		h.mu.Unlock()
		switch {
		case r.URL.Path == "/api/v1/namespaces" && strings.HasPrefix(r.Header.Get("Accept"), wantContentType):
			var buf bytes.Buffer
			list := &corev1.NamespaceList{Items: []corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "from-protobuf"}}}}
			if err := pb.Encode(list, &buf); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", wantContentType)
			_, _ = w.Write(buf.Bytes())
		case r.URL.Path == "/api/v1/namespaces":
			w.Header().Set("Content-Type", jsonType)
			_, _ = w.Write([]byte(`{"kind":"NamespaceList","apiVersion":"v1","items":[{"metadata":{"name":"from-json"}}]}`))
		case r.URL.Path == "/api/v1/nodes":
			w.Header().Set("Content-Type", jsonType)
			_, _ = w.Write([]byte(`{"kind":"NodeList","apiVersion":"v1","items":[{"metadata":{"name":"from-json"}}]}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/selfsubjectaccessreviews"):
			w.Header().Set("Content-Type", jsonType)
			_, _ = w.Write([]byte(`{"kind":"SelfSubjectAccessReview","apiVersion":"authorization.k8s.io/v1","status":{"allowed":true}}`))
		default:
			w.Header().Set("Content-Type", jsonType)
			_, _ = w.Write([]byte(`{"kind":"List","apiVersion":"v1","items":[]}`))
		}
	}))
	t.Cleanup(h.srv.Close)
	return h
}

func (h *headerRecorder) header(t *testing.T, method, path, name string) string {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	hdr, ok := h.got[method+" "+path]
	if !ok {
		t.Fatalf("no %s %s request reached the server", method, path)
	}
	return hdr.Get(name)
}

func TestDefaultContentTypesAreProtobufThenJSON(t *testing.T) {
	if DefaultAcceptContentTypes != wantAccept {
		t.Errorf("DefaultAcceptContentTypes = %q, want %q", DefaultAcceptContentTypes, wantAccept)
	}
	if DefaultContentType != wantContentType {
		t.Errorf("DefaultContentType = %q, want %q", DefaultContentType, wantContentType)
	}
}

func TestBuildConfigNegotiatesProtobuf(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	cases := []struct {
		name string
		opts Options
	}{
		{"kubeconfig", Options{Kubeconfig: writeKubeconfig(t)}},
		// callerConfig rebuilds the config from rest.AnonymousClientConfig;
		// the content types have to survive that.
		{"caller token", Options{Kubeconfig: writeTokenKubeconfig(t, "https://cluster.example:6443", "own-token", nil), CallerToken: callerSource()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := BuildConfig(tc.opts)
			if err != nil {
				t.Fatalf("BuildConfig: %v", err)
			}
			if cfg.AcceptContentTypes != wantAccept {
				t.Errorf("AcceptContentTypes = %q, want %q", cfg.AcceptContentTypes, wantAccept)
			}
			if cfg.ContentType != wantContentType {
				t.Errorf("ContentType = %q, want %q", cfg.ContentType, wantContentType)
			}
		})
	}
}

// TestBuildClientFromConfigContentTypes checks what reaches the wire: the
// Accept header of a typed GET and of a raw GET through the typed client's
// RESTClient, the Content-Type of a typed POST, and that the caller's config
// is left as it was handed in.
//
// client-go's generated clients already ask for protobuf when a config sets
// neither field (gentype.PrefersProtobuf), so the typed GET and POST pass
// with or without the defaults when both are unset. The raw GET is what the
// defaults alone decide: without them it asks for JSON.
func TestBuildClientFromConfigContentTypes(t *testing.T) {
	cases := []struct {
		name                 string
		accept, contentType  string
		wantAccept, wantBody string
	}{
		{"unset gets the protobuf defaults", "", "", wantAccept, wantContentType},
		{"caller Accept kept, ContentType defaulted", jsonType, "", jsonType, wantContentType},
		{"caller ContentType kept, Accept defaulted", "", jsonType, wantAccept, jsonType},
		{"both caller values kept", jsonType, jsonType, jsonType, jsonType},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHeaderRecorder(t)
			caller := &rest.Config{Host: h.srv.URL}
			caller.AcceptContentTypes = tc.accept
			caller.ContentType = tc.contentType

			client, err := BuildClientFromConfig(caller)
			if err != nil {
				t.Fatalf("BuildClientFromConfig: %v", err)
			}
			if caller.AcceptContentTypes != tc.accept || caller.ContentType != tc.contentType {
				t.Errorf("caller config modified: AcceptContentTypes=%q ContentType=%q, want %q and %q",
					caller.AcceptContentTypes, caller.ContentType, tc.accept, tc.contentType)
			}

			ctx := context.Background()
			if _, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{}); err != nil {
				t.Fatalf("list namespaces: %v", err)
			}
			if got := h.header(t, http.MethodGet, "/api/v1/namespaces", "Accept"); got != tc.wantAccept {
				t.Errorf("GET Accept = %q, want %q", got, tc.wantAccept)
			}
			if err := client.CoreV1().RESTClient().Get().AbsPath("/api/v1/nodes").Do(ctx).Error(); err != nil {
				t.Fatalf("raw GET nodes: %v", err)
			}
			if got := h.header(t, http.MethodGet, "/api/v1/nodes", "Accept"); got != tc.wantAccept {
				t.Errorf("raw GET Accept = %q, want %q", got, tc.wantAccept)
			}
			review := &authv1.SelfSubjectAccessReview{Spec: authv1.SelfSubjectAccessReviewSpec{
				ResourceAttributes: &authv1.ResourceAttributes{Verb: "list", Resource: "pods"},
			}}
			if _, err := client.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, review, metav1.CreateOptions{}); err != nil {
				t.Fatalf("create SelfSubjectAccessReview: %v", err)
			}
			path := "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews"
			if got := h.header(t, http.MethodPost, path, "Content-Type"); got != tc.wantBody {
				t.Errorf("POST Content-Type = %q, want %q", got, tc.wantBody)
			}
		})
	}
}

// TestTypedClientDecodesBothEncodings: a protobuf answer decodes, and an
// endpoint that answers JSON to a protobuf request still works.
func TestTypedClientDecodesBothEncodings(t *testing.T) {
	h := newHeaderRecorder(t)
	client, err := BuildClientFromConfig(&rest.Config{Host: h.srv.URL})
	if err != nil {
		t.Fatalf("BuildClientFromConfig: %v", err)
	}
	list, err := client.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list namespaces: %v", err)
	}
	if len(list.Items) != 1 || list.Items[0].Name != "from-protobuf" {
		t.Errorf("namespaces = %v, want [from-protobuf]", list.Items)
	}

	// The node list is answered in JSON whatever the client asks for.
	nodes, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes.Items) != 1 || nodes.Items[0].Name != "from-json" {
		t.Errorf("nodes = %v, want [from-json]", nodes.Items)
	}
	if got := h.header(t, http.MethodGet, "/api/v1/nodes", "Accept"); got != wantAccept {
		t.Errorf("GET nodes Accept = %q, want %q", got, wantAccept)
	}
}

// TestDynamicClientStaysJSON pins client-go's dynamic.ConfigFor, which
// replaces both content types with JSON: CRDs have no protobuf encoding, so
// a dynamic client that inherited the protobuf defaults would ask for one.
func TestDynamicClientStaysJSON(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	h := newHeaderRecorder(t)
	gvr := schema.GroupVersionResource{Group: "leeway.lookout.go-steer.io", Version: "v1alpha1", Resource: "clusterleewaypolicies"}
	path := "/apis/leeway.lookout.go-steer.io/v1alpha1/clusterleewaypolicies"

	fromOptions, err := BuildDynamicClient(Options{Kubeconfig: writeTokenKubeconfig(t, h.srv.URL, "tok", nil)})
	if err != nil {
		t.Fatalf("BuildDynamicClient: %v", err)
	}
	protobufCfg := &rest.Config{Host: h.srv.URL, ContentConfig: rest.ContentConfig{
		AcceptContentTypes: wantAccept, ContentType: wantContentType,
	}}
	fromConfig, err := BuildDynamicClientFromConfig(protobufCfg)
	if err != nil {
		t.Fatalf("BuildDynamicClientFromConfig: %v", err)
	}
	for _, c := range []struct {
		name string
		list func() error
	}{
		{"BuildDynamicClient", func() error {
			_, err := fromOptions.Resource(gvr).List(context.Background(), metav1.ListOptions{})
			return err
		}},
		{"BuildDynamicClientFromConfig", func() error {
			_, err := fromConfig.Resource(gvr).List(context.Background(), metav1.ListOptions{})
			return err
		}},
	} {
		if err := c.list(); err != nil {
			t.Fatalf("%s: list: %v", c.name, err)
		}
		if got := h.header(t, http.MethodGet, path, "Accept"); strings.Contains(got, "protobuf") || !strings.HasPrefix(got, jsonType) {
			t.Errorf("%s: GET Accept = %q, want JSON and no protobuf", c.name, got)
		}
	}
}

// TestDiscoveryDecodesProtobuf: discovery's group-version read is a raw
// request on the typed client, so it is the one generated-client read whose
// encoding the defaults change. The policy and ComputeClass sources treat a
// discovery error as "not served", so a protobuf answer that failed to decode
// would silently drop them.
func TestDiscoveryDecodesProtobuf(t *testing.T) {
	pb := protobuf.NewSerializer(scheme.Scheme, scheme.Scheme)
	var accept atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		accept.Store(r.Header.Get("Accept"))
		list := &metav1.APIResourceList{
			TypeMeta:     metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
			GroupVersion: "cloud.google.com/v1",
			APIResources: []metav1.APIResource{{Name: "computeclasses", Kind: "ComputeClass"}},
		}
		var buf bytes.Buffer
		if err := pb.Encode(list, &buf); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", wantContentType)
		_, _ = w.Write(buf.Bytes())
	}))
	defer srv.Close()

	client, err := BuildClientFromConfig(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatalf("BuildClientFromConfig: %v", err)
	}
	got, err := client.Discovery().ServerResourcesForGroupVersion("cloud.google.com/v1")
	if err != nil {
		t.Fatalf("ServerResourcesForGroupVersion: %v", err)
	}
	if got, _ := accept.Load().(string); got != wantAccept {
		t.Errorf("discovery Accept = %q, want %q", got, wantAccept)
	}
	if len(got.APIResources) != 1 || got.APIResources[0].Name != "computeclasses" {
		t.Errorf("APIResources = %+v, want [computeclasses]", got.APIResources)
	}
}
