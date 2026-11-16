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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// installFakeKubectl puts a bash kubectl running body on PATH and returns the
// file each invocation's arguments are logged to.
func installFakeKubectl(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	logFile := filepath.Join(dir, "kubectl.log")
	script := "#!/usr/bin/env bash\nset -euo pipefail\necho \"$*\" >> \"" + logFile + "\"\nargs=\"$*\"\n" + body
	if err := os.WriteFile(filepath.Join(dir, "kubectl"), []byte(script), 0o755); err != nil {
		t.Fatalf("write fake kubectl: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return logFile
}

func readKubectlLog(t *testing.T, logFile string) string {
	t.Helper()
	raw, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read kubectl log: %v", err)
	}
	return string(raw)
}

func TestRawCollectionPath(t *testing.T) {
	for _, tc := range []struct {
		c    rawCollection
		ns   string
		want string
	}{
		{rawPods, metav1.NamespaceAll, "/api/v1/pods"},
		{rawPods, "team-a", "/api/v1/namespaces/team-a/pods"},
		{rawReplicaSets, "team-a", "/apis/apps/v1/namespaces/team-a/replicasets"},
		{rawNodes, "team-a", "/api/v1/nodes"},
		{rawPVs, "team-a", "/api/v1/persistentvolumes"},
	} {
		if got := tc.c.path(tc.ns); got != tc.want {
			t.Errorf("%s.path(%q) = %q, want %q", tc.c.resource, tc.ns, got, tc.want)
		}
	}
}

func TestKubectlListPaged_ReturnsEveryPageAndTrims(t *testing.T) {
	installFakeKubectl(t, `case "$args" in
  *"/api/v1/namespaces/team-a/pods?limit=250")
    echo '{"metadata":{"continue":"2"},"items":[{"metadata":{"name":"p0","managedFields":[{"manager":"m"}]}}]}' ;;
  *"/api/v1/namespaces/team-a/pods?continue=2&limit=250")
    echo '{"metadata":{},"items":[{"metadata":{"name":"p1"}},{"metadata":{"name":"p2"}}]}' ;;
  *)
    echo "unexpected: $args" >&2; exit 1 ;;
esac
`)
	got, err := kubectlListPaged(context.Background(), nil, rawPods, "team-a", "", trimmed[corev1.Pod])
	if err != nil {
		t.Fatalf("kubectlListPaged: %v", err)
	}
	var names []string
	for _, p := range got {
		names = append(names, p.Name)
		if p.ManagedFields != nil {
			t.Errorf("pod %s kept managedFields; want them trimmed", p.Name)
		}
	}
	if strings.Join(names, ",") != "p0,p1,p2" {
		t.Errorf("pods = %v, want p0,p1,p2", names)
	}
}

func TestKubectlListPaged_ErrorMidListKeepsNothing(t *testing.T) {
	logFile := installFakeKubectl(t, `case "$args" in
  *"/api/v1/pods?limit=250")
    echo '{"metadata":{"continue":"next"},"items":[{"metadata":{"name":"p0"}}]}' ;;
  *)
    echo 'Error from server (Forbidden): pods is forbidden' >&2; exit 1 ;;
esac
`)
	got, err := kubectlListPaged(context.Background(), nil, rawPods, metav1.NamespaceAll, "", kept[corev1.Pod])
	if err == nil || !strings.Contains(err.Error(), "Forbidden") {
		t.Fatalf("err = %v, want the second page's Forbidden error", err)
	}
	if got != nil {
		t.Errorf("items = %d, want none kept from the first page", len(got))
	}
	if calls := readKubectlLog(t, logFile); !strings.Contains(calls, "/api/v1/pods?continue=next&limit=250") {
		t.Errorf("second page not requested with the continue token; log:\n%s", calls)
	}
}

func TestKubectlListPaged_TruncatedAtTheSmallestPageFails(t *testing.T) {
	logFile := installFakeKubectl(t, `echo "credential proxy output truncated" >&2; exit 1
`)
	_, err := kubectlListPaged(context.Background(), nil, rawPods, metav1.NamespaceAll, "", kept[corev1.Pod])
	if !errors.Is(err, errKubectlOutputTruncated) {
		t.Fatalf("err = %v, want errKubectlOutputTruncated", err)
	}
	calls := readKubectlLog(t, logFile)
	for _, want := range []string{"limit=250\n", "limit=125\n", "limit=62\n", "limit=1\n"} {
		if !strings.Contains(calls, want) {
			t.Errorf("no read at %q; log:\n%s", strings.TrimSpace(want), calls)
		}
	}
	if strings.Contains(calls, "limit=0") {
		t.Errorf("page size went below %d; log:\n%s", minRawPageLimit, calls)
	}
}
