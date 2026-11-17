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

func TestKubectlListPaged_RecoversLimitAfterTruncatedPage(t *testing.T) {
	logFile := installFakeKubectl(t, `case "$args" in
  *"/api/v1/pods?limit=250"|*"/api/v1/pods?limit=125")
    echo "credential proxy output truncated" >&2; exit 1 ;;
  *"/api/v1/pods?limit=62")
    echo '{"metadata":{"continue":"p2"},"items":[{"metadata":{"name":"p0"}}]}' ;;
  *"/api/v1/pods?continue=p2&limit=124")
    echo '{"metadata":{"continue":"p3"},"items":[{"metadata":{"name":"p1"}}]}' ;;
  *"/api/v1/pods?continue=p3&limit=248")
    echo '{"metadata":{"continue":"p4"},"items":[{"metadata":{"name":"p2"}}]}' ;;
  *"/api/v1/pods?continue=p4&limit=250")
    echo '{"metadata":{},"items":[{"metadata":{"name":"p3"}}]}' ;;
  *)
    echo "unexpected: $args" >&2; exit 1 ;;
esac
`)
	got, err := kubectlListPaged(context.Background(), nil, rawPods, metav1.NamespaceAll, "", kept[corev1.Pod])
	if err != nil {
		t.Fatalf("kubectlListPaged: %v (log:\n%s)", err, readKubectlLog(t, logFile))
	}
	var names []string
	for _, p := range got {
		names = append(names, p.Name)
	}
	if strings.Join(names, ",") != "p0,p1,p2,p3" {
		t.Errorf("pods = %v, want p0,p1,p2,p3", names)
	}
	calls := readKubectlLog(t, logFile)
	for _, want := range []string{
		"/api/v1/pods?limit=62",
		"/api/v1/pods?continue=p2&limit=124",
		"/api/v1/pods?continue=p3&limit=248",
		"/api/v1/pods?continue=p4&limit=250",
	} {
		if !strings.Contains(calls, want) {
			t.Errorf("kubectl calls missing %q; log:\n%s", want, calls)
		}
	}
}

func TestKubectlListPaged_RetriesOnceOnExpiredContinue(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "attempt")
	logFile := installFakeKubectl(t, `count=0
if [ -f "`+stateFile+`" ]; then count=$(cat "`+stateFile+`"); fi
case "$args" in
  *"/api/v1/pods?limit=250")
    count=$((count + 1))
    echo "$count" > "`+stateFile+`"
    if [ "$count" -eq 1 ]; then
      echo '{"metadata":{"continue":"stale"},"items":[{"metadata":{"name":"stale-p0"}}]}'
    else
      echo '{"metadata":{"continue":"fresh"},"items":[{"metadata":{"name":"fresh-p0"}}]}'
    fi
    ;;
  *"/api/v1/pods?continue=stale&limit=250")
    echo 'Error from server (Expired): The continue parameter is too old to display a consistent list result.' >&2
    exit 1
    ;;
  *"/api/v1/pods?continue=fresh&limit=250")
    echo '{"metadata":{},"items":[{"metadata":{"name":"fresh-p1"}}]}'
    ;;
  *)
    echo "unexpected: $args" >&2; exit 1 ;;
esac
`)
	got, err := kubectlListPaged(context.Background(), nil, rawPods, metav1.NamespaceAll, "", kept[corev1.Pod])
	if err != nil {
		t.Fatalf("kubectlListPaged: %v (log:\n%s)", err, readKubectlLog(t, logFile))
	}
	var names []string
	for _, p := range got {
		names = append(names, p.Name)
	}
	if strings.Join(names, ",") != "fresh-p0,fresh-p1" {
		t.Errorf("pods = %v, want fresh-p0,fresh-p1 (stale items must be discarded)", names)
	}
	if calls := readKubectlLog(t, logFile); strings.Count(calls, "/api/v1/pods?limit=250\n") != 2 {
		t.Errorf("expected exactly 2 page-1 reads (initial + restart); log:\n%s", calls)
	}
}

func TestKubectlListPaged_SecondExpiredContinueFails(t *testing.T) {
	logFile := installFakeKubectl(t, `case "$args" in
  *"/api/v1/pods?limit=250")
    echo '{"metadata":{"continue":"tok"},"items":[{"metadata":{"name":"p0"}}]}' ;;
  *"/api/v1/pods?continue=tok&limit=250")
    echo 'Error from server (Expired): The continue parameter is too old to display a consistent list result.' >&2
    exit 1 ;;
  *)
    echo "unexpected: $args" >&2; exit 1 ;;
esac
`)
	got, err := kubectlListPaged(context.Background(), nil, rawPods, metav1.NamespaceAll, "", kept[corev1.Pod])
	if err == nil || !isKubectlContinueExpired(err) {
		t.Fatalf("err = %v, want expired continue error", err)
	}
	if got != nil {
		t.Errorf("items = %d, want nil on failed restart", len(got))
	}
	if calls := readKubectlLog(t, logFile); strings.Count(calls, "/api/v1/pods?limit=250\n") != 2 {
		t.Errorf("expected exactly 2 page-1 reads (at most one restart); log:\n%s", calls)
	}
}

func TestIsKubectlContinueExpired(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errors.New("kubectl get pods: Error from server (Forbidden): pods is forbidden"), false},
		{errors.New("kubectl get pods: Error from server (NotFound): namespaces \"x\" not found"), false},
		{errors.New("kubectl get pods: Error from server (Expired): The continue parameter is too old"), true},
		{errors.New("kubectl get pods: Error from server (Gone): resource expired"), true},
		{errors.New("kubectl get pods: the server responded with the status code 410"), true},
		{errors.New("kubectl get pods: the continue parameter is too old to display a consistent list result"), true},
	} {
		if got := isKubectlContinueExpired(tc.err); got != tc.want {
			t.Errorf("isKubectlContinueExpired(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
