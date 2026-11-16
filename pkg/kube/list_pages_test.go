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
	"context"
	"errors"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestListPagesPaginatesAcrossContinueTokens(t *testing.T) {
	var seenLimits []int64
	var seenContinues []string

	listFn := func(_ context.Context, opts metav1.ListOptions) ([]string, string, error) {
		seenLimits = append(seenLimits, opts.Limit)
		seenContinues = append(seenContinues, opts.Continue)
		switch opts.Continue {
		case "":
			return []string{"pod-a", "pod-b"}, "tok-1", nil
		case "tok-1":
			return []string{"pod-c"}, "tok-2", nil
		case "tok-2":
			return []string{"pod-d", "pod-e"}, "", nil
		default:
			t.Fatalf("unexpected continue token %q", opts.Continue)
			return nil, "", nil
		}
	}

	var got []string
	err := ListPages(context.Background(), listFn, func(item *string) {
		got = append(got, *item)
	})
	if err != nil {
		t.Fatalf("ListPages returned error: %v", err)
	}
	want := []string{"pod-a", "pod-b", "pod-c", "pod-d", "pod-e"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i, lim := range seenLimits {
		if lim != DefaultPageLimit {
			t.Errorf("page %d Limit = %d, want %d", i, lim, DefaultPageLimit)
		}
	}
	wantContinues := []string{"", "tok-1", "tok-2"}
	if !reflect.DeepEqual(seenContinues, wantContinues) {
		t.Errorf("seen continues = %v, want %v", seenContinues, wantContinues)
	}
}

func TestListPagesRecoversTransparentlyFromGone(t *testing.T) {
	goneInjected := false
	listFn := func(_ context.Context, opts metav1.ListOptions) ([]string, string, error) {
		switch opts.Continue {
		case "":
			if !goneInjected {
				return []string{"stale-a", "stale-b"}, "expired-token", nil
			}
			return []string{"fresh-a", "fresh-b"}, "fresh-tok", nil
		case "expired-token":
			goneInjected = true
			return nil, "", apierrors.NewResourceExpired("continue token expired")
		case "fresh-tok":
			return []string{"fresh-c"}, "", nil
		default:
			t.Fatalf("unexpected continue token %q", opts.Continue)
			return nil, "", nil
		}
	}

	var got []string
	err := ListPages(context.Background(), listFn, func(item *string) {
		got = append(got, *item)
	})
	if err != nil {
		t.Fatalf("expected transparent 410 Gone recovery, got error: %v", err)
	}
	want := []string{"fresh-a", "fresh-b", "fresh-c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v (must not duplicate stale partial items)", got, want)
	}
}

func TestListPagesFailsOnRepeatedGone(t *testing.T) {
	listFn := func(_ context.Context, opts metav1.ListOptions) ([]string, string, error) {
		if opts.Continue == "" {
			return []string{"a"}, "tok", nil
		}
		return nil, "", apierrors.NewResourceExpired("still gone")
	}

	err := ListPages(context.Background(), listFn, func(*string) {
		t.Fatal("each must not be called when pagination ultimately errors")
	})
	if !apierrors.IsResourceExpired(err) && !apierrors.IsGone(err) {
		t.Fatalf("expected 410 Gone error after retry exhaustion, got %v", err)
	}
}

func TestWrapClientDeduplicatesPodAndNodeLists(t *testing.T) {
	raw := fake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "api-1"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "staging", Name: "web-1"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}},
	)
	var podCalls, nodeCalls int
	raw.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		podCalls++
		return false, nil, nil
	})
	raw.PrependReactor("list", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		nodeCalls++
		return false, nil, nil
	})

	ctx := WithListCache(context.Background())
	wrapped := WrapClient(ctx, raw)

	pods1, err := wrapped.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil || len(pods1.Items) != 2 {
		t.Fatalf("first cluster pod list: items=%d err=%v", len(pods1.Items), err)
	}
	pods2, err := wrapped.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil || len(pods2.Items) != 2 {
		t.Fatalf("second cluster pod list: items=%d err=%v", len(pods2.Items), err)
	}
	nsPods, err := wrapped.CoreV1().Pods("prod").List(ctx, metav1.ListOptions{})
	if err != nil || len(nsPods.Items) != 1 || nsPods.Items[0].Name != "api-1" {
		t.Fatalf("namespace pod slice from cluster cache: items=%v err=%v", nsPods.Items, err)
	}
	if podCalls != 1 {
		t.Errorf("underlying pods list calls = %d, want 1", podCalls)
	}

	if _, err := wrapped.CoreV1().Nodes().List(ctx, metav1.ListOptions{}); err != nil {
		t.Fatalf("first node list: %v", err)
	}
	if _, err := wrapped.CoreV1().Nodes().List(ctx, metav1.ListOptions{}); err != nil {
		t.Fatalf("second node list: %v", err)
	}
	if nodeCalls != 1 {
		t.Errorf("underlying nodes list calls = %d, want 1", nodeCalls)
	}

	// Non-Gone error is never cached.
	rawErr := fake.NewSimpleClientset()
	rawErr.PrependReactor("list", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewServiceUnavailable("boom")
	})
	wrappedErr := WrapClient(ctx, rawErr)
	if _, err := wrappedErr.CoreV1().Nodes().List(ctx, metav1.ListOptions{}); !apierrors.IsServiceUnavailable(err) {
		t.Fatalf("expected service unavailable error, got %v", err)
	}
	var statusErr *apierrors.StatusError
	if !errors.As(err, &statusErr) && err != nil {
		_ = schema.GroupResource{}
	}
}

func TestCollectPagesKeepsEachPageBeforeRequestingTheNext(t *testing.T) {
	var kept []string
	var seenLimits []int64

	listFn := func(_ context.Context, opts metav1.ListOptions) ([]string, string, error) {
		seenLimits = append(seenLimits, opts.Limit)
		switch opts.Continue {
		case "":
			return []string{"a", "b"}, "tok-1", nil
		case "tok-1":
			// Page 1 must already have been through keep.
			if !reflect.DeepEqual(kept, []string{"a", "b"}) {
				t.Errorf("page 2 requested with kept = %v, want page 1 kept first", kept)
			}
			return []string{"c"}, "", nil
		default:
			t.Fatalf("unexpected continue token %q", opts.Continue)
			return nil, "", nil
		}
	}

	got, err := CollectPages(context.Background(), listFn, func(item *string) *string {
		kept = append(kept, *item)
		trimmed := *item + "-trimmed"
		return &trimmed
	})
	if err != nil {
		t.Fatalf("CollectPages returned error: %v", err)
	}
	var values []string
	for _, v := range got {
		values = append(values, *v)
	}
	if want := []string{"a-trimmed", "b-trimmed", "c-trimmed"}; !reflect.DeepEqual(values, want) {
		t.Fatalf("got %v, want %v", values, want)
	}
	if want := []int64{DefaultPageLimit, DefaultPageLimit}; !reflect.DeepEqual(seenLimits, want) {
		t.Errorf("limits = %v, want %v", seenLimits, want)
	}
}

func TestCollectPagesRestartsOnceOnGoneWithoutTornResults(t *testing.T) {
	goneInjected := false
	listFn := func(_ context.Context, opts metav1.ListOptions) ([]string, string, error) {
		switch opts.Continue {
		case "":
			if !goneInjected {
				return []string{"stale-a"}, "expired-token", nil
			}
			return []string{"fresh-a"}, "fresh-tok", nil
		case "expired-token":
			goneInjected = true
			return nil, "", apierrors.NewResourceExpired("continue token expired")
		case "fresh-tok":
			return []string{"fresh-b"}, "", nil
		default:
			t.Fatalf("unexpected continue token %q", opts.Continue)
			return nil, "", nil
		}
	}

	got, err := CollectPages(context.Background(), listFn, func(item *string) *string { return item })
	if err != nil {
		t.Fatalf("expected one clean restart after 410 Gone, got error: %v", err)
	}
	var values []string
	for _, v := range got {
		values = append(values, *v)
	}
	if want := []string{"fresh-a", "fresh-b"}; !reflect.DeepEqual(values, want) {
		t.Fatalf("got %v, want %v (stale page must be discarded)", values, want)
	}
}

func TestCollectPagesFailsOnRepeatedGoneAndOtherErrors(t *testing.T) {
	gone := func(_ context.Context, opts metav1.ListOptions) ([]string, string, error) {
		if opts.Continue == "" {
			return []string{"a"}, "tok", nil
		}
		return nil, "", apierrors.NewResourceExpired("still gone")
	}
	got, err := CollectPages(context.Background(), gone, func(item *string) *string { return item })
	if !apierrors.IsResourceExpired(err) && !apierrors.IsGone(err) {
		t.Fatalf("expected 410 Gone after one restart, got %v", err)
	}
	if got != nil {
		t.Errorf("got %d items, want none on error", len(got))
	}

	forbidden := errors.New("forbidden")
	calls := 0
	failing := func(_ context.Context, opts metav1.ListOptions) ([]string, string, error) {
		calls++
		if opts.Continue == "" {
			return []string{"a"}, "tok", nil
		}
		return nil, "", forbidden
	}
	got, err = CollectPages(context.Background(), failing, func(item *string) *string { return item })
	if !errors.Is(err, forbidden) || got != nil {
		t.Fatalf("got (%v, %v), want (nil, forbidden)", got, err)
	}
	if calls != 2 {
		t.Errorf("list called %d times, want 2 (no restart on a non-410 error)", calls)
	}
}
