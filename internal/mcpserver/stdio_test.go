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
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type stubHandler struct {
	callTool func(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error)
	onClose  func()
}

func (s *stubHandler) ServerName() string    { return "leeway" }
func (s *stubHandler) ServerVersion() string { return "test" }
func (s *stubHandler) ListTools(context.Context) (json.RawMessage, error) {
	return json.RawMessage(`{"tools":[]}`), nil
}
func (s *stubHandler) CallTool(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	return s.callTool(ctx, name, args)
}
func (s *stubHandler) Close() error {
	if s.onClose != nil {
		s.onClose()
	}
	return nil
}

func TestServeStdio_CancelNotificationCancelsInFlightCallAndUnblocksNext(t *testing.T) {
	t.Parallel()

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()

	call1Started := make(chan struct{})
	call1Canceled := make(chan struct{})
	releaseCall1 := make(chan struct{})

	h := &stubHandler{
		callTool: func(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
			var p struct {
				Kind string `json:"kind"`
			}
			_ = json.Unmarshal(args, &p)
			if p.Kind == "slow" {
				close(call1Started)
				<-ctx.Done()
				close(call1Canceled)
				<-releaseCall1
				return TextResult("should never be written", false), nil
			}
			return TextResult("fast-ok", false), nil
		},
	}

	serveDone := make(chan error, 1)
	go func() {
		defer outW.Close()
		serveDone <- ServeStdio(context.Background(), inR, outW, h)
	}()

	responses := make(chan string, 16)
	go func() {
		defer close(responses)
		sc := bufio.NewScanner(outR)
		for sc.Scan() {
			responses <- sc.Text()
		}
	}()

	writeLine := func(line string) {
		t.Helper()
		if _, err := io.WriteString(inW, line+"\n"); err != nil {
			t.Fatalf("write stdin %q: %v", line, err)
		}
	}

	// 1. Start slow tools/call with numeric id 1.
	writeLine(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"report","arguments":{"kind":"slow"}}}`)
	select {
	case <-call1Started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for slow CallTool to start")
	}

	// 2. Ping while call 1 is in flight must respond promptly without waiting for call 1.
	writeLine(`{"jsonrpc":"2.0","id":2,"method":"ping"}`)
	select {
	case line := <-responses:
		if !strings.Contains(line, `"id":2`) {
			t.Fatalf("expected ping response id=2, got %s", line)
		}
	case <-time.After(1 * time.Second):
		close(releaseCall1)
		inW.Close()
		t.Fatal("ping blocked behind in-flight tools/call")
	}

	// 3. Cancel call 1 via notifications/cancelled.
	writeLine(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1,"reason":"client timeout"}}`)
	select {
	case <-call1Canceled:
	case <-time.After(1 * time.Second):
		close(releaseCall1)
		inW.Close()
		t.Fatal("notifications/cancelled did not cancel in-flight CallTool context")
	}

	// 4. Subsequent tools/call must complete promptly even while cancelled call 1 is still unwinding.
	writeLine(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"report","arguments":{"kind":"fast"}}}`)
	select {
	case line := <-responses:
		if !strings.Contains(line, `"id":3`) || !strings.Contains(line, "fast-ok") {
			t.Fatalf("expected fast response id=3, got %s", line)
		}
	case <-time.After(1 * time.Second):
		close(releaseCall1)
		inW.Close()
		t.Fatal("next tools/call blocked behind cancelled call 1")
	}

	// 5. Release cancelled call 1, close stdin, and verify NO response was written for id=1.
	close(releaseCall1)
	_ = inW.Close()

	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("ServeStdio returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeStdio did not finish after stdin EOF")
	}

	for line := range responses {
		t.Errorf("unexpected extra JSON-RPC response (cancelled id=1 must not emit a response): %s", line)
	}
}

func TestServeStdio_CancelNotificationStringIDTypeSafetyAndNoOps(t *testing.T) {
	t.Parallel()

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()

	started := make(chan struct{})
	canceled := make(chan struct{})

	h := &stubHandler{
		callTool: func(ctx context.Context, _ string, _ json.RawMessage) (json.RawMessage, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			return TextResult("late", false), nil
		},
	}

	serveDone := make(chan error, 1)
	go func() {
		defer outW.Close()
		serveDone <- ServeStdio(context.Background(), inR, outW, h)
	}()

	responses := make(chan string, 16)
	go func() {
		defer close(responses)
		sc := bufio.NewScanner(outR)
		for sc.Scan() {
			responses <- sc.Text()
		}
	}()

	writeLine := func(line string) {
		t.Helper()
		if _, err := io.WriteString(inW, line+"\n"); err != nil {
			t.Fatalf("write stdin: %v", err)
		}
	}

	// String id "1" must NOT match numeric requestId 1.
	writeLine(`{"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"report","arguments":{}}}`)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("CallTool did not start")
	}

	// Numeric 1, unknown id, malformed params, and missing requestId are all ignored.
	writeLine(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}`)
	writeLine(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"unknown"}}`)
	writeLine(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":"malformed"}`)
	writeLine(`{"jsonrpc":"2.0","method":"notifications/cancelled"}`)
	writeLine(`{"jsonrpc":"2.0","id":99,"method":"ping"}`)

	select {
	case line := <-responses:
		if !strings.Contains(line, `"id":99`) {
			t.Fatalf("expected ping response id=99, got %s", line)
		}
	case <-time.After(1 * time.Second):
		_ = inW.Close()
		t.Fatal("timed out waiting for ping")
	}
	select {
	case <-canceled:
		t.Fatal(`numeric requestId 1 wrongly cancelled string id "1"`)
	default:
	}

	// Matching string requestId "1" cancels it; duplicate cancel is a safe no-op.
	writeLine(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"1"}}`)
	writeLine(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"1"}}`)

	select {
	case <-canceled:
	case <-time.After(1 * time.Second):
		_ = inW.Close()
		t.Fatal(`string requestId "1" did not cancel string id "1"`)
	}

	_ = inW.Close()
	if err := <-serveDone; err != nil {
		t.Fatalf("ServeStdio: %v", err)
	}
	for line := range responses {
		t.Errorf("unexpected response after cancel: %s", line)
	}
}

func TestServeStdio_ParentCancelCancelsInFlightCallBeforeClose(t *testing.T) {
	t.Parallel()

	inR, inW := io.Pipe()
	defer inW.Close() // remains open like SIGTERM with open os.Stdin
	outR, outW := io.Pipe()
	defer outR.Close()

	var (
		started        = make(chan struct{})
		callExited     atomic.Bool
		closedEarly    atomic.Bool
		responseBuffer bytes.Buffer
		respDone       = make(chan struct{})
	)
	go func() {
		defer close(respDone)
		_, _ = io.Copy(&responseBuffer, outR)
	}()

	h := &stubHandler{
		callTool: func(ctx context.Context, _ string, _ json.RawMessage) (json.RawMessage, error) {
			close(started)
			<-ctx.Done()
			time.Sleep(25 * time.Millisecond)
			callExited.Store(true)
			return TextResult("should-not-send", false), nil
		},
		onClose: func() {
			if !callExited.Load() {
				closedEarly.Store(true)
			}
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveDone := make(chan error, 1)
	go func() {
		defer outW.Close()
		serveDone <- ServeStdio(ctx, inR, outW, h)
	}()

	if _, err := io.WriteString(inW, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"report","arguments":{}}}`+"\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("CallTool did not start")
	}

	cancel()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("ServeStdio returned error on parent cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeStdio hung after parent cancel with in-flight call")
	}

	<-respDone
	if !callExited.Load() {
		t.Error("ServeStdio returned before in-flight CallTool exited")
	}
	if closedEarly.Load() {
		t.Error("Handler.Close was called before in-flight CallTool exited")
	}
	if responseBuffer.Len() != 0 {
		t.Errorf("unexpected response written after parent cancel: %q", responseBuffer.String())
	}
}

func TestServeStdio_EOFWaitsForConcurrentCallsWithoutInterleaving(t *testing.T) {
	t.Parallel()

	var (
		active      atomic.Int32
		closedEarly atomic.Bool
	)
	h := &stubHandler{
		callTool: func(_ context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
			active.Add(1)
			defer active.Add(-1)
			time.Sleep(20 * time.Millisecond)
			return TextResult(fmt.Sprintf("ok:%s", bytes.TrimSpace(args)), false), nil
		},
		onClose: func() {
			if active.Load() != 0 {
				closedEarly.Store(true)
			}
		},
	}

	var input strings.Builder
	const n = 8
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&input, `{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":"report","arguments":{"n":%d}}}`+"\n", i, i)
	}

	var out bytes.Buffer
	if err := ServeStdio(context.Background(), strings.NewReader(input.String()), &out, h); err != nil {
		t.Fatalf("ServeStdio: %v", err)
	}
	if closedEarly.Load() {
		t.Error("Handler.Close ran while CallTool goroutines were still active")
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != n {
		t.Fatalf("got %d responses, want %d:\n%s", len(lines), n, out.String())
	}
	seenIDs := make(map[int]bool, n)
	for _, line := range lines {
		var resp struct {
			ID int `json:"id"`
		}
		if err := json.Unmarshal([]byte(line), &resp); err != nil {
			t.Fatalf("corrupted/interleaved JSON-RPC line %q: %v", line, err)
		}
		seenIDs[resp.ID] = true
	}
	for i := 1; i <= n; i++ {
		if !seenIDs[i] {
			t.Errorf("missing response for id=%d", i)
		}
	}
}

func TestServeStdio_DuplicateInFlightRequestIDRejected(t *testing.T) {
	t.Parallel()

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()

	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32

	h := &stubHandler{
		callTool: func(_ context.Context, _ string, _ json.RawMessage) (json.RawMessage, error) {
			if calls.Add(1) == 1 {
				close(started)
				<-release
			}
			return TextResult("first-ok", false), nil
		},
	}

	serveDone := make(chan error, 1)
	go func() {
		defer outW.Close()
		serveDone <- ServeStdio(context.Background(), inR, outW, h)
	}()

	gotLine := make(chan string, 4)
	go func() {
		defer close(gotLine)
		sc := bufio.NewScanner(outR)
		for sc.Scan() {
			gotLine <- sc.Text()
		}
	}()

	_, _ = io.WriteString(inW, `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"report","arguments":{}}}`+"\n")
	<-started
	_, _ = io.WriteString(inW, `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"report","arguments":{}}}`+"\n")

	select {
	case dup := <-gotLine:
		if !strings.Contains(dup, "-32600") {
			t.Fatalf("expected -32600 for duplicate in-flight id, got %s", dup)
		}
	case <-time.After(1 * time.Second):
		close(release)
		_ = inW.Close()
		t.Fatal("timed out waiting for duplicate-id rejection")
	}

	close(release)
	_ = inW.Close()

	select {
	case ok := <-gotLine:
		if !strings.Contains(ok, "first-ok") {
			t.Fatalf("expected original call result, got %s", ok)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for first call to finish")
	}
	if err := <-serveDone; err != nil {
		t.Fatalf("ServeStdio: %v", err)
	}
	if calls.Load() != 1 {
		t.Errorf("CallTool invoked %d times, want 1", calls.Load())
	}
}

type failingWriter struct {
	err error
}

func (f failingWriter) Write([]byte) (int, error) {
	return 0, f.err
}

func TestServeStdio_WriteErrorCancelsInFlightAndReturnsError(t *testing.T) {
	t.Parallel()

	inR, inW := io.Pipe()
	defer inW.Close()

	slowStarted := make(chan struct{})
	slowCanceled := make(chan struct{})
	boom := errors.New("broken stdout pipe")

	h := &stubHandler{
		callTool: func(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
			if strings.Contains(string(args), "slow") {
				close(slowStarted)
				<-ctx.Done()
				close(slowCanceled)
				return TextResult("late", false), nil
			}
			return TextResult("trigger-write", false), nil
		},
	}

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- ServeStdio(context.Background(), inR, failingWriter{err: boom}, h)
	}()

	_, _ = io.WriteString(inW, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"report","arguments":{"kind":"slow"}}}`+"\n")
	select {
	case <-slowStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("slow call did not start")
	}

	// Second call triggers failingWriter; ServeStdio must cancel slow call and return boom.
	_, _ = io.WriteString(inW, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"report","arguments":{"kind":"fast"}}}`+"\n")

	select {
	case <-slowCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("write error did not cancel in-flight sibling call")
	}
	select {
	case err := <-serveDone:
		if !errors.Is(err, boom) {
			t.Fatalf("ServeStdio error = %v, want %v", err, boom)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeStdio did not return after write error")
	}
}

func TestServeStdio_SequentialRequestIDReuseNeverSpuriouslyRejects(t *testing.T) {
	t.Parallel()

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()

	h := &stubHandler{
		callTool: func(_ context.Context, _ string, _ json.RawMessage) (json.RawMessage, error) {
			return TextResult("ok", false), nil
		},
	}

	serveDone := make(chan error, 1)
	go func() {
		defer outW.Close()
		serveDone <- ServeStdio(context.Background(), inR, outW, h)
	}()

	sc := bufio.NewScanner(outR)
	for i := range 50 {
		if _, err := io.WriteString(inW, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"report","arguments":{}}}`+"\n"); err != nil {
			t.Fatalf("iter %d write: %v", i, err)
		}
		if !sc.Scan() {
			t.Fatalf("iter %d EOF reading response: %v", i, sc.Err())
		}
		if strings.Contains(sc.Text(), "-32600") || !strings.Contains(sc.Text(), `"ok"`) {
			t.Fatalf("iter %d unexpected response on sequential id reuse: %s", i, sc.Text())
		}
	}

	_ = inW.Close()
	if err := <-serveDone; err != nil {
		t.Fatalf("ServeStdio: %v", err)
	}
}

func TestRequestIDKey_Canonicalization(t *testing.T) {
	t.Parallel()

	samePairs := [][2]string{
		{`1`, `1.0`},
		{`1000000`, `1000000.0`},
		{`1000000`, `1e6`},
		{`0`, `-0.0`},
		{`"abc"`, `"\u0061bc"`},
	}
	for _, pair := range samePairs {
		k1, ok1 := requestIDKey(json.RawMessage(pair[0]))
		k2, ok2 := requestIDKey(json.RawMessage(pair[1]))
		if !ok1 || !ok2 || k1 != k2 {
			t.Errorf("requestIDKey(%s)=(%q,%v) vs requestIDKey(%s)=(%q,%v), want equal", pair[0], k1, ok1, pair[1], k2, ok2)
		}
	}

	diffPairs := [][2]string{
		{`1`, `"1"`},
		{`9007199254740992`, `9007199254740993`},
	}
	for _, pair := range diffPairs {
		k1, ok1 := requestIDKey(json.RawMessage(pair[0]))
		k2, ok2 := requestIDKey(json.RawMessage(pair[1]))
		if !ok1 || !ok2 || k1 == k2 {
			t.Errorf("requestIDKey(%s)=(%q,%v) vs requestIDKey(%s)=(%q,%v), want distinct", pair[0], k1, ok1, pair[1], k2, ok2)
		}
	}

	for _, invalid := range []string{``, `   `, `null`} {
		if k, ok := requestIDKey(json.RawMessage(invalid)); ok {
			t.Errorf("requestIDKey(%q) = (%q, true), want false", invalid, k)
		}
	}
}
