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

// Package mcpserver implements the `leeway mcp` stdio Model Context Protocol
// server exposing `leeway report` as a read-only tool.
package mcpserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	// DefaultProtocolVersion is the fallback MCP protocol version returned when
	// an initialize request omits protocolVersion.
	DefaultProtocolVersion = "2024-11-05"
	maxJSONRPCLineBytes    = 16 << 20 // 16 MiB
)

// Handler serves MCP JSON-RPC requests over ServeStdio.
type Handler interface {
	ServerName() string
	ServerVersion() string
	ListTools(ctx context.Context) (json.RawMessage, error)
	CallTool(ctx context.Context, name string, arguments json.RawMessage) (json.RawMessage, error)
	Close() error
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type scanFrame struct {
	line []byte
	err  error
}

type inflightRequest struct {
	cancel    context.CancelFunc
	cancelled atomic.Bool
}

type inflightTracker struct {
	mu   sync.Mutex
	byID map[string]*inflightRequest
}

func newInflightTracker() *inflightTracker {
	return &inflightTracker{byID: make(map[string]*inflightRequest)}
}

func (t *inflightTracker) register(parent context.Context, idKey string) (context.Context, *inflightRequest, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.byID[idKey]; exists {
		return nil, nil, false
	}
	reqCtx, cancel := context.WithCancel(parent)
	entry := &inflightRequest{cancel: cancel}
	t.byID[idKey] = entry
	return reqCtx, entry, true
}

func (t *inflightTracker) cancel(idKey string) {
	t.mu.Lock()
	entry, exists := t.byID[idKey]
	if exists {
		entry.cancelled.Store(true)
		delete(t.byID, idKey)
	}
	t.mu.Unlock()
	if exists {
		entry.cancel()
	}
}

// commitSend atomically verifies that entry has not been cancelled and removes
// it from the in-flight map before its response is written to stdout, so a
// client reusing the same request ID immediately after reading the response
// cannot hit a spurious duplicate-ID rejection.
func (t *inflightTracker) commitSend(idKey string, entry *inflightRequest) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if entry.cancelled.Load() {
		return false
	}
	if t.byID[idKey] == entry {
		delete(t.byID, idKey)
	}
	return true
}

func (t *inflightTracker) remove(idKey string, entry *inflightRequest) {
	t.mu.Lock()
	if t.byID[idKey] == entry {
		delete(t.byID, idKey)
	}
	t.mu.Unlock()
}

// ServeStdio serves newline-delimited MCP JSON-RPC 2.0 requests from r to w.
// Tool calls run asynchronously so incoming notifications/cancelled messages and
// subsequent requests are never blocked behind an in-flight call. It returns
// promptly when ctx is canceled (for example on SIGTERM from core-agent while
// os.Stdin remains open) after canceling and waiting for in-flight tool calls.
func ServeStdio(ctx context.Context, r io.Reader, w io.Writer, h Handler) error {
	defer func() { _ = h.Close() }()

	serveCtx, cancelServe := context.WithCancel(ctx)
	defer cancelServe()

	sess := &stdioSession{
		parentCtx:  ctx,
		serveCtx:   serveCtx,
		cancel:     cancelServe,
		w:          w,
		h:          h,
		writeErrCh: make(chan error, 1),
		tracker:    newInflightTracker(),
	}
	frames := startFrameScanner(serveCtx, r)

	for {
		if serveCtx.Err() != nil {
			return sess.shutdown(nil)
		}
		select {
		case <-serveCtx.Done():
			return sess.shutdown(nil)
		case frame, ok := <-frames:
			if !ok {
				return sess.shutdown(nil)
			}
			if frame.err != nil {
				cancelServe()
				return sess.shutdown(frame.err)
			}
			if err := sess.handleLine(frame.line); err != nil {
				cancelServe()
				return sess.shutdown(err)
			}
		}
	}
}

type stdioSession struct {
	parentCtx  context.Context
	serveCtx   context.Context
	cancel     context.CancelFunc
	w          io.Writer
	h          Handler
	writeMu    sync.Mutex
	wg         sync.WaitGroup
	writeErrCh chan error
	tracker    *inflightTracker
}

func startFrameScanner(ctx context.Context, r io.Reader) <-chan scanFrame {
	frames := make(chan scanFrame, 1)
	go func() {
		defer close(frames)
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 64*1024), maxJSONRPCLineBytes)
		for sc.Scan() {
			line := slices.Clone(bytes.TrimSpace(sc.Bytes()))
			if len(line) == 0 {
				continue
			}
			select {
			case frames <- scanFrame{line: line}:
			case <-ctx.Done():
				return
			}
		}
		if err := sc.Err(); err != nil {
			select {
			case frames <- scanFrame{err: err}:
			case <-ctx.Done():
			}
		}
	}()
	return frames
}

func (s *stdioSession) recordWriteErr(err error) {
	select {
	case s.writeErrCh <- err:
	default:
	}
	s.cancel()
}

func (s *stdioSession) send(resp rpcResponse) error {
	resp.JSONRPC = "2.0"
	b, err := marshalNoEscape(resp)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.serveCtx.Err() != nil {
		return nil
	}
	_, err = s.w.Write(b)
	return err
}

func (s *stdioSession) sendToolResponse(idKey string, entry *inflightRequest, resp rpcResponse) error {
	resp.JSONRPC = "2.0"
	b, err := marshalNoEscape(resp)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.serveCtx.Err() != nil || !s.tracker.commitSend(idKey, entry) {
		return nil
	}
	_, err = s.w.Write(b)
	return err
}

func (s *stdioSession) shutdown(loopErr error) error {
	s.wg.Wait()
	if s.parentCtx.Err() != nil {
		return nil
	}
	if loopErr != nil {
		return loopErr
	}
	select {
	case werr := <-s.writeErrCh:
		return werr
	default:
		return nil
	}
}

func (s *stdioSession) handleLine(line []byte) error {
	var req rpcRequest
	if err := json.Unmarshal(line, &req); err != nil {
		return s.send(rpcResponse{
			ID:    json.RawMessage("null"),
			Error: &rpcError{Code: -32700, Message: "parse error: " + err.Error()},
		})
	}
	if req.Method == "notifications/cancelled" {
		handleCancelNotification(s.tracker, req.Params)
		return nil
	}
	if isNotification(req) {
		return nil
	}
	if strings.TrimSpace(req.Method) == "" {
		return s.send(rpcResponse{
			ID:    req.ID,
			Error: &rpcError{Code: -32600, Message: "invalid request: missing method"},
		})
	}
	if req.Method == "tools/call" {
		return s.dispatchToolCall(req)
	}
	return s.send(dispatchSyncRequest(s.serveCtx, s.h, req))
}

func (s *stdioSession) dispatchToolCall(req rpcRequest) error {
	var p struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(req.Params, &p); err != nil || strings.TrimSpace(p.Name) == "" {
		return s.send(rpcResponse{
			ID:    req.ID,
			Error: &rpcError{Code: -32602, Message: "invalid tools/call params"},
		})
	}
	idKey, ok := requestIDKey(req.ID)
	if !ok {
		return s.send(rpcResponse{
			ID:    req.ID,
			Error: &rpcError{Code: -32600, Message: "invalid request: invalid id"},
		})
	}
	reqCtx, entry, registered := s.tracker.register(s.serveCtx, idKey)
	if !registered {
		return s.send(rpcResponse{
			ID:    req.ID,
			Error: &rpcError{Code: -32600, Message: "invalid request: duplicate in-flight request id"},
		})
	}

	id := slices.Clone(req.ID)
	args := slices.Clone(p.Arguments)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			entry.cancel()
			s.tracker.remove(idKey, entry)
		}()
		if entry.cancelled.Load() || reqCtx.Err() != nil {
			return
		}
		res, err := s.h.CallTool(reqCtx, p.Name, args)
		if entry.cancelled.Load() || s.serveCtx.Err() != nil {
			return
		}
		var resp rpcResponse
		if err != nil {
			resp = rpcResponse{ID: id, Error: &rpcError{Code: -32603, Message: err.Error()}}
		} else {
			resp = rpcResponse{ID: id, Result: res}
		}
		if werr := s.sendToolResponse(idKey, entry, resp); werr != nil {
			s.recordWriteErr(werr)
		}
	}()
	return nil
}

func handleCancelNotification(tracker *inflightTracker, rawParams json.RawMessage) {
	if len(rawParams) == 0 {
		return
	}
	var p struct {
		RequestID json.RawMessage `json:"requestId"`
	}
	if err := json.Unmarshal(rawParams, &p); err != nil {
		return
	}
	if idKey, ok := requestIDKey(p.RequestID); ok {
		tracker.cancel(idKey)
	}
}

func requestIDKey(raw json.RawMessage) (string, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", false
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", false
	}
	switch id := v.(type) {
	case string:
		return "s:" + id, true
	case json.Number:
		if i, err := strconv.ParseInt(id.String(), 10, 64); err == nil {
			return "n:" + strconv.FormatInt(i, 10), true
		}
		if f, err := strconv.ParseFloat(id.String(), 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			if f == 0 {
				return "n:0", true
			}
			if f == math.Trunc(f) && f >= -9223372036854775808.0 && f < 9223372036854775808.0 {
				return "n:" + strconv.FormatInt(int64(f), 10), true
			}
			return "n:" + strconv.FormatFloat(f, 'g', -1, 64), true
		}
		return "n:" + id.String(), true
	default:
		var compact bytes.Buffer
		if err := json.Compact(&compact, trimmed); err != nil {
			return "", false
		}
		return "raw:" + compact.String(), true
	}
}

func isNotification(req rpcRequest) bool {
	if strings.HasPrefix(req.Method, "notifications/") {
		return true
	}
	trimmed := bytes.TrimSpace(req.ID)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}

func dispatchSyncRequest(ctx context.Context, h Handler, req rpcRequest) rpcResponse {
	switch req.Method {
	case "initialize":
		proto := DefaultProtocolVersion
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		if len(req.Params) > 0 && json.Unmarshal(req.Params, &p) == nil && strings.TrimSpace(p.ProtocolVersion) != "" {
			proto = strings.TrimSpace(p.ProtocolVersion)
		}
		ver := strings.TrimSpace(h.ServerVersion())
		if ver == "" {
			ver = "0.1.0"
		}
		return rpcResponse{
			ID: req.ID,
			Result: map[string]any{
				"protocolVersion": proto,
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]string{"name": h.ServerName(), "version": ver},
			},
		}
	case "ping":
		return rpcResponse{ID: req.ID, Result: map[string]any{}}
	case "resources/list":
		return rpcResponse{ID: req.ID, Result: map[string]any{"resources": []any{}}}
	case "prompts/list":
		return rpcResponse{ID: req.ID, Result: map[string]any{"prompts": []any{}}}
	case "tools/list":
		res, err := h.ListTools(ctx)
		if err != nil {
			return rpcResponse{ID: req.ID, Error: &rpcError{Code: -32603, Message: err.Error()}}
		}
		return rpcResponse{ID: req.ID, Result: res}
	default:
		return rpcResponse{
			ID:    req.ID,
			Error: &rpcError{Code: -32601, Message: fmt.Sprintf("method %q not found", req.Method)},
		}
	}
}

// TextResult encodes a text MCP CallToolResult payload.
func TextResult(text string, isErr bool) json.RawMessage {
	payload := map[string]any{
		"content": []map[string]string{{"type": "text", "text": text}},
	}
	if isErr {
		payload["isError"] = true
	}
	b, _ := marshalNoEscape(payload)
	return bytes.TrimSuffix(b, []byte("\n"))
}

func marshalNoEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
