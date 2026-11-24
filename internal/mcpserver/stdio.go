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
	"slices"
	"strings"
	"sync"
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

// ServeStdio serves newline-delimited MCP JSON-RPC 2.0 requests from r to w.
// It returns promptly when ctx is canceled (for example on SIGTERM from
// core-agent while os.Stdin remains open) without blocking on a pending read.
func ServeStdio(ctx context.Context, r io.Reader, w io.Writer, h Handler) error {
	defer func() { _ = h.Close() }()

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

	var writeMu sync.Mutex
	send := func(resp rpcResponse) error {
		resp.JSONRPC = "2.0"
		b, err := marshalNoEscape(resp)
		if err != nil {
			return err
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		_, err = w.Write(b)
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case frame, ok := <-frames:
			if !ok {
				return nil
			}
			if frame.err != nil {
				return frame.err
			}
			var req rpcRequest
			if err := json.Unmarshal(frame.line, &req); err != nil {
				if werr := send(rpcResponse{
					ID:    json.RawMessage("null"),
					Error: &rpcError{Code: -32700, Message: "parse error: " + err.Error()},
				}); werr != nil {
					return werr
				}
				continue
			}
			if isNotification(req) {
				continue
			}
			if strings.TrimSpace(req.Method) == "" {
				if werr := send(rpcResponse{
					ID:    req.ID,
					Error: &rpcError{Code: -32600, Message: "invalid request: missing method"},
				}); werr != nil {
					return werr
				}
				continue
			}
			resp := dispatchRequest(ctx, h, req)
			if err := send(resp); err != nil {
				return err
			}
		}
	}
}

func isNotification(req rpcRequest) bool {
	if strings.HasPrefix(req.Method, "notifications/") {
		return true
	}
	trimmed := bytes.TrimSpace(req.ID)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}

func dispatchRequest(ctx context.Context, h Handler, req rpcRequest) rpcResponse {
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
	case "tools/call":
		var p struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil || strings.TrimSpace(p.Name) == "" {
			return rpcResponse{ID: req.ID, Error: &rpcError{Code: -32602, Message: "invalid tools/call params"}}
		}
		res, err := h.CallTool(ctx, p.Name, p.Arguments)
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
