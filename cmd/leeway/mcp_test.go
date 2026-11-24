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

package main

import (
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestMCPFlagValidation(t *testing.T) {
	t.Parallel()

	run := func(args ...string) (int, string, string) {
		var out, errb bytes.Buffer
		code := runMCPCmd(args, strings.NewReader(""), &out, &errb)
		return code, out.String(), errb.String()
	}

	if code, out, _ := run("--help"); code != 0 || out != "" {
		t.Errorf("mcp --help = (%d, %q), want (0, \"\")", code, out)
	}

	for _, bad := range [][]string{
		{"--unknown-flag"},
		{"unexpected-arg"},
		{"--parallelism=0"},
		{"--timeout=0s"},
		{"--google-auth=bogus"},
		{"--sources=bogus"},
		{"--sources=,"},
		{"--topology-keys=,"},
		{"--topology-cluster-defaults=malformed"},
	} {
		if code, out, _ := run(bad...); code != 2 || out != "" {
			t.Errorf("mcp %v = (%d, stdout=%q), want exit 2 and empty stdout", bad, code, out)
		}
	}
}

// TestMCPSubprocess_SIGTERMWithOpenStdin verifies that `leeway mcp` exits
// cleanly on SIGTERM even when its parent keeps os.Stdin open (matching how
// core-agent terminates stdio MCP servers).
func TestMCPSubprocess_SIGTERMWithOpenStdin(t *testing.T) {
	if os.Getenv("LEEWAY_TEST_MCP_SUBPROCESS") == "1" {
		os.Exit(realMain([]string{"mcp"}, os.Stdout, os.Stderr))
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	cmd := exec.Command(exe, "-test.run=^TestMCPSubprocess_SIGTERMWithOpenStdin$")
	cmd.Env = append(os.Environ(),
		"LEEWAY_TEST_MCP_SUBPROCESS=1",
		"KUBECONFIG="+filepath.Join(t.TempDir(), "missing-kubeconfig-proves-offline-startup"),
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe: %v", err)
	}
	defer stdin.Close()

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Send initialize and tools/list while keeping stdin open.
	reqs := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
	}, "\n") + "\n"
	if _, err := stdin.Write([]byte(reqs)); err != nil {
		t.Fatalf("write stdin: %v", err)
	}

	sc := bufio.NewScanner(stdout)
	for i, want := range []string{`"name":"leeway"`, `"name":"report"`} {
		if !sc.Scan() {
			t.Fatalf("EOF waiting for response %d: err=%v stderr=%s", i, sc.Err(), stderr.String())
		}
		if !strings.Contains(sc.Text(), want) {
			t.Errorf("response %d = %s, want substring %q", i, sc.Text(), want)
		}
	}

	// Send SIGTERM without closing stdin; process must exit 0 promptly.
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("Signal(SIGTERM): %v", err)
	}

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	select {
	case waitErr := <-waitCh:
		if waitErr != nil {
			t.Fatalf("leeway mcp exited non-zero on SIGTERM: %v (stderr=%s)", waitErr, stderr.String())
		}
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("leeway mcp hung after SIGTERM with open stdin (stderr=%s)", stderr.String())
	}
}
