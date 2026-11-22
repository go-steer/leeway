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
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-steer/leeway/pkg/report"
)

type boolFlag interface {
	flag.Value
	IsBoolFlag() bool
}

// TestReportFlagSurfaceFrozen pins the `leeway report` CLI flag surface
// promised in docs/stability-policy.md §4 so removing, renaming, or changing a
// flag's bool/value syntax or default fails CI.
func TestReportFlagSurfaceFrozen(t *testing.T) {
	t.Parallel()

	type wantFlag struct {
		defVal string
		isBool bool
	}
	want := map[string]wantFlag{
		"clusters":                         {defVal: "", isBool: false},
		"clusters-from":                    {defVal: "", isBool: false},
		"profiles-dir":                     {defVal: "", isBool: false},
		"kubeconfig":                       {defVal: "", isBool: false},
		"context":                          {defVal: "", isBool: false},
		"in-cluster":                       {defVal: "false", isBool: true},
		"cluster-name":                     {defVal: "", isBool: false},
		"project":                          {defVal: "", isBool: false},
		"location":                         {defVal: "", isBool: false},
		"namespace":                        {defVal: "", isBool: false},
		"sources":                          {defVal: "topology-drift,compute-class", isBool: false},
		"topology-keys":                    {defVal: "topology.kubernetes.io/zone,topology.kubernetes.io/region", isBool: false},
		"topology-domain-unavailable-keys": {defVal: "topology.kubernetes.io/zone,topology.kubernetes.io/region", isBool: false},
		"topology-cluster-defaults":        {defVal: "", isBool: false},
		"topology-tier-c-signals":          {defVal: "false", isBool: true},
		"compute-class-tier-c-signals":     {defVal: "false", isBool: true},
		"all":                              {defVal: "false", isBool: true},
		"min-drift":                        {defVal: "0", isBool: false},
		"parallelism":                      {defVal: "8", isBool: false},
		"timeout":                          {defVal: "1m0s", isBool: false},
		"output":                           {defVal: "", isBool: false},
		"o":                                {defVal: "", isBool: false},
		"pretty":                           {defVal: "true", isBool: true},
	}

	var o reportOptions
	fs := newReportFlagSet(&o, os.Stderr)

	seen := map[string]bool{}
	fs.VisitAll(func(f *flag.Flag) {
		seen[f.Name] = true
		spec, ok := want[f.Name]
		if !ok {
			// New unmarked flags become Stable on release; require updating
			// docs/stability-policy.md and this table when adding one.
			t.Errorf("unrecorded leeway report flag --%s (default %q) — document in docs/stability-policy.md §4 and pin here", f.Name, f.DefValue)
			return
		}
		var gotBool bool
		if bf, ok := f.Value.(boolFlag); ok && bf.IsBoolFlag() {
			gotBool = true
		}
		if gotBool != spec.isBool {
			t.Errorf("flag --%s IsBoolFlag() = %v, want %v (syntax change breaks callers)", f.Name, gotBool, spec.isBool)
		}
		if f.DefValue != spec.defVal {
			t.Errorf("flag --%s default = %q, want %q (default changes require CHANGELOG ### Changed per docs/stability-policy.md §4)", f.Name, f.DefValue, spec.defVal)
		}
	})

	for name := range want {
		if !seen[name] {
			t.Errorf("stable leeway report flag --%s was removed or renamed (violates docs/stability-policy.md §4)", name)
		}
	}
}

// TestReportExitCodesContract verifies docs/stability-policy.md §4 exit codes:
//   - 0 on --help, clean/breaching reports, and partial-fleet reports where a
//     target cluster fails with status="error" inside the JSON document;
//   - 1 on target discovery or output write failure (nothing written to stdout);
//   - 2 on CLI usage / flag validation errors (nothing written to stdout).
func TestReportExitCodesContract(t *testing.T) {
	dir := t.TempDir()

	run := func(args ...string) (int, string, string) {
		t.Helper()
		outFile, err := os.CreateTemp(dir, "stdout-*")
		if err != nil {
			t.Fatal(err)
		}
		defer outFile.Close()
		errFile, err := os.CreateTemp(dir, "stderr-*")
		if err != nil {
			t.Fatal(err)
		}
		defer errFile.Close()

		code := realMain(append([]string{"report"}, args...), outFile, errFile)
		outBytes, _ := os.ReadFile(outFile.Name())
		errBytes, _ := os.ReadFile(errFile.Name())
		return code, string(outBytes), string(errBytes)
	}

	// 0: --help writes usage to stderr, nothing to stdout.
	if code, out, _ := run("--help"); code != 0 || out != "" {
		t.Errorf("report --help = (%d, %q), want (0, \"\")", code, out)
	}

	// 2: Usage errors (unknown flag, positional arg, bad --sources, bad --topology-cluster-defaults, empty --topology-keys).
	for _, badArgs := range [][]string{
		{"--no-such-flag"},
		{"unexpected-arg"},
		{"--sources=unknown-source"},
		{"--topology-cluster-defaults=malformed"},
		{"--topology-keys=,"},
	} {
		if code, out, _ := run(badArgs...); code != 2 || out != "" {
			t.Errorf("report %v = (%d, stdout=%q), want exit 2 and empty stdout", badArgs, code, out)
		}
	}

	// 1: Discovery error (non-existent kubeconfig) and output-file write error.
	t.Setenv("CREDENTIAL_PROXY_URL", "")
	if code, out, _ := run("--kubeconfig=" + filepath.Join(dir, "missing-kubeconfig")); code != 1 || out != "" {
		t.Errorf("report missing kubeconfig = (%d, stdout=%q), want exit 1 and empty stdout", code, out)
	}

	// 0: Partial/unreachable explicit endpoint target produces valid JSON with status="error" and exits 0.
	code, out, _ := run("--clusters=dead=http://127.0.0.1:1", "--timeout=200ms", "--pretty=false")
	if code != 0 {
		t.Fatalf("report with unreachable cluster target exited %d, want 0", code)
	}
	var rep report.Report
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatalf("stdout is not valid Report JSON: %v (stdout=%q)", err, out)
	}
	if rep.SchemaVersion != report.SchemaVersion || len(rep.Clusters) != 1 || rep.Clusters[0].Status != report.StatusError {
		t.Errorf("unexpected partial-error report: %+v", rep)
	}

	// 1: Unwritable --output path fails with exit 1 and empty stdout.
	badOut := filepath.Join(dir, "nonexistent-subdir", "report.json")
	if code, out, _ := run("--clusters=dead=http://127.0.0.1:1", "--timeout=200ms", "--output="+badOut); code != 1 || out != "" {
		t.Errorf("report unwritable --output = (%d, stdout=%q), want exit 1 and empty stdout", code, out)
	}
}
