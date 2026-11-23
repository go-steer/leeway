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
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/go-steer/leeway/pkg/leeway"
	"github.com/go-steer/leeway/pkg/report"
	"github.com/go-steer/leeway/pkg/store"
)

var (
	mermaidBlockRe  = regexp.MustCompile("(?s)```mermaid\\r?\\n(.*?)\\r?\\n```")
	mermaidFlagRe   = regexp.MustCompile(`--([a-z0-9][a-z0-9-]*)`)
	mermaidKindRe   = regexp.MustCompile(`\bleeway\.([a-z0-9_]+)\b`)
	mermaidTableRe  = regexp.MustCompile(`\b(leeway_[a-z0-9_]+|sink_sessions)\b`)
	mermaidSnakeRe  = regexp.MustCompile(`\b[a-z]+(?:_[a-z]+)+\b`)
	watchHelpFlagRe = regexp.MustCompile(`^\s*-([a-z0-9][a-z0-9-]*)\b`)
)

func knownMermaidVocabulary(t *testing.T) (flags, kinds, tables, remediations map[string]bool) {
	t.Helper()

	devNull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("open devnull: %v", err)
	}
	defer func() { _ = devNull.Close() }()

	flags = map[string]bool{}
	var ro reportOptions
	rfs := newReportFlagSet(&ro, devNull)
	rfs.VisitAll(func(f *flag.Flag) {
		flags[f.Name] = true
	})

	// Extract watch flags from runWatchCmd --help via real CLI flag registration.
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}

	code := runWatchCmd([]string{"--help"}, devNull, w)
	_ = w.Close()
	var helpBuf bytes.Buffer
	_, _ = io.Copy(&helpBuf, r)
	_ = r.Close()
	if code != 0 {
		t.Fatalf("runWatchCmd(--help) = %d, want 0", code)
	}
	for _, line := range strings.Split(helpBuf.String(), "\n") {
		if m := watchHelpFlagRe.FindStringSubmatch(line); m != nil {
			flags[m[1]] = true
		}
	}

	kinds = map[string]bool{
		leeway.KindContractViolated:  true,
		leeway.KindPlacementDrift:    true,
		leeway.KindBaselineBreach:    true,
		leeway.KindDomainUnavailable: true,
		leeway.KindRankWedged:        true,
		leeway.KindRankDegraded:      true,
		leeway.KindRankNoMigration:   true,
		leeway.KindRankTierUnused:    true,
		report.SchemaVersion:         true, // leeway.report/v1alpha1 matches `leeway.report`
		"leeway.report":              true,
	}

	// Verify SQLite tables against an actual opened store rather than a static list.
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	if _, err := st.LeewayAlertStates(context.Background(), "c1"); err != nil {
		t.Fatalf("LeewayAlertStates: %v", err)
	}
	if _, err := st.LeewayBaselines(context.Background(), "c1"); err != nil {
		t.Fatalf("LeewayBaselines: %v", err)
	}
	// sink_sessions exists in the schema but cmd/leeway never reads or writes
	// it, so a diagram that draws it is describing behaviour that is not there.
	tables = map[string]bool{
		"leeway_alert_state": true,
		"leeway_baseline":    true,
	}

	remediations = map[string]bool{
		string(leeway.RemediationManifestPatch):   true,
		string(leeway.RemediationPlatformManaged): true,
		string(leeway.RemediationRolloutRestart):  true,
		string(leeway.RemediationClusterCapacity): true,
		string(leeway.RemediationClusterNodes):    true,
		string(leeway.RemediationStorageTopology): true,
	}
	return flags, kinds, tables, remediations
}

// validateMermaidBlock checks that a Mermaid code block starts with a supported
// diagram header, has balanced subgraphs, and only names real CLI flags,
// leeway.* finding kinds (excluding Go symbol selectors in UpperCamelCase),
// SQLite tables, and remediation kinds.
func validateMermaidBlock(src string, flags, kinds, tables, remediations map[string]bool) error {
	trimmed := strings.TrimSpace(src)
	if trimmed == "" {
		return fmt.Errorf("empty mermaid block")
	}
	firstLine := strings.TrimSpace(strings.SplitN(trimmed, "\n", 2)[0])
	if !strings.HasPrefix(firstLine, "flowchart ") && !strings.HasPrefix(firstLine, "graph ") && !strings.HasPrefix(firstLine, "sequenceDiagram") {
		return fmt.Errorf("unsupported mermaid diagram header %q", firstLine)
	}

	var subgraphs, ends int
	for _, line := range strings.Split(trimmed, "\n") {
		l := strings.TrimSpace(line)
		if strings.HasPrefix(l, "subgraph ") {
			subgraphs++
		} else if l == "end" {
			ends++
		}
	}
	if subgraphs != ends {
		return fmt.Errorf("unbalanced subgraph/end count (%d subgraph vs %d end)", subgraphs, ends)
	}

	// Strip `kubectl get --raw` before checking leeway's own CLI flags.
	withoutKubectlRaw := strings.ReplaceAll(trimmed, "kubectl get --raw", "kubectl get")
	for _, m := range mermaidFlagRe.FindAllStringSubmatch(withoutKubectlRaw, -1) {
		name := m[1]
		if !flags[name] {
			return fmt.Errorf("unknown CLI flag --%s in diagram", name)
		}
	}

	for _, m := range mermaidKindRe.FindAllStringSubmatch(trimmed, -1) {
		full := "leeway." + m[1]
		if !kinds[full] {
			return fmt.Errorf("unknown leeway.* wire kind %q in diagram", full)
		}
	}

	for _, m := range mermaidTableRe.FindAllStringSubmatch(trimmed, -1) {
		tbl := m[1]
		// Exclude leeway_watch.py (downstream kube-agents script filename).
		if tbl == "leeway_watch" {
			continue
		}
		if !tables[tbl] {
			return fmt.Errorf("unknown SQLite table %q in diagram", tbl)
		}
	}

	// A node that lists remediation kinds names nothing else in snake_case,
	// so every such token on its line must be a real kind.
	for _, line := range strings.Split(trimmed, "\n") {
		if !strings.Contains(line, "Remediation") {
			continue
		}
		for _, tok := range mermaidSnakeRe.FindAllString(line, -1) {
			if !remediations[tok] {
				return fmt.Errorf("unknown remediation kind %q in diagram", tok)
			}
		}
	}

	return nil
}

func TestDocsSiteMermaidDiagramsAndDesignSplit(t *testing.T) {
	t.Parallel()

	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	siteDir := filepath.Join(repoRoot, "docs", "site")
	docsContentDir := filepath.Join(siteDir, "src", "content", "docs")

	astroCfg, err := os.ReadFile(filepath.Join(siteDir, "astro.config.mjs"))
	if err != nil {
		t.Fatalf("read astro.config.mjs: %v", err)
	}
	if !bytes.Contains(astroCfg, []byte("mermaid-render.js")) {
		t.Errorf("docs/site/astro.config.mjs does not load mermaid-render.js")
	}
	if _, err := os.Stat(filepath.Join(siteDir, "public", "mermaid-render.js")); err != nil {
		t.Errorf("docs/site/public/mermaid-render.js missing: %v", err)
	}

	wantPages := []string{
		"concepts/architecture.md",
		"getting-started/report.md",
		"getting-started/deploy.md",
		"concepts/scoring-and-attribution.md",
		"concepts/compute-class-ranks.md",
	}

	flags, kinds, tables, remediations := knownMermaidVocabulary(t)

	foundByPage := map[string]int{}
	err = filepath.WalkDir(docsContentDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		if ext := filepath.Ext(path); ext != ".md" && ext != ".mdx" {
			return nil
		}
		raw, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		rel, _ := filepath.Rel(docsContentDir, path)
		rel = filepath.ToSlash(rel)
		matches := mermaidBlockRe.FindAllStringSubmatch(string(raw), -1)
		foundByPage[rel] = len(matches)
		for i, m := range matches {
			if verr := validateMermaidBlock(m[1], flags, kinds, tables, remediations); verr != nil {
				t.Errorf("%s mermaid block #%d: %v", rel, i+1, verr)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", docsContentDir, err)
	}

	for _, page := range wantPages {
		if foundByPage[page] < 1 {
			t.Errorf("expected at least 1 ```mermaid diagram in docs/site/src/content/docs/%s, found %d", page, foundByPage[page])
		}
	}

	// Verify design doc split invariants (#32).
	designBytes, err := os.ReadFile(filepath.Join(repoRoot, "docs", "leeway-design.md"))
	if err != nil {
		t.Fatalf("read docs/leeway-design.md: %v", err)
	}
	design := string(designBytes)
	for _, retired := range []string{
		"## 2. Where this fits in `k8s-lookout`",
		"## 13. Prerequisite spikes",
		"## 14. Delivery plan",
		"## 15. Open questions",
	} {
		if strings.Contains(design, retired) {
			t.Errorf("docs/leeway-design.md still contains archived/retired heading %q", retired)
		}
	}
	for _, required := range []string{
		"## 2. Relationship to `k8s-lookout`",
		"go-steer/k8s-lookout#842",
		"stability-policy.md",
		"archive/leeway-implementation-journal.md",
	} {
		if !strings.Contains(design, required) {
			t.Errorf("docs/leeway-design.md missing required living-spec content %q", required)
		}
	}

	archiveBytes, err := os.ReadFile(filepath.Join(repoRoot, "docs", "archive", "leeway-implementation-journal.md"))
	if err != nil {
		t.Fatalf("read docs/archive/leeway-implementation-journal.md: %v", err)
	}
	archive := string(archiveBytes)
	for _, required := range []string{
		"../leeway-design.md",
		"## 13. Prerequisite spikes",
		"## 14. Delivery plan",
		"## 15. Open questions",
	} {
		if !strings.Contains(archive, required) {
			t.Errorf("docs/archive/leeway-implementation-journal.md missing %q", required)
		}
	}
}

func TestValidateMermaidBlock_RefusesStaleIdentifiers(t *testing.T) {
	t.Parallel()

	flags, kinds, tables, remediations := knownMermaidVocabulary(t)

	cases := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "unknown CLI flag",
			src:  "flowchart TB\n  A[\"--nonexistent-flag=true\"] --> B[\"ok\"]",
			want: "unknown CLI flag --nonexistent-flag",
		},
		{
			name: "unknown leeway finding kind",
			src:  "flowchart TB\n  A[\"leeway.retired_fake_kind\"] --> B[\"ok\"]",
			want: "unknown leeway.* wire kind \"leeway.retired_fake_kind\"",
		},
		{
			name: "unknown SQLite table",
			src:  "flowchart TB\n  A[\"leeway_nonexistent_table\"] --> B[\"ok\"]",
			want: "unknown SQLite table \"leeway_nonexistent_table\"",
		},
		{
			name: "unwired SQLite table",
			src:  "flowchart TB\n  A[\"sink_sessions\"] --> B[\"ok\"]",
			want: "unknown SQLite table \"sink_sessions\"",
		},
		{
			name: "unknown remediation kind",
			src:  "flowchart TB\n  A[\"Deterministic Remediation: manifest_patch | node_reboot\"] --> B[\"ok\"]",
			want: "unknown remediation kind \"node_reboot\"",
		},
		{
			name: "unbalanced subgraph",
			src:  "flowchart TB\n  subgraph S[\"One\"]\n    A[\"node\"]",
			want: "unbalanced subgraph/end count",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMermaidBlock(tc.src, flags, kinds, tables, remediations)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validateMermaidBlock() error = %v, want substring %q", err, tc.want)
			}
		})
	}
}
