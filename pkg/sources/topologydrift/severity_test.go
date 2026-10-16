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

package topologydrift

import (
	"slices"
	"testing"

	"github.com/go-steer/leeway/pkg/engine"
	"github.com/go-steer/leeway/pkg/leeway"
)

// TestTierSeverities_MatchEngine holds pkg/leeway's duplicated severity strings
// equal to pkg/engine's.
//
// pkg/leeway remains dependency-free, so Tier.Severity returns string literals
// that match pkg/engine's Severity constants. This package imports both and is
// where the duplication is made safe.
func TestTierSeverities_MatchEngine(t *testing.T) {
	want := map[leeway.Tier]string{
		leeway.TierA:    string(engine.SeverityCritical),
		leeway.TierB:    string(engine.SeverityWarning),
		leeway.TierC:    string(engine.SeverityInfo),
		leeway.TierNone: "",
	}
	for tier, sev := range want {
		if got := tier.Severity(); got != sev {
			t.Errorf("Tier %v severity = %q, want engine's %q", tier, got, sev)
		}
	}

	known := []string{
		string(engine.SeverityCritical),
		string(engine.SeverityWarning),
		string(engine.SeverityInfo),
	}
	for tier := range want {
		sev := tier.Severity()
		if sev == "" {
			continue
		}
		if !slices.Contains(known, sev) {
			t.Errorf("Tier %v routes to %q, which is not one of engine severities %v", tier, sev, known)
		}
	}
}
