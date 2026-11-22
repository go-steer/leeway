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
	"strings"

	"github.com/go-steer/leeway/pkg/leeway"
)

const gkeSourcePrefix = "gke:"

// CanonicalCluster returns the stable fleet-wide cluster identifier for cr.
//
// When cr.Source carries a GKE identity ("gke:<project>/<location>/<cluster>"
// with three non-empty slash-separated segments), CanonicalCluster returns the
// "<project>/<location>/<cluster>" triple so two clusters sharing a short name
// in different GCP projects or regions never collide in a fleet ledger.
// Otherwise it returns cr.Cluster trimmed of surrounding whitespace.
func CanonicalCluster(cr ClusterReport) string {
	if raw, ok := strings.CutPrefix(strings.TrimSpace(cr.Source), gkeSourcePrefix); ok {
		triple := strings.TrimSpace(raw)
		parts := strings.Split(triple, "/")
		if len(parts) == 3 && parts[0] != "" && parts[1] != "" && parts[2] != "" {
			return triple
		}
	}
	return strings.TrimSpace(cr.Cluster)
}

// CanonicalCluster returns CanonicalCluster(cr).
func (cr ClusterReport) CanonicalCluster() string {
	return CanonicalCluster(cr)
}

// TopologyFindingKey returns the deterministic cross-sweep deduplication key
// for f on cr's CanonicalCluster (see leeway.TopologyFindingKey).
func (cr ClusterReport) TopologyFindingKey(f leeway.Finding) string {
	return leeway.TopologyFindingKey(cr.CanonicalCluster(), f)
}

// RankFindingKey returns the deterministic cross-sweep deduplication key for f
// on cr's CanonicalCluster (see leeway.RankFindingKey).
func (cr ClusterReport) RankFindingKey(f leeway.RankFinding) string {
	return leeway.RankFindingKey(cr.CanonicalCluster(), f)
}
