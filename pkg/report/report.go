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

// Package report implements one-shot, multi-cluster topology skew and
// compute-class rank reporting (`leeway report`).
package report

import (
	"time"

	"github.com/go-steer/leeway/pkg/leeway"
	"github.com/go-steer/leeway/pkg/sources/computeclass"
)

// SchemaVersion identifies the wire format of a leeway report.
const SchemaVersion = "leeway.report/v1alpha1"

// Status values for ClusterReport.Status.
const (
	StatusOK    = "ok"
	StatusError = "error"
)

// Report is the top-level JSON document emitted by `leeway report`.
type Report struct {
	SchemaVersion string          `json:"schema_version"`
	GeneratedAt   time.Time       `json:"generated_at"`
	Clusters      []ClusterReport `json:"clusters"`
	Summary       Summary         `json:"summary"`
}

// Summary aggregates counts across all target clusters in a Report.
type Summary struct {
	ClustersTotal     int            `json:"clusters_total"`
	ClustersSucceeded int            `json:"clusters_succeeded"`
	ClustersFailed    int            `json:"clusters_failed"`
	TopologyFindings  int            `json:"topology_findings"`
	RankFindings      int            `json:"rank_findings"`
	BySeverity        map[string]int `json:"by_severity,omitempty"`
	ByTier            map[string]int `json:"by_tier,omitempty"`
}

// ClusterReport is the point-in-time evaluation of a single Kubernetes cluster.
type ClusterReport struct {
	Cluster          string                              `json:"cluster"`
	Source           string                              `json:"source,omitempty"`
	Status           string                              `json:"status"`
	Error            string                              `json:"error,omitempty"`
	Domains          map[string]map[string]DomainSummary `json:"domains,omitempty"`
	TopologyFindings []leeway.Finding                    `json:"topology_findings,omitempty"`
	ComputeClasses   []computeclass.ClassSummary         `json:"compute_classes,omitempty"`
	RankFindings     []leeway.RankFinding                `json:"rank_findings,omitempty"`
	Subjects         []leeway.Finding                    `json:"subjects,omitempty"`
}

// DomainSummary reports node census and allocatable capacity for one topology
// domain on one topology key.
type DomainSummary struct {
	Nodes               int64 `json:"nodes"`
	Ready               int64 `json:"ready"`
	Usable              int64 `json:"usable"`
	AllocatableCPUMilli int64 `json:"allocatable_cpu_milli,omitempty"`
	AllocatableMemBytes int64 `json:"allocatable_mem_bytes,omitempty"`
}
