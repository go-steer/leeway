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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/go-steer/leeway/pkg/clusters"
	"github.com/go-steer/leeway/pkg/leeway"
	"github.com/go-steer/leeway/pkg/report"
	"github.com/go-steer/leeway/pkg/sources/computeclass"
	"github.com/go-steer/leeway/pkg/sources/topologydrift"
)

const (
	// ServerName is advertised in MCP initialize responses so hosts that prefix
	// `<server>_<tool>` register the `report` tool as `leeway_report`.
	ServerName = "leeway"
	// ReportToolName is the single read-only tool exposed by `leeway mcp`.
	ReportToolName = "report"
	// EnvClustersFrom optionally sets a default --clusters-from fleet discovery
	// scope for `leeway mcp` when neither CLI flags nor tool arguments specify one.
	EnvClustersFrom = "LEEWAY_CLUSTERS_FROM"
)

// Config configures a Leeway MCP server instance. Constructing a Server is
// purely in-memory: neither New nor ListTools performs cluster discovery,
// kubeconfig loading, or network I/O.
type Config struct {
	Version string

	Clusters     string
	ClustersFrom string
	ProfilesDir  string
	Kubeconfig   string
	Context      string
	InCluster    bool
	ClusterName  string
	Project      string
	Location     string
	GoogleAuth   string

	Sources                   []string
	TopologyKeys              []leeway.TopologyKey
	DomainUnavailableKeys     []leeway.TopologyKey
	ClusterDefaultConstraints *[]corev1.TopologySpreadConstraint
	Parallelism               int
	Timeout                   time.Duration

	Stderr io.Writer

	// Optional test injection hooks.
	Getenv          func(string) string
	ResolveClusters func(ctx context.Context, opts clusters.Options) ([]clusters.Cluster, error)
	CollectReport   func(ctx context.Context, targets []clusters.Cluster, opts report.Options) (*report.Report, error)
}

// Server implements Handler for `leeway mcp`.
type Server struct {
	cfg Config
}

// New constructs a Server from cfg without touching the network or filesystem.
func New(cfg Config) *Server {
	if cfg.Getenv == nil {
		cfg.Getenv = os.Getenv
	}
	if cfg.ResolveClusters == nil {
		cfg.ResolveClusters = clusters.Resolve
	}
	if cfg.CollectReport == nil {
		cfg.CollectReport = report.Collect
	}
	if cfg.Stderr == nil {
		cfg.Stderr = io.Discard
	}
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = report.DefaultParallelism
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = report.DefaultClusterTimeout
	}
	return &Server{cfg: cfg}
}

// ServerName implements Handler.
func (s *Server) ServerName() string { return ServerName }

// ServerVersion implements Handler.
func (s *Server) ServerVersion() string { return s.cfg.Version }

// Close implements Handler.
func (s *Server) Close() error { return nil }

// ListTools implements Handler. It operates completely offline.
func (s *Server) ListTools(context.Context) (json.RawMessage, error) {
	return json.RawMessage(toolsListJSON), nil
}

const toolsListJSON = `{
  "tools": [
    {
      "name": "report",
      "description": "Evaluates live runtime zonal/regional pod placement skew (Hamilton apportionment and root-cause attribution) and GKE ComputeClass priority-rank fallback occupancy across one or more Kubernetes clusters, returning a leeway.report/v1alpha1 JSON document.",
      "annotations": {
        "readOnlyHint": true,
        "destructiveHint": false,
        "idempotentHint": true
      },
      "inputSchema": {
        "type": "object",
        "additionalProperties": false,
        "properties": {
          "clusters": {
            "type": "string",
            "description": "Comma-separated target clusters: GKE resource paths (projects/<project>/locations/<location>/clusters/<cluster>), <project>/<location>/<cluster> triples, gke_<project>_<location>_<cluster> context names, name=https://endpoint pairs, or kubeconfig contexts (acts as a filter when clusters_from is set)."
          },
          "clusters_from": {
            "type": "string",
            "description": "Comma-separated fleet discovery scopes: <project>, <project>/<location>, projects/<project>/locations/-, kubeconfig[:<path>], or profiles:<dir>."
          },
          "namespace": {
            "type": "string",
            "description": "Comma-separated namespaces to scope workload and pod evaluation (empty evaluates all namespaces)."
          },
          "sources": {
            "type": "string",
            "description": "Comma-separated leeway sources to evaluate: topology-drift, compute-class (default evaluates both)."
          },
          "topology_keys": {
            "type": "string",
            "description": "Comma-separated node labels to treat as topology axes, in precedence order."
          },
          "topology_domain_unavailable_keys": {
            "type": "string",
            "description": "Comma-separated topology axes on which a domain with no usable node raises leeway.domain_unavailable (pass empty string \"\" to disable)."
          },
          "topology_cluster_defaults": {
            "type": "string",
            "description": "Cluster kube-scheduler PodTopologySpread defaultConstraints as key=maxSkew[:DoNotSchedule|ScheduleAnyway] comma-separated, or none."
          },
          "topology_tier_c_signals": {
            "type": "boolean",
            "description": "Include Tier C topology-drift findings (workloads without declared or assumed spread constraints) in topology_findings."
          },
          "compute_class_tier_c_signals": {
            "type": "boolean",
            "description": "Include Tier C compute-class findings (leeway.rank_tier_unused) in rank_findings."
          },
          "all": {
            "type": "boolean",
            "description": "Include all evaluated workload and node-group subjects in clusters[].subjects, even when not breaching."
          },
          "min_drift": {
            "type": "number",
            "description": "Include evaluated subjects with score.drift >= min_drift in clusters[].subjects."
          }
        }
      }
    }
  ]
}`

// ReportArgs is the JSON argument payload accepted by the `report` MCP tool.
type ReportArgs struct {
	Clusters                      string   `json:"clusters"`
	ClustersFrom                  string   `json:"clusters_from"`
	Namespace                     string   `json:"namespace"`
	Sources                       *string  `json:"sources"`
	TopologyKeys                  *string  `json:"topology_keys"`
	TopologyDomainUnavailableKeys *string  `json:"topology_domain_unavailable_keys"`
	TopologyClusterDefaults       *string  `json:"topology_cluster_defaults"`
	TopologyTierCSignals          bool     `json:"topology_tier_c_signals"`
	ComputeClassTierCSignals      bool     `json:"compute_class_tier_c_signals"`
	All                           bool     `json:"all"`
	MinDrift                      *float64 `json:"min_drift"`
}

// CallTool implements Handler.
func (s *Server) CallTool(ctx context.Context, name string, rawArgs json.RawMessage) (json.RawMessage, error) {
	if name != ReportToolName {
		return TextResult(fmt.Sprintf("unknown tool %q (want %q)", name, ReportToolName), true), nil
	}
	in, err := decodeReportArgs(rawArgs)
	if err != nil {
		return TextResult("invalid arguments: "+err.Error(), true), nil
	}

	var minDrift float64
	if in.MinDrift != nil {
		minDrift = *in.MinDrift
		if math.IsNaN(minDrift) || math.IsInf(minDrift, 0) || minDrift < 0 {
			return TextResult("invalid arguments: min_drift must be >= 0", true), nil
		}
	}

	sources := s.cfg.Sources
	if in.Sources != nil && strings.TrimSpace(*in.Sources) != "" {
		parsed, err := parseSourcesCSV(*in.Sources)
		if err != nil {
			return TextResult("invalid arguments: "+err.Error(), true), nil
		}
		sources = parsed
	} else if in.Sources != nil && strings.TrimSpace(*in.Sources) == "" {
		// Explicit empty string keeps configured default sources, matching
		// zero-value string arguments from LLM tool calls.
		sources = s.cfg.Sources
	}

	topoKeys := s.cfg.TopologyKeys
	if in.TopologyKeys != nil && strings.TrimSpace(*in.TopologyKeys) != "" {
		keys := splitTopoKeys(*in.TopologyKeys)
		if len(keys) == 0 {
			return TextResult("invalid arguments: topology_keys must name at least one node label", true), nil
		}
		topoKeys = keys
	}

	domainUnavailableKeys := s.cfg.DomainUnavailableKeys
	if in.TopologyDomainUnavailableKeys != nil {
		domainUnavailableKeys = splitTopoKeys(*in.TopologyDomainUnavailableKeys)
	}

	clusterDefaults := s.cfg.ClusterDefaultConstraints
	if in.TopologyClusterDefaults != nil && strings.TrimSpace(*in.TopologyClusterDefaults) != "" {
		parsed, err := topologydrift.ParseClusterDefaults(*in.TopologyClusterDefaults)
		if err != nil {
			return TextResult("invalid arguments: topology_cluster_defaults: "+err.Error(), true), nil
		}
		clusterDefaults = parsed
	}

	targets, err := s.resolveTargets(ctx, in)
	if err != nil {
		return TextResult(fmt.Sprintf("leeway report: %v", err), true), nil
	}

	rep, err := s.cfg.CollectReport(ctx, targets, report.Options{
		Parallelism:               s.cfg.Parallelism,
		Timeout:                   s.cfg.Timeout,
		Namespaces:                splitCSV(in.Namespace),
		Sources:                   sources,
		TopologyKeys:              topoKeys,
		DomainUnavailableKeys:     domainUnavailableKeys,
		ClusterDefaultConstraints: clusterDefaults,
		TopologyTierC:             in.TopologyTierCSignals,
		ComputeClassTierC:         in.ComputeClassTierCSignals,
		All:                       in.All,
		MinDrift:                  minDrift,
	})
	if err != nil {
		return TextResult(fmt.Sprintf("leeway report: %v", err), true), nil
	}

	encoded, err := json.Marshal(rep)
	if err != nil {
		return TextResult(fmt.Sprintf("leeway report: marshal JSON: %v", err), true), nil
	}
	return TextResult(string(encoded), false), nil
}

func (s *Server) resolveTargets(ctx context.Context, in ReportArgs) ([]clusters.Cluster, error) {
	baseOpts, err := s.baseClusterOptions()
	if err != nil {
		return nil, err
	}

	argFrom := strings.TrimSpace(in.ClustersFrom)
	argClusters := s.expandConfiguredClusterNames(in.Clusters)
	hasKC := strings.TrimSpace(baseOpts.Kubeconfig) != "" || strings.TrimSpace(s.cfg.Getenv("KUBECONFIG")) != ""
	defaultCloudScope := firstNonEmpty(
		s.cfg.ClustersFrom,
		s.cfg.Getenv(EnvClustersFrom),
		s.cfg.Project,
		s.cfg.Getenv("GOOGLE_CLOUD_PROJECT"),
	)

	switch {
	case argFrom != "":
		opts := baseOpts
		opts.ClustersFrom = argFrom
		opts.Clusters = argClusters
		return s.cfg.ResolveClusters(ctx, opts)

	case argClusters != "":
		if strings.TrimSpace(s.cfg.ProfilesDir) != "" {
			opts := baseOpts
			opts.ProfilesDir = s.cfg.ProfilesDir
			opts.Clusters = argClusters
			return s.cfg.ResolveClusters(ctx, opts)
		}
		if hasKC || defaultCloudScope == "" {
			opts := baseOpts
			opts.Clusters = argClusters
			return s.cfg.ResolveClusters(ctx, opts)
		}

		// Partition bare cluster names (resolved against defaultCloudScope) from
		// self-contained GKE refs and name=endpoint targets so mixing e.g.
		// "demo, projects/other-proj/locations/us-east1/clusters/prod" never
		// drops cross-project or explicit endpoint targets.
		bare, explicit := partitionClusterSpecs(argClusters)
		switch {
		case len(bare) == 0:
			opts := baseOpts
			opts.Clusters = strings.Join(explicit, ",")
			return s.cfg.ResolveClusters(ctx, opts)
		case len(explicit) == 0:
			opts := baseOpts
			opts.ClustersFrom = defaultCloudScope
			opts.Clusters = strings.Join(bare, ",")
			return s.cfg.ResolveClusters(ctx, opts)
		default:
			scopedOpts := baseOpts
			scopedOpts.ClustersFrom = defaultCloudScope
			scopedOpts.Clusters = strings.Join(bare, ",")
			scopedTargets, scopedErr := s.cfg.ResolveClusters(ctx, scopedOpts)

			explicitOpts := baseOpts
			explicitOpts.Clusters = strings.Join(explicit, ",")
			explicitTargets, explicitErr := s.cfg.ResolveClusters(ctx, explicitOpts)

			if scopedErr != nil && explicitErr != nil {
				return nil, errors.Join(scopedErr, explicitErr)
			}
			var merged []clusters.Cluster
			seen := make(map[string]bool)
			appendUnique := func(list []clusters.Cluster) {
				for _, c := range list {
					key := c.Profile
					if key == "" {
						key = c.Identity.String()
					}
					if !seen[key] {
						seen[key] = true
						merged = append(merged, c)
					}
				}
			}
			if scopedErr != nil {
				merged = append(merged, clusters.Cluster{
					Identity:     clusters.Identity{Cluster: strings.Join(bare, ",")},
					Profile:      "gke:" + defaultCloudScope,
					ResolveError: scopedErr,
				})
			} else {
				appendUnique(scopedTargets)
			}
			if explicitErr != nil {
				merged = append(merged, clusters.Cluster{
					Identity:     clusters.Identity{Cluster: strings.Join(explicit, ",")},
					ResolveError: explicitErr,
				})
			} else {
				appendUnique(explicitTargets)
			}
			return merged, nil
		}

	default:
		opts := baseOpts
		opts.ProfilesDir = s.cfg.ProfilesDir
		opts.ClustersFrom = s.cfg.ClustersFrom
		opts.Clusters = s.cfg.Clusters
		if opts.ProfilesDir == "" && opts.ClustersFrom == "" && opts.Clusters == "" && !opts.InCluster && opts.Context == "" {
			switch {
			case hasKC:
				opts.ClustersFrom = "kubeconfig"
			case defaultCloudScope != "":
				opts.ClustersFrom = defaultCloudScope
			}
		}
		return s.cfg.ResolveClusters(ctx, opts)
	}
}

func (s *Server) baseClusterOptions() (clusters.Options, error) {
	broker, err := clusters.BrokerFromEnvWith(s.cfg.Getenv)
	if err != nil {
		return clusters.Options{}, err
	}
	if broker != nil && broker.CredentialProxy {
		// `leeway mcp` always uses native client-go over CREDENTIAL_PROXY_URL rather
		// than shelling out to `kubectl`.
		cp := *broker
		cp.ShellMode = false
		broker = &cp
	}
	return clusters.Options{
		Kubeconfig:       s.cfg.Kubeconfig,
		Context:          s.cfg.Context,
		InCluster:        s.cfg.InCluster,
		ClusterName:      s.cfg.ClusterName,
		Project:          s.cfg.Project,
		Location:         s.cfg.Location,
		GoogleAuth:       s.cfg.GoogleAuth,
		Getenv:           s.cfg.Getenv,
		Broker:           broker,
		KeepFailedScopes: true,
		OnSkip: func(source string, skipErr error) {
			fmt.Fprintf(s.cfg.Stderr, "leeway mcp: skipping cluster %s: %v\n", source, skipErr)
		},
	}, nil
}

// expandConfiguredClusterNames maps bare cluster names in raw onto canonical
// <project>/<location>/<cluster> triples when s.cfg.Clusters configured a
// unique GKE reference for that cluster name.
func (s *Server) expandConfiguredClusterNames(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || strings.TrimSpace(s.cfg.Clusters) == "" {
		return trimmed
	}
	byName := make(map[string]string)
	ambiguous := make(map[string]bool)
	for _, cfgSpec := range splitCSV(s.cfg.Clusters) {
		id, ok := clusters.ParseClusterRef(cfgSpec)
		if !ok {
			continue
		}
		if prev, exists := byName[id.Cluster]; exists && prev != id.String() {
			ambiguous[id.Cluster] = true
			delete(byName, id.Cluster)
			continue
		}
		if !ambiguous[id.Cluster] {
			byName[id.Cluster] = id.String()
		}
	}
	if len(byName) == 0 {
		return trimmed
	}
	parts := splitCSV(trimmed)
	for i, p := range parts {
		if !strings.Contains(p, "=") {
			if _, ok := clusters.ParseClusterRef(p); !ok {
				if full, found := byName[p]; found {
					parts[i] = full
				}
			}
		}
	}
	return strings.Join(parts, ",")
}

func partitionClusterSpecs(csv string) (bare, explicit []string) {
	for _, spec := range splitCSV(csv) {
		if strings.Contains(spec, "=") {
			explicit = append(explicit, spec)
			continue
		}
		if _, ok := clusters.ParseClusterRef(spec); ok {
			explicit = append(explicit, spec)
			continue
		}
		bare = append(bare, spec)
	}
	return bare, explicit
}

func decodeReportArgs(raw json.RawMessage) (ReportArgs, error) {
	var in ReportArgs
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return in, nil
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		return ReportArgs{}, err
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return ReportArgs{}, errors.New("unexpected trailing JSON")
	}
	return in, nil
}

func parseSourcesCSV(raw string) ([]string, error) {
	parts := splitCSV(raw)
	if len(parts) == 0 {
		return nil, errors.New("sources must name topology-drift and/or compute-class")
	}
	seen := make(map[string]bool, len(parts))
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != topologydrift.Name && p != computeclass.Name {
			return nil, fmt.Errorf("sources: unknown source %q (want %s and/or %s)", p, topologydrift.Name, computeclass.Name)
		}
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out, nil
}

func splitTopoKeys(csv string) []leeway.TopologyKey {
	parts := splitCSV(csv)
	out := make([]leeway.TopologyKey, 0, len(parts))
	for _, p := range parts {
		out = append(out, leeway.TopologyKey(p))
	}
	return out
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
