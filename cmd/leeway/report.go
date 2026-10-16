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
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-steer/leeway/pkg/clusters"
	"github.com/go-steer/leeway/pkg/report"
	"github.com/go-steer/leeway/pkg/sources/topologydrift"
)

type reportOptions struct {
	clusters     string
	clustersFrom string
	profilesDir  string
	kubeconfig   string
	kubeContext  string
	inCluster    bool
	clusterName  string
	project      string
	location     string

	namespaces        string
	sources           string
	topologyKeys      string
	topologyDefaults  string
	topologyTierC     bool
	computeClassTierC bool

	all         bool
	minDrift    float64
	parallelism int
	timeout     time.Duration
	output      string
	pretty      bool
}

func runReportCmd(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("leeway report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o reportOptions

	fs.StringVar(&o.clusters, "clusters", "", "Comma-separated list of target clusters: GKE triples (`project/location/cluster`), `name=https://endpoint`, or kubeconfig context names.")
	fs.StringVar(&o.clustersFrom, "clusters-from", "", "Fleet discovery source: `kubeconfig[:/path]`, `profiles:/path/to/profiles`, or `<project>[/<location>]`.")
	fs.StringVar(&o.profilesDir, "profiles-dir", "", "Path to a Hermes profiles directory where each Cluster Agent profile has a config.yaml with a cluster_identity block.")
	fs.StringVar(&o.kubeconfig, "kubeconfig", "", "Path to a kubeconfig file (falls back to KUBECONFIG or ~/.kube/config).")
	fs.StringVar(&o.kubeContext, "context", "", "Kubeconfig context name for single-cluster report.")
	fs.BoolVar(&o.inCluster, "in-cluster", false, "Use in-cluster service account credentials.")
	fs.StringVar(&o.clusterName, "cluster-name", "", "Logical cluster name for single-cluster report.")
	fs.StringVar(&o.project, "project", "", "Default GCP project for cluster identity.")
	fs.StringVar(&o.location, "location", "", "Default GCP location (region or zone) for cluster identity.")

	fs.StringVar(&o.namespaces, "namespace", "", "Comma-separated namespaces to scope workload and pod evaluation (empty queries all namespaces).")
	fs.StringVar(&o.sources, "sources", strings.Join(defaultSources, ","), "Comma-separated sources to evaluate (`topology-drift`, `compute-class`).")
	fs.StringVar(&o.topologyKeys, "topology-keys", strings.Join(defaultTopologyKeys(), ","), "Comma-separated node labels to treat as topology axes, in precedence order.")
	fs.StringVar(&o.topologyDefaults, "topology-cluster-defaults", "", "Cluster kube-scheduler PodTopologySpread defaultConstraints as `key=maxSkew[:DoNotSchedule|ScheduleAnyway]` comma-separated, or `none`.")
	fs.BoolVar(&o.topologyTierC, "topology-tier-c-signals", false, "Include Tier C topology-drift findings (workloads without declared or assumed spread constraints) in topology_findings.")
	fs.BoolVar(&o.computeClassTierC, "compute-class-tier-c-signals", false, "Include Tier C compute-class findings (`leeway.rank_tier_unused`) in rank_findings.")

	fs.BoolVar(&o.all, "all", false, "Include all evaluated workload and node-group subjects in each cluster's `subjects` array, even when not breaching.")
	fs.Float64Var(&o.minDrift, "min-drift", 0, "Include evaluated subjects with Score.Drift >= min-drift in each cluster's `subjects` array.")
	fs.IntVar(&o.parallelism, "parallelism", report.DefaultParallelism, "Maximum number of clusters to evaluate concurrently.")
	fs.DurationVar(&o.timeout, "timeout", report.DefaultClusterTimeout, "Per-cluster evaluation timeout.")
	fs.StringVar(&o.output, "output", "", "Write JSON report to this file path instead of stdout.")
	fs.StringVar(&o.output, "o", "", "Shorthand for --output.")
	fs.BoolVar(&o.pretty, "pretty", true, "Pretty-print JSON output with indentation.")

	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: leeway report [flags]\n\n"+
			"Evaluates point-in-time topology skews, domain availability, and compute-class\n"+
			"priority rank occupancy across one or more Kubernetes clusters and writes a\n"+
			"structured JSON report to stdout (or --output).\n\nFlags:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "leeway report: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	if o.parallelism <= 0 {
		fmt.Fprintf(stderr, "leeway report: --parallelism must be > 0 (got %d)\n", o.parallelism)
		return 2
	}
	if o.timeout <= 0 {
		fmt.Fprintf(stderr, "leeway report: --timeout must be > 0 (got %s)\n", o.timeout)
		return 2
	}

	parsedDefaults, err := topologydrift.ParseClusterDefaults(o.topologyDefaults)
	if err != nil {
		fmt.Fprintf(stderr, "leeway report: --topology-cluster-defaults: %v\n", err)
		return 2
	}
	topoKeys := topologyKeysFrom(o.topologyKeys)
	if len(topoKeys) == 0 {
		fmt.Fprintf(stderr, "leeway report: --topology-keys must name at least one node label\n")
		return 2
	}
	srcs := splitCSV(o.sources)
	for _, s := range srcs {
		if s != topologydrift.Name && s != "compute-class" {
			fmt.Fprintf(stderr, "leeway report: --sources: unknown source %q\n", s)
			return 2
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	targets, err := clusters.Resolve(ctx, clusters.Options{
		Clusters:     o.clusters,
		ClustersFrom: o.clustersFrom,
		ProfilesDir:  o.profilesDir,
		Kubeconfig:   o.kubeconfig,
		Context:      o.kubeContext,
		InCluster:    o.inCluster,
		ClusterName:  o.clusterName,
		Project:      o.project,
		Location:     o.location,
		OnSkip: func(source string, err error) {
			fmt.Fprintf(stderr, "leeway report: skipping cluster %s: %v\n", source, err)
		},
	})
	if err != nil {
		fmt.Fprintf(stderr, "leeway report: %v\n", err)
		return 1
	}

	rep, err := report.Collect(ctx, targets, report.Options{
		Parallelism:               o.parallelism,
		Timeout:                   o.timeout,
		Namespaces:                splitCSV(o.namespaces),
		Sources:                   srcs,
		TopologyKeys:              topoKeys,
		ClusterDefaultConstraints: parsedDefaults,
		TopologyTierC:             o.topologyTierC,
		ComputeClassTierC:         o.computeClassTierC,
		All:                       o.all,
		MinDrift:                  o.minDrift,
	})
	if err != nil {
		fmt.Fprintf(stderr, "leeway report: %v\n", err)
		return 1
	}

	var encoded []byte
	if o.pretty {
		encoded, err = json.MarshalIndent(rep, "", "  ")
	} else {
		encoded, err = json.Marshal(rep)
	}
	if err != nil {
		fmt.Fprintf(stderr, "leeway report: marshal JSON: %v\n", err)
		return 1
	}
	encoded = append(encoded, '\n')

	if o.output != "" {
		if err := os.WriteFile(o.output, encoded, 0o644); err != nil {
			fmt.Fprintf(stderr, "leeway report: write %s: %v\n", o.output, err)
			return 1
		}
		return 0
	}

	if _, err := stdout.Write(encoded); err != nil {
		fmt.Fprintf(stderr, "leeway report: write stdout: %v\n", err)
		return 1
	}
	return 0
}
