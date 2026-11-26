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
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-steer/leeway/internal/mcpserver"
	"github.com/go-steer/leeway/internal/version"
	"github.com/go-steer/leeway/pkg/clusters"
	"github.com/go-steer/leeway/pkg/report"
	"github.com/go-steer/leeway/pkg/sources/computeclass"
	"github.com/go-steer/leeway/pkg/sources/topologydrift"
)

type mcpOptions struct {
	clusters     string
	clustersFrom string
	profilesDir  string
	kubeconfig   string
	kubeContext  string
	inCluster    bool
	clusterName  string
	project      string
	location     string
	googleAuth   string

	sources            string
	topologyKeys       string
	topologyDomainKeys string
	topologyDefaults   string
	parallelism        int
	timeout            time.Duration
}

func newMCPFlagSet(o *mcpOptions, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("leeway mcp", flag.ContinueOnError)
	fs.SetOutput(stderr)

	fs.StringVar(&o.clusters, "clusters", "", "Default comma-separated target clusters when omitted by tool arguments (`projects/<p>/locations/<l>/clusters/<c>`, `<p>/<l>/<c>`, `gke_<p>_<l>_<c>`, `name=https://endpoint`, or kubeconfig contexts).")
	fs.StringVar(&o.clustersFrom, "clusters-from", "", "Default comma-separated fleet discovery scopes (`kubeconfig[:/path]`, `profiles:/path/to/profiles`, or `<project>[/<location>]`).")
	fs.StringVar(&o.profilesDir, "profiles-dir", "", "Path to a Hermes profiles directory where each Cluster Agent profile has a config.yaml with a cluster_identity block.")
	fs.StringVar(&o.kubeconfig, "kubeconfig", "", "Path to a kubeconfig file (falls back to KUBECONFIG or ~/.kube/config).")
	fs.StringVar(&o.kubeContext, "context", "", "Default kubeconfig context name for single-cluster fallback.")
	fs.BoolVar(&o.inCluster, "in-cluster", false, "Use in-cluster service account credentials when no cluster target is given.")
	fs.StringVar(&o.clusterName, "cluster-name", "", "Logical cluster name for single-cluster fallback.")
	fs.StringVar(&o.project, "project", "", "Default GCP project for cluster identity and fleet discovery fallback.")
	fs.StringVar(&o.location, "location", "", "Default GCP location (region or zone) for cluster identity.")
	fs.StringVar(&o.googleAuth, "google-auth", "", "Google authentication mode (`adc` or `injected`; empty consults LEEWAY_GOOGLE_AUTH, CORE_AGENT_GOOGLE_AUTH, and MAST_GOOGLE_AUTH).")

	fs.StringVar(&o.sources, "sources", strings.Join(defaultSources, ","), "Default comma-separated sources to evaluate (`topology-drift`, `compute-class`).")
	fs.StringVar(&o.topologyKeys, "topology-keys", strings.Join(defaultTopologyKeys(), ","), "Comma-separated node labels to treat as topology axes, in precedence order.")
	fs.StringVar(&o.topologyDomainKeys, "topology-domain-unavailable-keys", strings.Join(defaultDomainUnavailableKeys(), ","), "Comma-separated topology axes on which a domain with no usable node raises leeway.domain_unavailable (pass empty string to disable).")
	fs.StringVar(&o.topologyDefaults, "topology-cluster-defaults", "", "Cluster kube-scheduler PodTopologySpread defaultConstraints as `key=maxSkew[:DoNotSchedule|ScheduleAnyway]` comma-separated, or `none`.")
	fs.IntVar(&o.parallelism, "parallelism", report.DefaultParallelism, "Maximum number of clusters to evaluate concurrently.")
	fs.DurationVar(&o.timeout, "timeout", report.DefaultClusterTimeout, "Maximum duration for each report tool call, including target discovery and cluster evaluation.")

	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: leeway mcp [flags]\n\n"+
			"Serves a read-only Model Context Protocol (MCP) server over stdio exposing\n"+
			"the `report` tool (`leeway.report/v1alpha1` JSON). Startup and `tools/list`\n"+
			"perform no cluster discovery or network calls.\n\nFlags:\n")
		fs.PrintDefaults()
	}
	return fs
}

func runMCPCmd(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var o mcpOptions
	fs := newMCPFlagSet(&o, stderr)

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "leeway mcp: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	if o.parallelism <= 0 {
		fmt.Fprintf(stderr, "leeway mcp: --parallelism must be > 0 (got %d)\n", o.parallelism)
		return 2
	}
	if o.timeout <= 0 {
		fmt.Fprintf(stderr, "leeway mcp: --timeout must be > 0 (got %s)\n", o.timeout)
		return 2
	}
	if _, err := clusters.ResolveGoogleAuthMode(o.googleAuth); err != nil {
		fmt.Fprintf(stderr, "leeway mcp: --google-auth: %v\n", err)
		return 2
	}

	parsedDefaults, err := topologydrift.ParseClusterDefaults(o.topologyDefaults)
	if err != nil {
		fmt.Fprintf(stderr, "leeway mcp: --topology-cluster-defaults: %v\n", err)
		return 2
	}
	topoKeys := topologyKeysFrom(o.topologyKeys)
	if len(topoKeys) == 0 {
		fmt.Fprintf(stderr, "leeway mcp: --topology-keys must name at least one node label\n")
		return 2
	}
	srcs := splitCSV(o.sources)
	if len(srcs) == 0 {
		fmt.Fprintf(stderr, "leeway mcp: --sources names nothing to run\n")
		return 2
	}
	for _, s := range srcs {
		if s != topologydrift.Name && s != computeclass.Name {
			fmt.Fprintf(stderr, "leeway mcp: --sources: unknown source %q\n", s)
			return 2
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := mcpserver.New(mcpserver.Config{
		Version:                   version.Version,
		Clusters:                  o.clusters,
		ClustersFrom:              o.clustersFrom,
		ProfilesDir:               o.profilesDir,
		Kubeconfig:                o.kubeconfig,
		Context:                   o.kubeContext,
		InCluster:                 o.inCluster,
		ClusterName:               o.clusterName,
		Project:                   o.project,
		Location:                  o.location,
		GoogleAuth:                o.googleAuth,
		Sources:                   srcs,
		TopologyKeys:              topoKeys,
		DomainUnavailableKeys:     topologyKeysFrom(o.topologyDomainKeys),
		ClusterDefaultConstraints: parsedDefaults,
		Parallelism:               o.parallelism,
		Timeout:                   o.timeout,
		Stderr:                    stderr,
	})

	if err := mcpserver.ServeStdio(ctx, stdin, stdout, srv); err != nil {
		fmt.Fprintf(stderr, "leeway mcp: %v\n", err)
		return 1
	}
	return 0
}
