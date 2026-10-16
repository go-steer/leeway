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

// Command leeway runs the placement-drift and compute-class priority rank
// subsystem in two primary execution modes:
//
//   - `leeway report [flags]`: One-shot multi-cluster CLI evaluation that
//     outputs a structured JSON report of topology skews, domain availability,
//     and compute-class priority rank occupancy across target clusters.
//   - `leeway watch [flags]` (or `leeway [flags]`): Continuous daemon across
//     one or more clusters, exporting Prometheus/OTel metrics and optionally
//     reporting incidents and resolutions over an HTTP webhook callback.
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
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.opentelemetry.io/otel/metric"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/go-steer/leeway/internal/telemetry"
	"github.com/go-steer/leeway/internal/version"
	"github.com/go-steer/leeway/pkg/clusters"
	"github.com/go-steer/leeway/pkg/inject"
	"github.com/go-steer/leeway/pkg/kube"
	"github.com/go-steer/leeway/pkg/leeway"
	"github.com/go-steer/leeway/pkg/sources"
	"github.com/go-steer/leeway/pkg/sources/computeclass"
	"github.com/go-steer/leeway/pkg/sources/topologydrift"
	"github.com/go-steer/leeway/pkg/store"
)

// DefaultMetricsAddr is the default Prometheus /metrics + /healthz + /readyz listener address.
const DefaultMetricsAddr = ":9090"

type options struct {
	clusters     string
	clustersFrom string
	profilesDir  string
	kubeconfig   string
	kubeContext  string
	inCluster    bool
	kubeQPS      float64
	kubeBurst    int
	cluster      string
	project      string
	location     string

	metricsAddr  string
	otelExporter string

	sources       string
	sourcesNamed  bool
	accessRecheck time.Duration

	topologyKeys         string
	topologyDomainKeys   string
	topologyDefaults     string
	topologyPerDomain    bool
	topologyDwell        time.Duration
	topologyTierC        bool
	computeClassWindow   time.Duration
	computeClassDwell    time.Duration
	computeClassRank0    float64
	computeClassLastRank float64
	computeClassTierC    bool

	webhookURL        string
	sinkKind          string
	sinkURL           string
	sinkTokenEnv      string
	dryRun            bool
	dedupWindow       time.Duration
	recoveryStableFor time.Duration
	storePath         string

	exitAfter   time.Duration
	showVersion bool

	// openedStore holds the optional SQLite store when --store is set.
	openedStore *store.Store
}

func main() { os.Exit(realMain(os.Args[1:], os.Stdout, os.Stderr)) }

func realMain(args []string, stdout, stderr *os.File) int {
	if len(args) > 0 {
		switch args[0] {
		case "report":
			return runReportCmd(args[1:], stdout, stderr)
		case "watch":
			return runWatchCmd(args[1:], stdout, stderr)
		case "version":
			fmt.Fprintln(stdout, version.String("leeway"))
			return 0
		}
	}
	return runWatchCmd(args, stdout, stderr)
}

func runWatchCmd(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("leeway", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o options

	fs.StringVar(&o.clusters, "clusters", "", "Comma-separated list of target clusters (`project/location/cluster`, `name=https://endpoint`, or kubeconfig contexts).")
	fs.StringVar(&o.clustersFrom, "clusters-from", "", "Fleet discovery source (`kubeconfig[:/path]`, `profiles:/path/to/profiles`, or `<project>[/<location>]`).")
	fs.StringVar(&o.profilesDir, "profiles-dir", "", "Path to a Hermes profiles directory where each Cluster Agent profile has a config.yaml with a cluster_identity block.")
	fs.StringVar(&o.kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Empty uses KUBECONFIG or ~/.kube/config, or in-cluster service account under --in-cluster.")
	fs.StringVar(&o.kubeContext, "context", "", "Context in the kubeconfig to use instead of its current-context.")
	fs.BoolVar(&o.inCluster, "in-cluster", false, "Force in-cluster service account credentials.")
	fs.Float64Var(&o.kubeQPS, "kube-qps", float64(kube.DefaultQPS), "Client-side token bucket rate limit (queries per second) for Kubernetes API calls. Must be positive.")
	fs.IntVar(&o.kubeBurst, "kube-burst", kube.DefaultBurst, "Client-side token bucket burst limit for Kubernetes API calls. Must be positive.")
	fs.StringVar(&o.cluster, "cluster-name", "", "Cluster identity stamped as the `cluster` label on exported series and incident payloads.")
	fs.StringVar(&o.project, "project", "", "GCP project stamped on incident payloads when not inferred from cluster discovery.")
	fs.StringVar(&o.location, "location", "", "GCP location (region or zone) stamped on incident payloads when not inferred from cluster discovery.")

	fs.StringVar(&o.metricsAddr, "metrics-addr", DefaultMetricsAddr, "Prometheus /metrics + /healthz + /readyz listener address (host:port). Empty disables the listener.")
	fs.StringVar(&o.otelExporter, "otel-exporter", "none", "Push metrics over OTLP as well as serving them: `otlp` or `none`.")

	fs.StringVar(&o.sources, "sources", strings.Join(defaultSources, ","), "Comma-separated sources to run (`topology-drift`, `compute-class`).")
	fs.DurationVar(&o.accessRecheck, "access-recheck", sources.DefaultAccessRecheck, "How often to re-run the SelfSubjectAccessReview probe over enabled sources; 0 disables runtime re-checking.")

	fs.StringVar(&o.topologyKeys, "topology-keys", strings.Join(defaultTopologyKeys(), ","), "Comma-separated node labels to treat as topology axes, in precedence order.")
	fs.StringVar(&o.topologyDomainKeys, "topology-domain-unavailable-keys", strings.Join(defaultDomainUnavailableKeys(), ","), "Comma-separated topology axes on which a domain with no usable node raises leeway.domain_unavailable (pass empty string to disable).")
	fs.StringVar(&o.topologyDefaults, "topology-cluster-defaults", "", "Cluster kube-scheduler PodTopologySpread defaultConstraints as `key=maxSkew[:DoNotSchedule|ScheduleAnyway]` comma-separated, or `none`.")
	fs.BoolVar(&o.topologyPerDomain, "topology-per-domain-series", false, "Export the per-domain breakdown for EVERY tracked subject rather than only the drifting ones.")
	fs.DurationVar(&o.topologyDwell, "topology-dwell", leeway.DefaultDwell().For, "How long a placement breach must persist before the topology-drift source raises a finding.")
	fs.BoolVar(&o.topologyTierC, "topology-tier-c-signals", false, "Emit Tier C topology-drift findings to the signals counter and configured sink.")
	fs.DurationVar(&o.computeClassWindow, "compute-class-window", computeclass.DefaultWindow, "How much recent history a compute class's rank shares are computed over.")
	fs.DurationVar(&o.computeClassDwell, "compute-class-dwell", leeway.DefaultDwell().For, "How long a compute class's rank verdict must persist before the source raises a finding.")
	fs.Float64Var(&o.computeClassRank0, "compute-class-rank0-floor", leeway.DefaultRankThresholds().Rank0ShareFloor, "Raise leeway.rank_degraded when less than this fraction of a class's windowed pod-time ran at rank 0 (0 disables).")
	fs.Float64Var(&o.computeClassLastRank, "compute-class-last-rank-ceiling", leeway.DefaultRankThresholds().LastRankShareCeiling, "Raise leeway.rank_degraded when more than this fraction of a class's windowed pod-time ran at its least-preferred priority (0 disables).")
	fs.BoolVar(&o.computeClassTierC, "compute-class-tier-c-signals", false, "Emit Tier C compute-class findings (`leeway.rank_tier_unused`) to the signals counter and configured sink.")

	fs.StringVar(&o.webhookURL, "webhook-url", "", "HTTP webhook receiver base URL (shorthand for --sink=webhook --sink-url=<url>).")
	fs.StringVar(&o.sinkKind, "sink", "", "Incident sink kind: `webhook`, `core-agent`, or `none` (default `none` unless --webhook-url or --sink-url is set).")
	fs.StringVar(&o.sinkURL, "sink-url", "", "Receiver base URL for --sink=webhook or --sink=core-agent (without trailing slash).")
	fs.StringVar(&o.sinkTokenEnv, "sink-token-env", "", "Environment variable name holding the Bearer token for --sink=webhook or --sink=core-agent.")
	fs.BoolVar(&o.dryRun, "dry-run", false, "Log outbound JSON incident and resolution payloads to stderr instead of POSTing to a sink.")
	fs.DurationVar(&o.dedupWindow, "dedup-window", defaultDedupWindow, "Rolling deduplication window for emitted incidents.")
	fs.DurationVar(&o.recoveryStableFor, "recovery-stable-for", defaultRecoveryStableFor, "How long a cleared episode must remain clear before emitting a `kind=resolved` outcome record.")
	fs.StringVar(&o.storePath, "store", "", "Optional SQLite file path for persisting leeway alert dwell timers and EWMA baselines across restarts.")

	fs.DurationVar(&o.exitAfter, "exit-after", 0, "Shut down cleanly after this duration (0 runs until signalled).")
	fs.BoolVar(&o.showVersion, "version", false, "Print the version and exit.")

	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage:\n"+
			"  leeway report [flags]   Run a one-shot topology skew & compute-class report across clusters\n"+
			"  leeway watch  [flags]   Run the continuous leeway daemon (metrics + optional webhook sink)\n"+
			"  leeway version          Print version and exit\n\n"+
			"Flags for `leeway watch` (default when invoked without a subcommand):\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if o.showVersion {
		fmt.Fprintln(stdout, version.String("leeway"))
		return 0
	}
	if o.kubeQPS <= 0 {
		fmt.Fprintf(stderr, "leeway: --kube-qps must be > 0 (got %v)\n", o.kubeQPS)
		return 2
	}
	if o.kubeBurst <= 0 {
		fmt.Fprintf(stderr, "leeway: --kube-burst must be > 0 (got %d)\n", o.kubeBurst)
		return 2
	}
	if o.accessRecheck < 0 {
		fmt.Fprintf(stderr, "leeway: --access-recheck must be >= 0 (got %v)\n", o.accessRecheck)
		return 2
	}
	if o.dedupWindow <= 0 {
		fmt.Fprintf(stderr, "leeway: --dedup-window must be > 0 (got %v)\n", o.dedupWindow)
		return 2
	}
	if o.recoveryStableFor <= 0 {
		fmt.Fprintf(stderr, "leeway: --recovery-stable-for must be > 0 (got %v)\n", o.recoveryStableFor)
		return 2
	}
	if o.topologyDwell <= 0 {
		fmt.Fprintf(stderr, "leeway: --topology-dwell must be > 0 (got %v)\n", o.topologyDwell)
		return 2
	}
	if o.computeClassWindow <= 0 {
		fmt.Fprintf(stderr, "leeway: --compute-class-window must be > 0 (got %v)\n", o.computeClassWindow)
		return 2
	}
	if o.computeClassDwell <= 0 {
		fmt.Fprintf(stderr, "leeway: --compute-class-dwell must be > 0 (got %v)\n", o.computeClassDwell)
		return 2
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "sources" {
			o.sourcesNamed = true
		}
	})
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "leeway: unexpected argument %q\n", fs.Arg(0))
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, o, stderr); err != nil {
		fmt.Fprintf(stderr, "leeway: %v\n", err)
		return 1
	}
	return 0
}

var defaultSources = []string{topologydrift.Name, computeclass.Name}

func defaultTopologyKeys() []string {
	out := make([]string, 0, len(topologydrift.DefaultTopologyKeys))
	for _, k := range topologydrift.DefaultTopologyKeys {
		out = append(out, string(k))
	}
	return out
}

func defaultDomainUnavailableKeys() []string {
	out := make([]string, 0, len(topologydrift.DefaultDomainUnavailableKeys))
	for _, k := range topologydrift.DefaultDomainUnavailableKeys {
		out = append(out, string(k))
	}
	return out
}

func buildSink(o options, stderr io.Writer) (inject.Sink, error) {
	url := strings.TrimSpace(o.sinkURL)
	kind := strings.TrimSpace(o.sinkKind)
	if strings.TrimSpace(o.webhookURL) != "" {
		if url != "" && url != strings.TrimSpace(o.webhookURL) {
			return nil, errors.New("cannot set both --webhook-url and --sink-url to different values")
		}
		url = strings.TrimSpace(o.webhookURL)
		if kind == "" {
			kind = "webhook"
		}
	}
	if kind == "" || kind == "none" {
		if url == "" {
			return nil, nil
		}
		kind = "webhook"
	}

	var token string
	if envName := strings.TrimSpace(o.sinkTokenEnv); envName != "" {
		token = os.Getenv(envName)
		if token == "" {
			return nil, fmt.Errorf("--sink-token-env=%s is set, but environment variable %s is empty", envName, envName)
		}
	}

	switch kind {
	case "webhook":
		if strings.HasPrefix(url, "http://") {
			fmt.Fprintf(stderr, "leeway: warning: webhook URL %s uses plain HTTP\n", inject.RedactedURL(url))
		}
		return inject.NewWebhookSink(inject.WebhookConfig{
			URL:         url,
			BearerToken: token,
		})
	case "core-agent":
		return inject.NewInjector(inject.Config{
			DaemonURL:   url,
			BearerToken: token,
		})
	default:
		return nil, fmt.Errorf("unknown --sink=%q (want webhook, core-agent, or none)", kind)
	}
}

func run(ctx context.Context, o options, stderr *os.File) error {
	if o.exitAfter > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.exitAfter)
		defer cancel()
	}

	sink, err := buildSink(o, stderr)
	if err != nil {
		return err
	}

	if o.storePath != "" {
		st, err := store.Open(o.storePath)
		if err != nil {
			return fmt.Errorf("open store %s: %w", o.storePath, err)
		}
		defer func() { _ = st.Close() }()
		o.openedStore = st
	}

	multiCluster := strings.TrimSpace(o.clusters) != "" ||
		strings.TrimSpace(o.clustersFrom) != "" ||
		strings.TrimSpace(o.profilesDir) != ""
	if multiCluster {
		return runMultiClusterWatch(ctx, o, sink, stderr)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	var registerer prometheus.Registerer = reg
	if o.cluster != "" {
		registerer = prometheus.WrapRegistererWith(prometheus.Labels{"cluster": o.cluster}, reg)
	}

	mp, shutdownMetrics, err := telemetry.SetupMetrics(ctx, telemetry.MetricsOptions{
		Mode:       o.otelExporter,
		Registerer: registerer,
		Cluster:    o.cluster,
	})
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownMetrics(shutdownCtx)
	}()

	region, zone := splitLocation(o.location)
	em, err := newEmitter(registerer, stderr, emitterConfig{
		Cluster:           o.cluster,
		Project:           o.project,
		Region:            region,
		Zone:              zone,
		Sink:              sink,
		DryRun:            o.dryRun,
		DedupWindow:       o.dedupWindow,
		RecoveryStableFor: o.recoveryStableFor,
	})
	if err != nil {
		return err
	}

	kopts := kube.Options{
		InCluster:  o.inCluster,
		Kubeconfig: o.kubeconfig,
		Context:    o.kubeContext,
		QPS:        float32(o.kubeQPS),
		Burst:      o.kubeBurst,
	}
	client, err := kube.BuildClient(kopts)
	if err != nil {
		return err
	}
	dyn, err := kube.BuildDynamicClient(kopts)
	if err != nil {
		return err
	}

	srcs, stopFactory, err := buildSourcesForClients(o, client, dyn, mp, stderr)
	if err != nil {
		return err
	}
	defer stopFactory()

	em.registerObservers(srcs)

	if err := probeSources(ctx, client, srcs, stderr); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- serve(ctx, o.metricsAddr, reg, srcs, stderr)
		cancel()
	}()
	go em.runRecoveryLoop(ctx, recoveryTickFor(o.recoveryStableFor))

	var (
		revMu  sync.Mutex
		revErr error
	)
	aw := sources.NewAccessWatch(sources.NewAccessReviewer(client), srcs)
	if aw.Watching() && o.accessRecheck > 0 {
		go func() {
			_ = aw.Run(ctx, o.accessRecheck, sources.AccessEvents{
				Revoked: func(rev sources.Revocation) {
					fmt.Fprintf(stderr, "leeway: %s\n", rev)
					if !rev.Requirement.Optional {
						revMu.Lock()
						if revErr == nil {
							revErr = rev
						}
						revMu.Unlock()
						cancel()
					}
				},
				Restored: func(source string, req sources.Requirement) {
					fmt.Fprintf(stderr, "leeway: source %q permission restored: %s\n", source, req)
				},
				Error: func(err error) {
					fmt.Fprintf(stderr, "leeway: access recheck error: %v\n", err)
				},
			})
		}()
	}

	runErr := sources.RunAll(ctx, srcs, em.emit)
	cancel()
	if err := <-serveErr; err != nil {
		return err
	}
	revMu.Lock()
	rerr := revErr
	revMu.Unlock()
	if rerr != nil {
		return rerr
	}
	return runErr
}

func runMultiClusterWatch(ctx context.Context, o options, sink inject.Sink, stderr *os.File) error {
	targets, err := clusters.Resolve(ctx, clusters.Options{
		Clusters:     o.clusters,
		ClustersFrom: o.clustersFrom,
		ProfilesDir:  o.profilesDir,
		Kubeconfig:   o.kubeconfig,
		Context:      o.kubeContext,
		InCluster:    o.inCluster,
		ClusterName:  o.cluster,
		Project:      o.project,
		Location:     o.location,
		OnSkip: func(source string, err error) {
			fmt.Fprintf(stderr, "leeway: skipping cluster %s: %v\n", source, err)
		},
	})
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	var (
		allSources []sources.Source
		stops      []func()
		Shutdowns  []func(context.Context) error
		runners    []func(context.Context) error
	)
	defer func() {
		for _, stop := range stops {
			stop()
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, sd := range Shutdowns {
			_ = sd(shutdownCtx)
		}
	}()

	for _, target := range targets {
		c := target
		cfg := rest.CopyConfig(c.Config)
		cfg.QPS = float32(o.kubeQPS)
		cfg.Burst = o.kubeBurst

		client, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			return fmt.Errorf("cluster %s client: %w", c.Name(), err)
		}
		dyn, err := dynamic.NewForConfig(cfg)
		if err != nil {
			return fmt.Errorf("cluster %s dynamic client: %w", c.Name(), err)
		}

		cReg := prometheus.WrapRegistererWith(prometheus.Labels{"cluster": c.Name()}, reg)
		mp, shutdownMetrics, err := telemetry.SetupMetrics(ctx, telemetry.MetricsOptions{
			Mode:       o.otelExporter,
			Registerer: cReg,
			Cluster:    c.Name(),
		})
		if err != nil {
			return err
		}
		Shutdowns = append(Shutdowns, shutdownMetrics)

		co := o
		co.cluster = c.Name()
		srcs, stopFactory, err := buildSourcesForClients(co, client, dyn, mp, stderr)
		if err != nil {
			return fmt.Errorf("cluster %s: %w", c.Name(), err)
		}
		stops = append(stops, stopFactory)

		if err := probeSources(ctx, client, srcs, stderr); err != nil {
			return fmt.Errorf("cluster %s: %w", c.Name(), err)
		}

		region, zone := c.RegionAndZone()
		em, err := newEmitter(cReg, stderr, emitterConfig{
			Cluster:           c.Name(),
			Project:           c.Identity.Project,
			Region:            region,
			Zone:              zone,
			Sink:              sink,
			DryRun:            o.dryRun,
			DedupWindow:       o.dedupWindow,
			RecoveryStableFor: o.recoveryStableFor,
		})
		if err != nil {
			return err
		}
		em.registerObservers(srcs)

		allSources = append(allSources, srcs...)
		runners = append(runners, func(rctx context.Context) error {
			go em.runRecoveryLoop(rctx, recoveryTickFor(o.recoveryStableFor))
			return sources.RunAll(rctx, srcs, em.emit)
		})
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- serve(ctx, o.metricsAddr, reg, allSources, stderr)
		cancel()
	}()

	errCh := make(chan error, len(runners))
	var wg sync.WaitGroup
	for _, fn := range runners {
		wg.Add(1)
		go func(runFn func(context.Context) error) {
			defer wg.Done()
			if err := runFn(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
				cancel()
			}
		}(fn)
	}
	wg.Wait()
	close(errCh)
	cancel()

	if err := <-serveErr; err != nil {
		return err
	}
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func probeSources(ctx context.Context, client kubernetes.Interface, srcs []sources.Source, stderr io.Writer) error {
	notes, err := sources.Probe(ctx, sources.NewAccessReviewer(client), srcs...)
	for _, note := range notes {
		fmt.Fprintf(stderr, "leeway: %s\n", note)
	}
	if err != nil {
		if errors.Is(err, sources.ErrAccessDenied) {
			return err
		}
		fmt.Fprintf(stderr, "leeway: startup access probe inconclusive (%v); deferring to runtime re-check\n", err)
	}
	return nil
}

func buildSourcesForClients(o options, client kubernetes.Interface, dyn dynamic.Interface, mp metric.MeterProvider, stderr io.Writer) ([]sources.Source, func(), error) {
	names := splitCSV(o.sources)
	if len(names) == 0 {
		return nil, nil, errors.New("--sources names nothing to run")
	}
	factory := kube.NewTransformingFactory(client)
	stop := func() {
		factory.Shutdown()
	}

	var out []sources.Source
	seen := map[string]bool{}
	for _, name := range names {
		if seen[name] {
			return nil, nil, fmt.Errorf("--sources names %q twice", name)
		}
		seen[name] = true
		switch name {
		case topologydrift.Name:
			keys := o.topologyKeys
			parsed, perr := topologydrift.ParseClusterDefaults(o.topologyDefaults)
			if perr != nil {
				return nil, nil, perr
			}
			topoKeys := topologyKeysFrom(keys)
			if len(topoKeys) == 0 {
				return nil, nil, errors.New("--topology-keys must name at least one node label")
			}
			cfg := topologydrift.Config{
				Cluster:                   o.cluster,
				TopologyKeys:              topoKeys,
				DomainUnavailableKeys:     topologyKeysFrom(o.topologyDomainKeys),
				Dwell:                     leeway.Dwell{For: o.topologyDwell},
				ClusterDefaultConstraints: parsed,
				PerDomainSeries:           o.topologyPerDomain,
				TierCSignals:              o.topologyTierC,
			}
			if o.topologyDwell > 0 && o.topologyDwell < topologydrift.DefaultAlertInterval {
				tick := max(100*time.Millisecond, o.topologyDwell/2)
				cfg.AlertInterval = tick
				cfg.ReadySampleInterval = tick
				cfg.CoalesceWindow = min(topologydrift.DefaultCoalesceWindow, tick)
				cfg.RolloutCoalesceWindow = min(topologydrift.DefaultRolloutCoalesceWindow, tick)
			}
			src := topologydrift.New(client, cfg)
			src.WithFactory(factory)
			src.WithDynamic(dyn)
			src.WithMeter(mp.Meter(topologydrift.MeterName))
			if o.openedStore != nil {
				src.WithStore(o.openedStore, o.cluster)
			}
			out = append(out, src)
		case computeclass.Name:
			if !o.sourcesNamed && !computeclass.ComputeClassesServed(client) {
				fmt.Fprintf(stderr, "leeway: source %s skipped — this cluster does not serve the GKE ComputeClass CRD (pass --sources=%s to make that a startup error instead)\n",
					name, name)
				continue
			}
			cfg := computeclass.DefaultConfig()
			cfg.Cluster = o.cluster
			cfg.Window = o.computeClassWindow
			cfg.Dwell = leeway.Dwell{For: o.computeClassDwell}
			cfg.Rank0ShareFloor = &o.computeClassRank0
			cfg.LastRankShareCeiling = &o.computeClassLastRank
			cfg.TierCSignals = o.computeClassTierC
			if minD := min(o.computeClassDwell, o.computeClassWindow); minD > 0 && minD < computeclass.DefaultAlertInterval {
				tick := max(100*time.Millisecond, minD/2)
				cfg.AlertInterval = tick
				cfg.FlushInterval = min(computeclass.DefaultFlushInterval, tick)
			}
			src, cerr := computeclass.New(client, dyn, cfg)
			if cerr != nil {
				return nil, nil, cerr
			}
			src.WithFactory(factory)
			src.WithMeter(mp.Meter(computeclass.MeterName))
			if o.openedStore != nil {
				src.WithStore(o.openedStore, o.cluster)
			}
			out = append(out, src)
		default:
			return nil, nil, fmt.Errorf("--sources: unknown source %q (this binary runs %s)",
				name, strings.Join(defaultSources, " and "))
		}
		fmt.Fprintf(stderr, "leeway: source %s enabled\n", name)
	}
	if len(out) == 0 {
		return nil, nil, errors.New("every source was skipped: this process would export an empty endpoint forever, which is indistinguishable from a healthy cluster")
	}
	return out, stop, nil
}

func topologyKeysFrom(csv string) []leeway.TopologyKey {
	fields := splitCSV(csv)
	out := make([]leeway.TopologyKey, 0, len(fields))
	for _, f := range fields {
		out = append(out, leeway.TopologyKey(f))
	}
	return out
}

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func splitLocation(loc string) (region, zone string) {
	loc = strings.TrimSpace(loc)
	if loc == "" {
		return "", ""
	}
	if strings.Count(loc, "-") >= 2 {
		if idx := strings.LastIndexByte(loc, '-'); idx > 0 {
			return loc[:idx], loc
		}
	}
	return loc, ""
}
