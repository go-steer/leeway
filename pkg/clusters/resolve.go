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

package clusters

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"

	"golang.org/x/oauth2"
	container "google.golang.org/api/container/v1"
	"k8s.io/client-go/rest"

	"github.com/go-steer/leeway/pkg/kube"
)

const (
	// DirectProfile is the Profile identifier assigned to a cluster reached
	// directly via --in-cluster or --kubeconfig rather than a Hermes profile.
	DirectProfile = "direct"

	kubeconfigPrefix = "kubeconfig"
	profilesPrefix   = "profiles:"
)

// Options configures unified multi-cluster discovery for `leeway report`,
// `leeway mcp`, and `leeway watch`.
type Options struct {
	// Clusters is the comma-separated --clusters value. Items may be:
	//   - "projects/<project>/locations/<location>/clusters/<cluster>" (GKE resource path)
	//   - "project/location/cluster" (GKE cluster identity triple)
	//   - "gke_<project>_<location>_<cluster>" (gcloud context / GKE identity)
	//   - "name=https://endpoint" (explicit API server endpoint)
	//   - "<context-name>" (kubeconfig context, or filter when ProfilesDir/ClustersFrom is set)
	Clusters string

	// ClustersFrom is the comma-separated --clusters-from fleet discovery source:
	//   - "kubeconfig" or "kubeconfig:/path/to/config"
	//   - "profiles:/path/to/profiles"
	//   - "<project>", "<project>/<location>", or "projects/<project>/locations/<loc|->"
	ClustersFrom string

	// ProfilesDir is the --profiles-dir path to a Hermes profiles directory
	// (e.g. /opt/data/profiles) where each Cluster Agent profile has a
	// config.yaml with a cluster_identity block.
	ProfilesDir string

	// Single-cluster / direct-cluster flags.
	Kubeconfig  string
	Context     string
	InCluster   bool
	ClusterName string
	Project     string
	Location    string

	// GoogleAuth selects "adc" or "injected" when no credential proxy is set.
	// Empty consults LEEWAY_GOOGLE_AUTH, CORE_AGENT_GOOGLE_AUTH, and MAST_GOOGLE_AUTH.
	GoogleAuth string

	// KeepFailedScopes, when true on multi-scope --clusters-from discovery,
	// retains scopes that failed discovery (while at least one other scope
	// succeeded) as Cluster entries with non-nil ResolveError so `leeway report`
	// and `leeway mcp` emit `"status": "error"` entries rather than aborting.
	KeepFailedScopes bool

	// Test and runtime injection seams.
	Getenv      func(string) string
	Describe    func(ctx context.Context, id Identity) (*container.Cluster, error)
	ListGKE     func(ctx context.Context, parent string) ([]*container.Cluster, error)
	TokenSource func(ctx context.Context) (oauth2.TokenSource, error)
	GKEEndpoint string
	Broker      *Broker
	BuildDirect func(kubeconfig, kubectx string, inCluster bool) (*rest.Config, error)
	OnSkip      func(source string, err error)
}

func (o Options) getenv() func(string) string {
	if o.Getenv != nil {
		return o.Getenv
	}
	return os.Getenv
}

// Name returns the cluster's display/logical name (Identity.Cluster).
func (c Cluster) Name() string {
	return c.Identity.Cluster
}

// RegionAndZone splits Location into (region, zone). A regional GKE location
// (e.g. "us-central1") returns ("us-central1", ""); a zonal GKE location
// (e.g. "us-central1-a") returns ("us-central1", "us-central1-a").
func (c Cluster) RegionAndZone() (region, zone string) {
	loc := strings.TrimSpace(c.Identity.Location)
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

// Resolve turns Options into the ordered list of target Clusters to scan or
// watch.
func Resolve(ctx context.Context, opts Options) ([]Cluster, error) {
	getenv := opts.getenv()
	googleAuthMode, err := ResolveGoogleAuthModeWith(opts.GoogleAuth, getenv)
	if err != nil {
		return nil, err
	}

	broker := opts.Broker
	if broker == nil {
		broker, err = BrokerFromEnvWith(getenv)
		if err != nil {
			return nil, err
		}
	}

	profilesDir := strings.TrimSpace(opts.ProfilesDir)
	clustersFrom := strings.TrimSpace(opts.ClustersFrom)
	rawScopes := splitCSV(clustersFrom)
	if len(rawScopes) == 1 {
		if after, ok := strings.CutPrefix(rawScopes[0], profilesPrefix); ok {
			dir := strings.TrimSpace(after)
			if dir == "" {
				return nil, fmt.Errorf("--clusters-from=%q: missing profiles directory path", opts.ClustersFrom)
			}
			if profilesDir == "" {
				profilesDir = dir
			}
			rawScopes = nil
		}
	}

	specs := splitCSV(opts.Clusters)
	disc := Discoverer{
		Describe:    opts.Describe,
		TokenSource: opts.TokenSource,
		GoogleAuth:  googleAuthMode,
		GKEEndpoint: opts.GKEEndpoint,
		Broker:      broker,
		OnSkip:      opts.OnSkip,
	}

	// 1. Hermes --profiles-dir path (with optional --clusters filter and
	// optional direct --in-cluster / --kubeconfig management cluster merge).
	if profilesDir != "" && len(rawScopes) == 0 {
		return resolveFromProfilesDir(ctx, profilesDir, specs, opts, disc)
	}

	// 2. --clusters-from fleet discovery (one or more comma-separated scopes:
	// kubeconfig[:path], profiles:<dir>, or GKE project[/location]).
	if len(rawScopes) > 0 {
		return resolveFromScopes(ctx, rawScopes, specs, opts, disc, googleAuthMode)
	}

	// 3. Explicit --clusters list without a discovery source.
	if len(specs) > 0 {
		return resolveExplicitSpecs(ctx, specs, opts, disc, googleAuthMode)
	}

	// 4. Single-cluster fallback (--in-cluster, --kubeconfig, --context).
	return resolveSingleCluster(opts, disc.Broker)
}

func resolveFromProfilesDir(ctx context.Context, dir string, filterSpecs []string, opts Options, disc Discoverer) ([]Cluster, error) {
	return resolveFromProfilesDirScope(ctx, dir, filterSpecs, false, opts, disc)
}

func resolveFromProfilesDirScope(ctx context.Context, dir string, filterSpecs []string, multiScope bool, opts Options, disc Discoverer) ([]Cluster, error) {
	if len(filterSpecs) > 0 {
		disc.Want = func(id Identity) bool {
			return matchesFilter(id, "", filterSpecs)
		}
	}
	discovered, err := disc.Discover(ctx, dir)
	if err != nil {
		return nil, err
	}

	// Filter also by profile name if any caller passed a profile directory name in --clusters.
	if len(filterSpecs) > 0 && len(discovered) == 0 {
		discAll := disc
		discAll.Want = nil
		all, err := discAll.Discover(ctx, dir)
		if err != nil {
			return nil, err
		}
		for _, c := range all {
			if matchesFilter(c.Identity, c.Profile, filterSpecs) {
				discovered = append(discovered, c)
			}
		}
	}

	// When --in-cluster or --kubeconfig is also supplied (and either no filter
	// was passed or the direct cluster matches the filter), merge the direct
	// management cluster using k8s-event-watcher's deduplication rule:
	// the direct client wins on a unique name match and absorbs the profile's
	// project/location/autopilot metadata.
	wantDirect := (opts.InCluster || opts.Kubeconfig != "" || opts.Context != "") &&
		(len(filterSpecs) == 0 || matchesDirectFilter(opts.ClusterName, filterSpecs))
	if wantDirect {
		byName := make(map[string][]Cluster)
		for _, c := range discovered {
			byName[c.Identity.Cluster] = append(byName[c.Identity.Cluster], c)
		}
		single, err := resolveSingleCluster(opts, disc.Broker)
		if err != nil {
			return nil, err
		}
		direct := single[0]
		candidates := byName[direct.Identity.Cluster]
		if len(candidates) <= 1 {
			if len(candidates) == 1 {
				covered := candidates[0]
				discovered = removeProfileCluster(discovered, covered.Profile)
				direct.Identity.Project = covered.Identity.Project
				direct.Identity.Location = covered.Identity.Location
				direct.Autopilot = covered.Autopilot
			}
			discovered = append(discovered, direct)
		}
	}

	if len(discovered) == 0 {
		if len(filterSpecs) > 0 {
			if multiScope {
				return nil, nil
			}
			return nil, fmt.Errorf("no clusters in %s matched --clusters=%s", dir, strings.Join(filterSpecs, ","))
		}
		if multiScope {
			return nil, nil
		}
		return nil, fmt.Errorf("no clusters to reach: %s contained no Cluster Agent profiles and neither --in-cluster nor --kubeconfig was given", dir)
	}
	return discovered, nil
}

type scopeKind int

const (
	scopeKindGKE scopeKind = iota
	scopeKindKubeconfig
	scopeKindProfiles
)

type parsedScope struct {
	kind     scopeKind
	key      string // canonical dedup key
	display  string // logical cluster name on partial-failure ClusterReport
	source   string // source string on partial-failure ClusterReport
	path     string // kubeconfig path or profiles dir
	project  string
	location string
}

func parseScope(raw, fullFlag string) (parsedScope, error) {
	s := strings.TrimSpace(raw)
	if kcPath, ok := parseKubeconfigFrom(s); ok {
		disp := kubeconfigPrefix
		if kcPath != "" {
			disp = kubeconfigPrefix + ":" + kcPath
		}
		return parsedScope{
			kind:    scopeKindKubeconfig,
			key:     "kc:" + kcPath,
			display: disp,
			source:  disp,
			path:    kcPath,
		}, nil
	}
	if after, ok := strings.CutPrefix(s, profilesPrefix); ok {
		dir := strings.TrimSpace(after)
		if dir == "" {
			return parsedScope{}, fmt.Errorf("--clusters-from=%q: missing directory in %q", fullFlag, s)
		}
		disp := profilesPrefix + dir
		return parsedScope{
			kind:    scopeKindProfiles,
			key:     "profiles:" + dir,
			display: disp,
			source:  disp,
			path:    dir,
		}, nil
	}
	project, location, ok := NormalizeScopeToken(s)
	if !ok {
		return parsedScope{}, fmt.Errorf("--clusters-from=%q: scope %q must be kubeconfig[:path], profiles:<dir>, or <project>[/<location>]", fullFlag, s)
	}
	disp := project
	if location != "" {
		disp = project + "/" + location
	}
	return parsedScope{
		kind:     scopeKindGKE,
		key:      "gke:" + disp,
		display:  disp,
		source:   "gke:" + disp,
		project:  project,
		location: location,
	}, nil
}

// NormalizeScopeToken parses a single GCP fleet scope token (`<project>`,
// `<project>/<location>`, `<project>/-`, `projects/<project>`, or
// `projects/<project>/locations/<location|->`) into `(project, location, true)`.
func NormalizeScopeToken(raw string) (project, location string, ok bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", "", false
	}
	if strings.HasPrefix(s, "projects/") {
		parts := strings.Split(s, "/")
		switch {
		case len(parts) == 2 && parts[1] != "":
			project = parts[1]
		case len(parts) == 4 && (parts[2] == "locations" || parts[2] == "zones") && parts[1] != "" && parts[3] != "":
			project = parts[1]
			if parts[3] != "-" {
				location = parts[3]
			}
		default:
			return "", "", false
		}
	} else {
		parts := strings.Split(s, "/")
		switch len(parts) {
		case 1:
			project = parts[0]
		case 2:
			project = parts[0]
			if parts[1] == "" {
				return "", "", false
			}
			if parts[1] != "-" {
				location = parts[1]
			}
		default:
			return "", "", false
		}
	}
	if !validGCPProjectScope(project) {
		return "", "", false
	}
	if location != "" && (strings.Contains(location, ":") || !brokerSegment.MatchString(location)) {
		return "", "", false
	}
	return project, location, true
}

func validGCPProjectScope(project string) bool {
	if !brokerSegment.MatchString(project) {
		return false
	}
	// Reject scheme-like colons ("bad:foo") while allowing legacy domain-scoped
	// GCP projects ("example.com:my-project") where the domain contains a dot.
	if dom, rest, hasColon := strings.Cut(project, ":"); hasColon {
		return strings.Contains(dom, ".") && dom != "" && rest != "" && !strings.Contains(rest, ":")
	}
	return true
}

func resolveFromScopes(ctx context.Context, rawScopes, filterSpecs []string, opts Options, disc Discoverer, googleAuthMode string) ([]Cluster, error) {
	var scopes []parsedScope
	seenScopes := make(map[string]bool, len(rawScopes))
	for _, raw := range rawScopes {
		ps, err := parseScope(raw, opts.ClustersFrom)
		if err != nil {
			return nil, err
		}
		if !seenScopes[ps.key] {
			seenScopes[ps.key] = true
			scopes = append(scopes, ps)
		}
	}

	var (
		all            []Cluster
		seenClusters   = make(map[string]bool)
		scopeErrs      []error
		succeededCount int
	)

	appendUnique := func(list []Cluster) {
		for _, c := range list {
			dedupKey := c.Profile
			if dedupKey == "" {
				dedupKey = c.Identity.String()
			}
			if !seenClusters[dedupKey] {
				seenClusters[dedupKey] = true
				all = append(all, c)
			}
		}
	}

	for _, sc := range scopes {
		found, err := resolveSingleScope(ctx, sc, len(scopes) > 1, filterSpecs, opts, disc, googleAuthMode)
		if err != nil {
			scopeErrs = append(scopeErrs, err)
			if opts.OnSkip != nil {
				opts.OnSkip(sc.display, err)
			}
			if opts.KeepFailedScopes {
				all = append(all, Cluster{
					Identity:     Identity{Cluster: sc.display, Project: sc.project, Location: sc.location},
					Profile:      sc.source,
					ResolveError: err,
				})
			}
			continue
		}
		succeededCount++
		appendUnique(found)
	}

	if succeededCount == 0 {
		if len(scopeErrs) == 1 {
			return nil, scopeErrs[0]
		}
		return nil, errors.Join(scopeErrs...)
	}

	// Count actual runnable clusters (excluding KeepFailedScopes error entries).
	runnable := 0
	for _, c := range all {
		if c.ResolveError == nil {
			runnable++
		}
	}
	if runnable == 0 && len(scopeErrs) == 0 {
		return nil, fmt.Errorf("no reachable clusters resolved from --clusters-from=%s", opts.ClustersFrom)
	}
	if runnable == 0 && !opts.KeepFailedScopes {
		return nil, fmt.Errorf("no reachable clusters resolved from --clusters-from=%s", opts.ClustersFrom)
	}
	return all, nil
}

func resolveSingleScope(ctx context.Context, sc parsedScope, multiScope bool, filterSpecs []string, opts Options, disc Discoverer, googleAuthMode string) ([]Cluster, error) {
	switch sc.kind {
	case scopeKindProfiles:
		return resolveFromProfilesDirScope(ctx, sc.path, filterSpecs, multiScope, opts, disc)

	case scopeKindKubeconfig:
		kcPath := sc.path
		if kcPath == "" {
			kcPath = effectiveKubeconfigPath(opts)
		}
		ctxs, err := kube.ListContexts(kcPath)
		if err != nil {
			return nil, err
		}
		var out []Cluster
		for _, c := range ctxs {
			id := Identity{Cluster: c.Name, Project: opts.Project, Location: opts.Location}
			if gkeID, ok := ParseClusterRef(c.Name); ok {
				if id.Project == "" {
					id.Project = gkeID.Project
				}
				if id.Location == "" {
					id.Location = gkeID.Location
				}
			}
			if len(filterSpecs) > 0 && !matchesKubeconfigFilter(c.Name, filterSpecs) {
				continue
			}
			if disc.Broker != nil && disc.Broker.ShellMode {
				out = append(out, Cluster{
					Identity:   id,
					Profile:    "kubeconfig:" + c.Name,
					Kubeconfig: kcPath,
					Context:    c.Name,
				})
				continue
			}
			cfg, err := kube.ConfigForContext(kcPath, c.Name)
			if err != nil {
				if opts.OnSkip != nil {
					opts.OnSkip(c.Name, err)
				}
				continue
			}
			out = append(out, Cluster{
				Identity: id,
				Profile:  "kubeconfig:" + c.Name,
				Config:   cfg,
			})
		}
		if len(out) == 0 && !multiScope {
			return nil, fmt.Errorf("no clusters resolved from %s", sc.display)
		}
		return out, nil

	default:
		loc := sc.location
		if loc == "" {
			loc = "-"
		}
		parent := fmt.Sprintf("projects/%s/locations/%s", sc.project, loc)
		var ts oauth2.TokenSource
		if opts.ListGKE == nil && disc.Broker == nil && (googleAuthMode == GoogleAuthInjected || disc.TokenSource != nil || strings.TrimSpace(disc.GKEEndpoint) != "") {
			var tsErr error
			ts, tsErr = disc.tokenSource(ctx)
			if tsErr != nil {
				return nil, fmt.Errorf("credentials for %s: %w", parent, tsErr)
			}
		}
		gkeClusters, err := listGKEClusters(ctx, parent, opts.ListGKE, disc.Broker, ts, googleAuthMode, disc.GKEEndpoint)
		if err != nil {
			return nil, fmt.Errorf("list GKE clusters under %s: %w", parent, err)
		}

		var out []Cluster
		var lastSkipErr error
		matchedAny := false
		for _, gc := range gkeClusters {
			if gc == nil {
				continue
			}
			id := Identity{
				Project:  sc.project,
				Location: gc.Location,
				Cluster:  gc.Name,
			}
			if len(filterSpecs) > 0 && !matchesFilter(id, "", filterSpecs) {
				continue
			}
			matchedAny = true
			if disc.Broker == nil && ts == nil {
				ts, err = disc.tokenSource(ctx)
				if err != nil {
					lastSkipErr = err
					if opts.OnSkip != nil {
						opts.OnSkip(id.String(), err)
					}
					continue
				}
			}
			cfg, err := disc.clientConfig(id, gc, ts)
			if err != nil {
				lastSkipErr = err
				if opts.OnSkip != nil {
					opts.OnSkip(id.String(), err)
				}
				continue
			}
			c := Cluster{
				Identity:  id,
				Profile:   "gke:" + id.String(),
				Config:    cfg,
				Autopilot: gc.Autopilot != nil && gc.Autopilot.Enabled,
			}
			if disc.Broker != nil && disc.Broker.ShellMode {
				c.Context = id.ContextName()
			}
			out = append(out, c)
		}
		if len(out) == 0 {
			if matchedAny && lastSkipErr != nil {
				return nil, fmt.Errorf("no reachable GKE clusters under %s: %w", parent, lastSkipErr)
			}
			if !multiScope {
				return nil, fmt.Errorf("no reachable GKE clusters found under %s", parent)
			}
		}
		return out, nil
	}
}

type explicitResolver struct {
	opts           Options
	disc           Discoverer
	googleAuthMode string
	explicitKC     bool
	envKC          bool

	ts         oauth2.TokenSource
	out        []Cluster
	seen       map[string]bool
	lastErr    error
	kcLoaded   bool
	kcContexts []kube.ContextRef
	kcLoadErr  error
}

func resolveExplicitSpecs(ctx context.Context, specs []string, opts Options, disc Discoverer, googleAuthMode string) ([]Cluster, error) {
	explicitKC := strings.TrimSpace(opts.Kubeconfig) != ""
	explicitInjectedFlag := strings.EqualFold(strings.TrimSpace(opts.GoogleAuth), GoogleAuthInjected)
	r := &explicitResolver{
		opts:           opts,
		disc:           disc,
		googleAuthMode: googleAuthMode,
		explicitKC:     explicitKC,
		// KUBECONFIG takes precedence over ambient CORE_AGENT_GOOGLE_AUTH=injected
		// / MAST_GOOGLE_AUTH=injected task defaults (unless the caller explicitly
		// passed --google-auth=injected without --kubeconfig).
		envKC: !explicitKC &&
			strings.TrimSpace(opts.getenv()("KUBECONFIG")) != "" &&
			disc.Broker == nil &&
			disc.Describe == nil &&
			!explicitInjectedFlag,
		seen: make(map[string]bool, len(specs)),
	}

	for _, spec := range specs {
		if err := r.resolveOne(ctx, spec); err != nil {
			return nil, err
		}
	}

	if len(r.out) == 0 {
		if r.lastErr != nil {
			return nil, fmt.Errorf("no clusters could be resolved from --clusters=%s: %w", strings.Join(specs, ","), r.lastErr)
		}
		return nil, fmt.Errorf("no clusters could be resolved from --clusters=%s", strings.Join(specs, ","))
	}
	return r.out, nil
}

func (r *explicitResolver) loadKC(kcPath string) ([]kube.ContextRef, error) {
	if !r.kcLoaded {
		r.kcContexts, r.kcLoadErr = kube.ListContexts(kcPath)
		r.kcLoaded = true
	}
	return r.kcContexts, r.kcLoadErr
}

func (r *explicitResolver) recordErr(target string, err error) error {
	r.lastErr = err
	if r.opts.OnSkip != nil {
		r.opts.OnSkip(target, err)
		return nil
	}
	return err
}

func (r *explicitResolver) resolveOne(ctx context.Context, spec string) error {
	if name, endpoint, ok := strings.Cut(spec, "="); ok {
		return r.resolveEndpoint(ctx, spec, name, endpoint)
	}
	if id, ok := ParseClusterRef(spec); ok {
		return r.resolveGKERef(ctx, strings.TrimSpace(spec), id)
	}
	return r.resolvePlainContext(spec)
}

func (r *explicitResolver) resolveEndpoint(ctx context.Context, spec, name, endpoint string) error {
	name = strings.TrimSpace(name)
	endpoint = strings.TrimSpace(endpoint)
	if name == "" || endpoint == "" {
		return fmt.Errorf("invalid --clusters entry %q: expected name=endpoint", spec)
	}
	host := endpoint
	if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		host = "https://" + host
	}
	cfg := &rest.Config{Host: host}
	if r.disc.TokenSource != nil || r.googleAuthMode == GoogleAuthInjected {
		if tok, err := r.disc.tokenSource(ctx); err == nil && tok != nil {
			UseGoogleTokenSource(cfg, tok)
		}
	}
	key := "endpoint:" + name
	if r.seen[key] {
		return nil
	}
	r.seen[key] = true
	r.out = append(r.out, Cluster{
		Identity: Identity{Cluster: name, Project: r.opts.Project, Location: r.opts.Location},
		Profile:  key,
		Config:   cfg,
	})
	return nil
}

func (r *explicitResolver) resolveGKERef(ctx context.Context, trimmedSpec string, id Identity) error {
	isGKEContext := strings.HasPrefix(trimmedSpec, "gke_")

	if r.disc.Broker != nil && r.disc.Broker.ShellMode && r.disc.Describe == nil {
		r.appendShellModeGKERef(trimmedSpec, id, isGKEContext)
		return nil
	}
	if r.explicitKC || r.envKC {
		return r.resolveGKEFromKubeconfig(trimmedSpec, id)
	}
	if isGKEContext && r.disc.Broker == nil && r.disc.Describe == nil && r.googleAuthMode != GoogleAuthInjected {
		if r.tryDefaultKubeconfigContext(trimmedSpec, id) {
			return nil
		}
	}
	if r.seen[id.String()] {
		return nil
	}
	r.seen[id.String()] = true

	if r.disc.Broker != nil && r.disc.Broker.CredentialProxy && !r.disc.Broker.ShellMode && r.disc.Describe == nil {
		return r.resolveGKEViaCredentialProxy(ctx, id)
	}
	return r.resolveGKEViaAPI(ctx, id)
}

func (r *explicitResolver) appendShellModeGKERef(trimmedSpec string, id Identity, isGKEContext bool) {
	if isGKEContext {
		key := "ctx:" + trimmedSpec
		if !r.seen[key] {
			r.seen[key] = true
			r.out = append(r.out, Cluster{
				Identity:   id,
				Profile:    "kubeconfig:" + trimmedSpec,
				Kubeconfig: r.opts.Kubeconfig,
				Context:    trimmedSpec,
			})
		}
		return
	}
	if !r.seen[id.String()] {
		r.seen[id.String()] = true
		r.out = append(r.out, Cluster{
			Identity:   id,
			Profile:    "gke:" + id.String(),
			Kubeconfig: r.opts.Kubeconfig,
			Context:    id.ContextName(),
		})
	}
}

func (r *explicitResolver) resolveGKEFromKubeconfig(trimmedSpec string, id Identity) error {
	kcPath := effectiveKubeconfigPath(r.opts)
	ctxs, err := r.loadKC(kcPath)
	if err != nil {
		return err
	}
	matched := matchKubeconfigContext(ctxs, trimmedSpec, id)
	if matched == "" {
		kcErr := fmt.Errorf("kubeconfig %s: no context matching %q (tried %q, %q, and %q)",
			kubeconfigLabel(kcPath, r.opts.getenv()), trimmedSpec, trimmedSpec, id.ContextName(), id.String())
		return r.recordErr(trimmedSpec, kcErr)
	}
	key := "ctx:" + matched
	if r.seen[key] {
		return nil
	}
	r.seen[key] = true
	cfg, err := kube.ConfigForContext(kcPath, matched)
	if err != nil {
		return r.recordErr(matched, err)
	}
	r.out = append(r.out, Cluster{
		Identity: Identity{
			Cluster:  matched,
			Project:  firstNonEmpty(r.opts.Project, id.Project),
			Location: firstNonEmpty(r.opts.Location, id.Location),
		},
		Profile: "kubeconfig:" + matched,
		Config:  cfg,
	})
	return nil
}

func (r *explicitResolver) tryDefaultKubeconfigContext(trimmedSpec string, id Identity) bool {
	ctxs, err := r.loadKC("")
	if err != nil {
		return false
	}
	matched := matchKubeconfigContext(ctxs, trimmedSpec, id)
	if matched == "" {
		return false
	}
	cfg, err := kube.ConfigForContext("", matched)
	if err != nil {
		return false
	}
	key := "ctx:" + matched
	if !r.seen[key] {
		r.seen[key] = true
		r.out = append(r.out, Cluster{
			Identity: Identity{
				Cluster:  matched,
				Project:  firstNonEmpty(r.opts.Project, id.Project),
				Location: firstNonEmpty(r.opts.Location, id.Location),
			},
			Profile: "kubeconfig:" + matched,
			Config:  cfg,
		})
	}
	return true
}

func (r *explicitResolver) resolveGKEViaCredentialProxy(ctx context.Context, id Identity) error {
	cfg, err := r.disc.Broker.ClientConfig(id)
	if err != nil {
		return r.recordErr(id.String(), err)
	}
	var autopilot bool
	described, descErr := r.disc.Broker.describeCluster(ctx, id)
	switch {
	case descErr == nil && described != nil:
		autopilot = described.Autopilot != nil && described.Autopilot.Enabled
	case descErr != nil:
		var transport *url.Error
		if errors.As(descErr, &transport) {
			return r.recordErr(id.String(), fmt.Errorf("credential proxy %s unreachable for %s: %w", r.disc.Broker.URL, id, descErr))
		}
		if why, own := brokerOwnFailure(descErr); own {
			return r.recordErr(id.String(), fmt.Errorf("credential proxy %s answered %s for %s: %w", r.disc.Broker.URL, why, id, descErr))
		}
	}
	r.out = append(r.out, Cluster{
		Identity:  id,
		Profile:   "gke:" + id.String(),
		Config:    cfg,
		Autopilot: autopilot,
	})
	return nil
}

func (r *explicitResolver) resolveGKEViaAPI(ctx context.Context, id Identity) error {
	if r.disc.Broker == nil && r.ts == nil {
		var err error
		r.ts, err = r.disc.tokenSource(ctx)
		if err != nil {
			return r.recordErr(id.String(), fmt.Errorf("credentials for %s: %w", id, err))
		}
	}
	described, err := r.disc.describe(ctx, id)
	if err != nil {
		return r.recordErr(id.String(), fmt.Errorf("describe GKE cluster %s: %w", id, err))
	}
	cfg, err := r.disc.clientConfig(id, described, r.ts)
	if err != nil {
		return r.recordErr(id.String(), fmt.Errorf("client config for %s: %w", id, err))
	}
	r.out = append(r.out, Cluster{
		Identity:  id,
		Profile:   "gke:" + id.String(),
		Config:    cfg,
		Autopilot: described.Autopilot != nil && described.Autopilot.Enabled,
	})
	return nil
}

func (r *explicitResolver) resolvePlainContext(spec string) error {
	if r.seen["ctx:"+spec] {
		return nil
	}
	r.seen["ctx:"+spec] = true
	if r.disc.Broker != nil && r.disc.Broker.ShellMode {
		r.out = append(r.out, Cluster{
			Identity:   Identity{Cluster: spec, Project: r.opts.Project, Location: r.opts.Location},
			Profile:    "kubeconfig:" + spec,
			Kubeconfig: r.opts.Kubeconfig,
			Context:    spec,
		})
		return nil
	}
	cfg, err := kube.ConfigForContext(effectiveKubeconfigPath(r.opts), spec)
	if err != nil {
		return r.recordErr(spec, err)
	}
	r.out = append(r.out, Cluster{
		Identity: Identity{
			Cluster:  spec,
			Project:  r.opts.Project,
			Location: r.opts.Location,
		},
		Profile: "kubeconfig:" + spec,
		Config:  cfg,
	})
	return nil
}

func resolveSingleCluster(opts Options, broker *Broker) ([]Cluster, error) {
	if broker != nil && broker.ShellMode && opts.BuildDirect == nil && !opts.InCluster {
		id := Identity{
			Cluster:  defaultClusterName(opts.ClusterName, opts.Context),
			Project:  opts.Project,
			Location: opts.Location,
		}
		if gkeID, ok := ParseClusterRef(opts.Context); ok {
			if strings.TrimSpace(opts.ClusterName) == "" {
				id.Cluster = gkeID.Cluster
			}
			if id.Project == "" {
				id.Project = gkeID.Project
			}
			if id.Location == "" {
				id.Location = gkeID.Location
			}
		}
		return []Cluster{{
			Identity:   id,
			Profile:    DirectProfile,
			Kubeconfig: opts.Kubeconfig,
			Context:    opts.Context,
		}}, nil
	}

	// Native CREDENTIAL_PROXY_URL single-cluster resolution when a complete GKE
	// identity is supplied via --context or --project/--location/--cluster-name.
	if broker != nil && broker.CredentialProxy && !broker.ShellMode && opts.BuildDirect == nil && !opts.InCluster && strings.TrimSpace(opts.Kubeconfig) == "" {
		var id Identity
		if gkeID, ok := ParseClusterRef(opts.Context); ok {
			id = gkeID
		} else if opts.Project != "" && opts.Location != "" && opts.ClusterName != "" {
			id = Identity{Project: opts.Project, Location: opts.Location, Cluster: opts.ClusterName}
		}
		if id.complete() {
			cfg, err := broker.ClientConfig(id)
			if err != nil {
				return nil, err
			}
			return []Cluster{{
				Identity: id,
				Profile:  "gke:" + id.String(),
				Config:   cfg,
			}}, nil
		}
	}

	effectiveOpts := opts
	if effectiveOpts.Kubeconfig == "" {
		effectiveOpts.Kubeconfig = effectiveKubeconfigPath(opts)
	}
	if id, ok := ParseClusterRef(opts.Context); ok && !opts.InCluster && opts.BuildDirect == nil {
		if ctxs, err := kube.ListContexts(effectiveOpts.Kubeconfig); err == nil {
			if matched := matchKubeconfigContext(ctxs, strings.TrimSpace(opts.Context), id); matched != "" {
				effectiveOpts.Context = matched
			}
		}
	}

	cfg, err := buildDirectConfig(effectiveOpts)
	if err != nil {
		return nil, err
	}
	name := defaultClusterName(effectiveOpts.ClusterName, effectiveOpts.Context)
	return []Cluster{{
		Identity: Identity{
			Cluster:  name,
			Project:  effectiveOpts.Project,
			Location: effectiveOpts.Location,
		},
		Profile: DirectProfile,
		Config:  cfg,
	}}, nil
}

func matchKubeconfigContext(ctxs []kube.ContextRef, rawSpec string, id Identity) string {
	names := make([]string, len(ctxs))
	for i, c := range ctxs {
		names[i] = c.Name
	}
	for _, candidate := range []string{rawSpec, id.ContextName(), id.String()} {
		if candidate != "" && slices.Contains(names, candidate) {
			return candidate
		}
	}
	for _, name := range names {
		if cid, ok := ParseClusterRef(name); ok && cid == id {
			return name
		}
	}
	return ""
}

func effectiveKubeconfigPath(opts Options) string {
	if p := strings.TrimSpace(opts.Kubeconfig); p != "" {
		return p
	}
	if opts.Getenv != nil {
		if envP := strings.TrimSpace(opts.Getenv("KUBECONFIG")); envP != "" && envP != strings.TrimSpace(os.Getenv("KUBECONFIG")) {
			return envP
		}
	}
	return ""
}

func kubeconfigLabel(kcPath string, getenv func(string) string) string {
	if kcPath != "" {
		return kcPath
	}
	if getenv == nil {
		getenv = os.Getenv
	}
	if env := strings.TrimSpace(getenv("KUBECONFIG")); env != "" {
		return env
	}
	return "(default search)"
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func buildDirectConfig(opts Options) (*rest.Config, error) {
	if opts.BuildDirect != nil {
		return opts.BuildDirect(opts.Kubeconfig, opts.Context, opts.InCluster)
	}
	return kube.BuildConfig(kube.Options{
		Kubeconfig: opts.Kubeconfig,
		Context:    opts.Context,
		InCluster:  opts.InCluster,
	})
}

func defaultClusterName(clusterName, kubectx string) string {
	if strings.TrimSpace(clusterName) != "" {
		return strings.TrimSpace(clusterName)
	}
	if strings.TrimSpace(kubectx) != "" {
		return strings.TrimSpace(kubectx)
	}
	return "default"
}

func listGKEClusters(
	ctx context.Context,
	parent string,
	custom func(ctx context.Context, parent string) ([]*container.Cluster, error),
	broker *Broker,
	ts oauth2.TokenSource,
	googleAuthMode string,
	gkeEndpoint string,
) ([]*container.Cluster, error) {
	if custom != nil {
		return custom(ctx, parent)
	}
	var (
		svc *container.Service
		err error
	)
	switch {
	case broker != nil:
		svc, err = container.NewService(ctx, broker.GoogleAPIOptions(ContainerAPIHost)...)
	case ts != nil || googleAuthMode == GoogleAuthInjected || strings.TrimSpace(gkeEndpoint) != "":
		if ts == nil {
			ts = InjectedTokenSource()
		}
		svc, err = container.NewService(ctx, googleAPIOptionsForTokenSource(ts, gkeEndpoint)...)
	default:
		svc, err = container.NewService(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("GKE API client: %w", err)
	}
	resp, err := svc.Projects.Locations.Clusters.List(parent).Context(ctx).Do()
	if err != nil {
		return nil, err
	}
	return resp.Clusters, nil
}

func parseKubeconfigFrom(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == kubeconfigPrefix {
		return "", true
	}
	if p, ok := strings.CutPrefix(s, kubeconfigPrefix+":"); ok {
		return strings.TrimSpace(p), true
	}
	return "", false
}

func matchesFilter(id Identity, profile string, specs []string) bool {
	full := id.String()
	for _, raw := range specs {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if s == full || s == id.Cluster || (profile != "" && s == profile) {
			return true
		}
		if specID, ok := ParseClusterRef(s); ok && id.complete() && specID == id {
			return true
		}
	}
	return false
}

func matchesKubeconfigFilter(ctxName string, specs []string) bool {
	ctxID, hasCtxID := ParseClusterRef(ctxName)
	for _, raw := range specs {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if s == ctxName {
			return true
		}
		if hasCtxID && s == ctxID.Cluster {
			return true
		}
		if specID, ok := ParseClusterRef(s); ok {
			if ctxName == specID.ContextName() || ctxName == specID.String() {
				return true
			}
			if hasCtxID && ctxID == specID {
				return true
			}
		}
	}
	return false
}

func matchesDirectFilter(clusterName string, specs []string) bool {
	if clusterName == "" {
		return false
	}
	for _, s := range specs {
		t := strings.TrimSpace(s)
		if t == clusterName {
			return true
		}
		if specID, ok := ParseClusterRef(t); ok && specID.Cluster == clusterName {
			return true
		}
	}
	return false
}

func removeProfileCluster(list []Cluster, profile string) []Cluster {
	out := make([]Cluster, 0, len(list))
	for _, c := range list {
		if c.Profile != profile {
			out = append(out, c)
		}
	}
	return out
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
