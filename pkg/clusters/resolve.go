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
	"fmt"
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

// Options configures unified multi-cluster discovery for both `leeway report`
// and `leeway watch`.
type Options struct {
	// Clusters is the comma-separated --clusters value. Items may be:
	//   - "project/location/cluster" (GKE cluster identity)
	//   - "name=https://endpoint" (explicit API server endpoint)
	//   - "<context-name>" (kubeconfig context, or filter when ProfilesDir/ClustersFrom is set)
	Clusters string

	// ClustersFrom is the --clusters-from fleet discovery source:
	//   - "kubeconfig" or "kubeconfig:/path/to/config"
	//   - "profiles:/path/to/profiles"
	//   - "<project>" or "<project>/<location>" (GKE fleet listing)
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

	// Test and runtime injection seams.
	Describe    func(ctx context.Context, id Identity) (*container.Cluster, error)
	ListGKE     func(ctx context.Context, parent string) ([]*container.Cluster, error)
	TokenSource func(ctx context.Context) (oauth2.TokenSource, error)
	Broker      *Broker
	BuildDirect func(kubeconfig, kubectx string, inCluster bool) (*rest.Config, error)
	OnSkip      func(source string, err error)
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
	broker := opts.Broker
	if broker == nil {
		var err error
		broker, err = BrokerFromEnv()
		if err != nil {
			return nil, err
		}
	}

	profilesDir := strings.TrimSpace(opts.ProfilesDir)
	clustersFrom := strings.TrimSpace(opts.ClustersFrom)
	if after, ok := strings.CutPrefix(clustersFrom, profilesPrefix); ok {
		if profilesDir == "" {
			profilesDir = strings.TrimSpace(after)
		}
		clustersFrom = ""
	}

	specs := splitCSV(opts.Clusters)
	disc := Discoverer{
		Describe:    opts.Describe,
		TokenSource: opts.TokenSource,
		Broker:      broker,
		OnSkip:      opts.OnSkip,
	}

	// 1. Hermes --profiles-dir path (with optional --clusters filter and
	// optional direct --in-cluster / --kubeconfig management cluster merge).
	if profilesDir != "" {
		return resolveFromProfilesDir(ctx, profilesDir, specs, opts, disc)
	}

	// 2. --clusters-from fleet discovery (kubeconfig[:path] or GKE project[/location]).
	if clustersFrom != "" {
		return resolveFromSource(ctx, clustersFrom, specs, opts, disc)
	}

	// 3. Explicit --clusters list without a discovery source.
	if len(specs) > 0 {
		return resolveExplicitSpecs(ctx, specs, opts, disc)
	}

	// 4. Single-cluster fallback (--in-cluster, --kubeconfig, --context).
	return resolveSingleCluster(opts, disc.Broker)
}

func resolveFromProfilesDir(ctx context.Context, dir string, filterSpecs []string, opts Options, disc Discoverer) ([]Cluster, error) {
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
		// Re-run without the identity-only pre-filter in case a spec named the profile directory.
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
		directName := defaultClusterName(opts.ClusterName, opts.Context)
		candidates := byName[directName]
		if len(candidates) <= 1 {
			cfg, err := buildDirectConfig(opts)
			if err != nil {
				return nil, err
			}
			direct := Cluster{
				Identity: Identity{
					Cluster:  directName,
					Project:  opts.Project,
					Location: opts.Location,
				},
				Profile: DirectProfile,
				Config:  cfg,
			}
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
			return nil, fmt.Errorf("no clusters in %s matched --clusters=%s", dir, strings.Join(filterSpecs, ","))
		}
		return nil, fmt.Errorf("no clusters to reach: %s contained no Cluster Agent profiles and neither --in-cluster nor --kubeconfig was given", dir)
	}
	return discovered, nil
}

func resolveFromSource(ctx context.Context, from string, filterSpecs []string, opts Options, disc Discoverer) ([]Cluster, error) {
	if kcPath, ok := parseKubeconfigFrom(from); ok {
		if kcPath == "" {
			kcPath = opts.Kubeconfig
		}
		ctxs, err := kube.ListContexts(kcPath)
		if err != nil {
			return nil, err
		}
		var out []Cluster
		for _, c := range ctxs {
			id := Identity{Cluster: c.Name, Project: opts.Project, Location: opts.Location}
			if len(filterSpecs) > 0 && !matchesFilter(id, c.Name, filterSpecs) {
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
		if len(out) == 0 {
			return nil, fmt.Errorf("no clusters resolved from %s", from)
		}
		return out, nil
	}

	// GKE fleet discovery: <project> or <project>/<location>.
	project, location := parseProjectLocation(from)
	if project == "" {
		return nil, fmt.Errorf("--clusters-from=%q must be kubeconfig[:path], profiles:<dir>, or <project>[/<location>]", from)
	}
	loc := location
	if loc == "" {
		loc = "-"
	}
	parent := fmt.Sprintf("projects/%s/locations/%s", project, loc)
	gkeClusters, err := listGKEClusters(ctx, parent, opts.ListGKE, disc.Broker)
	if err != nil {
		return nil, fmt.Errorf("list GKE clusters under %s: %w", parent, err)
	}

	var ts oauth2.TokenSource
	var out []Cluster
	for _, gc := range gkeClusters {
		if gc == nil {
			continue
		}
		id := Identity{
			Project:  project,
			Location: gc.Location,
			Cluster:  gc.Name,
		}
		if len(filterSpecs) > 0 && !matchesFilter(id, "", filterSpecs) {
			continue
		}
		if disc.Broker == nil && ts == nil {
			ts, err = disc.tokenSource(ctx)
			if err != nil {
				if opts.OnSkip != nil {
					opts.OnSkip(id.String(), err)
				}
				continue
			}
		}
		cfg, err := disc.clientConfig(id, gc, ts)
		if err != nil {
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
			c.Context = fmt.Sprintf("gke_%s_%s_%s", id.Project, id.Location, id.Cluster)
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no reachable GKE clusters found under %s", parent)
	}
	return out, nil
}

func resolveExplicitSpecs(ctx context.Context, specs []string, opts Options, disc Discoverer) ([]Cluster, error) {
	var ts oauth2.TokenSource
	var out []Cluster
	seen := make(map[string]bool, len(specs))

	for _, spec := range specs {
		// 1. Explicit name=endpoint pair (e.g. "prod-a=https://10.0.0.1:6443" or "prod-a=https://gke-...").
		if name, endpoint, ok := strings.Cut(spec, "="); ok {
			name = strings.TrimSpace(name)
			endpoint = strings.TrimSpace(endpoint)
			if name == "" || endpoint == "" {
				return nil, fmt.Errorf("invalid --clusters entry %q: expected name=endpoint", spec)
			}
			host := endpoint
			if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
				host = "https://" + host
			}
			cfg := &rest.Config{Host: host}
			// Attach Google token source if available, ignoring error for plain local/test endpoints.
			if disc.TokenSource != nil {
				if tok, err := disc.TokenSource(ctx); err == nil && tok != nil {
					UseGoogleTokenSource(cfg, tok)
				}
			}
			id := Identity{Cluster: name, Project: opts.Project, Location: opts.Location}
			key := "endpoint:" + name
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, Cluster{
				Identity: id,
				Profile:  key,
				Config:   cfg,
			})
			continue
		}

		// 2. GKE triple: "project/location/cluster".
		if parts := strings.Split(spec, "/"); len(parts) == 3 && parts[0] != "" && parts[1] != "" && parts[2] != "" {
			id := Identity{Project: parts[0], Location: parts[1], Cluster: parts[2]}
			if seen[id.String()] {
				continue
			}
			seen[id.String()] = true

			if disc.Broker != nil && disc.Broker.ShellMode && disc.Describe == nil {
				out = append(out, Cluster{
					Identity: id,
					Profile:  "gke:" + id.String(),
					Context:  fmt.Sprintf("gke_%s_%s_%s", id.Project, id.Location, id.Cluster),
				})
				continue
			}

			if disc.Broker == nil && ts == nil {
				var err error
				ts, err = disc.tokenSource(ctx)
				if err != nil {
					if opts.OnSkip != nil {
						opts.OnSkip(id.String(), err)
						continue
					}
					return nil, fmt.Errorf("credentials for %s: %w", id, err)
				}
			}
			described, err := disc.describe(ctx, id)
			if err != nil {
				if opts.OnSkip != nil {
					opts.OnSkip(id.String(), err)
					continue
				}
				return nil, fmt.Errorf("describe GKE cluster %s: %w", id, err)
			}
			cfg, err := disc.clientConfig(id, described, ts)
			if err != nil {
				if opts.OnSkip != nil {
					opts.OnSkip(id.String(), err)
					continue
				}
				return nil, fmt.Errorf("client config for %s: %w", id, err)
			}
			out = append(out, Cluster{
				Identity:  id,
				Profile:   "gke:" + id.String(),
				Config:    cfg,
				Autopilot: described.Autopilot != nil && described.Autopilot.Enabled,
			})
			continue
		}

		// 3. Kubeconfig context name (or gke_<project>_<location>_<cluster> under ShellMode).
		if seen["ctx:"+spec] {
			continue
		}
		seen["ctx:"+spec] = true
		if disc.Broker != nil && disc.Broker.ShellMode {
			id := Identity{Cluster: spec, Project: opts.Project, Location: opts.Location}
			if gkeID, ok := parseGKEContext(spec); ok {
				id = gkeID
			}
			out = append(out, Cluster{
				Identity:   id,
				Profile:    "kubeconfig:" + spec,
				Kubeconfig: opts.Kubeconfig,
				Context:    spec,
			})
			continue
		}
		cfg, err := kube.ConfigForContext(opts.Kubeconfig, spec)
		if err != nil {
			if opts.OnSkip != nil {
				opts.OnSkip(spec, err)
				continue
			}
			return nil, err
		}
		out = append(out, Cluster{
			Identity: Identity{
				Cluster:  spec,
				Project:  opts.Project,
				Location: opts.Location,
			},
			Profile: "kubeconfig:" + spec,
			Config:  cfg,
		})
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("no clusters could be resolved from --clusters=%s", strings.Join(specs, ","))
	}
	return out, nil
}

func resolveSingleCluster(opts Options, broker *Broker) ([]Cluster, error) {
	if broker != nil && broker.ShellMode && opts.BuildDirect == nil && !opts.InCluster {
		id := Identity{
			Cluster:  defaultClusterName(opts.ClusterName, opts.Context),
			Project:  opts.Project,
			Location: opts.Location,
		}
		if gkeID, ok := parseGKEContext(opts.Context); ok {
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
	cfg, err := buildDirectConfig(opts)
	if err != nil {
		return nil, err
	}
	name := defaultClusterName(opts.ClusterName, opts.Context)
	return []Cluster{{
		Identity: Identity{
			Cluster:  name,
			Project:  opts.Project,
			Location: opts.Location,
		},
		Profile: DirectProfile,
		Config:  cfg,
	}}, nil
}

func parseGKEContext(s string) (Identity, bool) {
	parts := strings.Split(strings.TrimSpace(s), "_")
	if len(parts) != 4 || parts[0] != "gke" || parts[1] == "" || parts[2] == "" || parts[3] == "" {
		return Identity{}, false
	}
	return Identity{
		Project:  parts[1],
		Location: parts[2],
		Cluster:  parts[3],
	}, true
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

func listGKEClusters(ctx context.Context, parent string, custom func(ctx context.Context, parent string) ([]*container.Cluster, error), broker *Broker) ([]*container.Cluster, error) {
	if custom != nil {
		return custom(ctx, parent)
	}
	var (
		svc *container.Service
		err error
	)
	if broker != nil {
		svc, err = container.NewService(ctx, broker.GoogleAPIOptions(ContainerAPIHost)...)
	} else {
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

func parseProjectLocation(s string) (project, location string) {
	s = strings.TrimSpace(s)
	if s == "" || strings.Contains(s, ":") {
		return "", ""
	}
	parts := strings.Split(s, "/")
	switch len(parts) {
	case 1:
		return parts[0], ""
	case 2:
		return parts[0], parts[1]
	default:
		return "", ""
	}
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
	}
	return false
}

func matchesDirectFilter(clusterName string, specs []string) bool {
	if clusterName == "" {
		return false
	}
	for _, s := range specs {
		if strings.TrimSpace(s) == clusterName {
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
