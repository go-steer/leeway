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

// Package clusterprofiles turns a Hermes profiles directory into a set of
// addressable GKE clusters.
//
// The Platform Agent creates one Cluster Agent profile per onboarded cluster
// and removes it on teardown (agents/platform/scripts/cluster_agent_profile.py),
// so that directory is the inventory of clusters a fleet-wide process should be
// reaching, and each profile records which cluster it is scoped to. This package
// reads that inventory, asks the GKE API where each cluster's control plane is,
// and returns a rest.Config per cluster with this process's own Google
// credential attached.
//
// It builds no Kubernetes client. The caller picks the client type, and this
// package stops at the configuration every client type is built from. The
// event watcher builds an informer-backed kubernetes.Interface from it and the
// drift detector a dynamic.Interface (cmd/drift-detector/cluster.go); the
// detector's scope also lists the identities alone, through ReadIdentities,
// without addressing anything.
package clusters

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"sigs.k8s.io/yaml"
)

// profileConfigFile is the file inside a profile directory that carries the
// cluster_identity block. Named here because it is the whole contract between
// this package and the Python side that writes the profiles.
const profileConfigFile = "config.yaml"

// ProfileConfigFile is profileConfigFile for a caller that needs to look at
// the file itself, as the drift detector's scope does to word a drop the
// listing does not report.
const ProfileConfigFile = profileConfigFile

// Identity is the cluster_identity block the Platform Agent writes into each
// Cluster Agent profile's config.yaml. sigs.k8s.io/yaml converts YAML to JSON,
// hence the json tags.
type Identity struct {
	Project  string `json:"project"`
	Cluster  string `json:"cluster"`
	Location string `json:"location"`
}

// String is what makes one cluster distinct from every other. A GKE cluster
// name is unique only within a project and location, so a fleet can
// legitimately run "prod" in us-central1 and "prod" in europe-west1 — the
// Platform Agent creates a profile for each, keyed on the full triple. Keying
// on the bare name would treat the second as a duplicate of the first and
// silently leave it unreached.
func (i Identity) String() string {
	return i.Project + "/" + i.Location + "/" + i.Cluster
}

// ContextName returns the gcloud kubeconfig context name
// "gke_<project>_<location>_<cluster>".
func (i Identity) ContextName() string {
	return "gke_" + i.Project + "_" + i.Location + "_" + i.Cluster
}

// ParseClusterRef parses a GKE cluster resource path
// ("projects/<p>/locations/<l>/clusters/<c>" or "projects/<p>/zones/<l>/clusters/<c>"),
// a canonical triple ("<p>/<l>/<c>"), or a gcloud kubeconfig context name
// ("gke_<p>_<l>_<c>").
func ParseClusterRef(raw string) (Identity, bool) {
	s := strings.TrimSpace(raw)
	if s == "" || strings.Contains(s, "=") {
		return Identity{}, false
	}
	for _, badPrefix := range []string{"kubeconfig:", "profiles:", "http:", "https:"} {
		if strings.HasPrefix(s, badPrefix) {
			return Identity{}, false
		}
	}
	if strings.HasPrefix(s, "projects/") {
		parts := strings.Split(s, "/")
		if len(parts) == 6 && parts[0] == "projects" && (parts[2] == "locations" || parts[2] == "zones") && parts[4] == "clusters" {
			id := Identity{Project: parts[1], Location: parts[3], Cluster: parts[5]}
			if id.brokerSafe() == nil {
				return id, true
			}
		}
		return Identity{}, false
	}
	if strings.HasPrefix(s, "gke_") {
		parts := strings.SplitN(s, "_", 4)
		if len(parts) == 4 {
			id := Identity{Project: parts[1], Location: parts[2], Cluster: parts[3]}
			if id.brokerSafe() == nil {
				return id, true
			}
		}
		return Identity{}, false
	}
	if parts := strings.Split(s, "/"); len(parts) == 3 {
		id := Identity{Project: parts[0], Location: parts[1], Cluster: parts[2]}
		if id.brokerSafe() == nil {
			return id, true
		}
	}
	return Identity{}, false
}

// complete reports whether all three parts are present. To ReadIdentity an
// incomplete block is "not a cluster profile" rather than a broken one,
// matching what cluster_agent_profile.read_cluster_identity treats as absent;
// ReadIdentities tells a block with some parts from one with none, and reports
// the first, because for the scope it is a cluster profile that names nothing.
func (i Identity) complete() bool {
	return i.Cluster != "" && i.Project != "" && i.Location != ""
}

// profileConfig is the subset of a profile's config.yaml that we read.
type profileConfig struct {
	ClusterIdentity Identity `json:"cluster_identity"`
}

// readProfileConfig reads a profile's config.yaml. Returns nil (not an error)
// when the file is absent; a file that cannot be read or parsed is an error.
func readProfileConfig(path string) (*profileConfig, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- Path to profile config file supplied via flag / discovery
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var cfg profileConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &cfg, nil
}

// ReadIdentity parses the cluster_identity block out of a profile's
// config.yaml. Returns nil (not an error) when the file is absent or the block
// is missing or incomplete — that means "not a cluster profile". A config.yaml
// that exists but cannot be parsed is a real error.
func ReadIdentity(path string) (*Identity, error) {
	cfg, err := readProfileConfig(path)
	if err != nil || cfg == nil {
		return nil, err
	}
	id := cfg.ClusterIdentity
	if !id.complete() {
		return nil, nil
	}
	return &id, nil
}
