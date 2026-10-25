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
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	dynfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/go-steer/leeway/pkg/clusters"
	"github.com/go-steer/leeway/pkg/leeway"
	"github.com/go-steer/leeway/pkg/sources/computeclass"
)

func TestCollect_MultiCluster(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

	// Cluster 1: Skewed Deployment (all 6 pods in us-east1-b despite 3 ready zones
	// and a DoNotSchedule maxSkew=1 constraint) + 1 unschedulable pod due to
	// insufficient CPU, plus a ComputeClass with all pods on its last rank.
	skewedClient, skewedDyn := buildSkewedCluster()

	// Cluster 2: Balanced Deployment (2 pods per zone across 3 zones).
	balancedClient := buildBalancedCluster()

	targets := []clusters.Cluster{
		{Identity: clusters.Identity{Cluster: "gke-prod-east"}, Profile: "flag"},
		{Identity: clusters.Identity{Cluster: "gke-prod-west"}, Profile: "flag"},
		{Identity: clusters.Identity{Cluster: "gke-unreachable"}, Profile: "flag"},
	}

	rep, err := Collect(context.Background(), targets, Options{
		Now:     now,
		All:     true,
		Timeout: 10 * time.Second,
		ClientBuilder: func(c clusters.Cluster) (kubernetes.Interface, dynamic.Interface, error) {
			switch c.Name() {
			case "gke-prod-east":
				return skewedClient, skewedDyn, nil
			case "gke-prod-west":
				return balancedClient, nil, nil
			default:
				return nil, nil, errors.New("connection refused")
			}
		},
	})
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if rep.SchemaVersion != SchemaVersion {
		t.Errorf("SchemaVersion = %q, want %q", rep.SchemaVersion, SchemaVersion)
	}
	if rep.Summary.ClustersTotal != 3 {
		t.Errorf("ClustersTotal = %d, want 3", rep.Summary.ClustersTotal)
	}
	if rep.Summary.ClustersSucceeded != 2 {
		t.Errorf("ClustersSucceeded = %d, want 2", rep.Summary.ClustersSucceeded)
	}
	if rep.Summary.ClustersFailed != 1 {
		t.Errorf("ClustersFailed = %d, want 1", rep.Summary.ClustersFailed)
	}

	// Verify Cluster 1 (gke-prod-east)
	east := rep.Clusters[0]
	if east.Status != StatusOK {
		t.Fatalf("east.Status = %q (%s), want %q", east.Status, east.Error, StatusOK)
	}
	if len(east.Domains[corev1.LabelTopologyZone]) != 3 {
		t.Errorf("east zones = %d, want 3", len(east.Domains[corev1.LabelTopologyZone]))
	}
	if len(east.TopologyFindings) == 0 {
		t.Fatal("expected at least 1 topology finding in gke-prod-east, got 0")
	}
	f := east.TopologyFindings[0]
	if f.Kind != leeway.KindContractViolated {
		t.Errorf("finding.Kind = %q, want %q", f.Kind, leeway.KindContractViolated)
	}
	if f.SuspectedCause != leeway.CauseCapacityShortfall {
		t.Errorf("finding.SuspectedCause = %q, want %q", f.SuspectedCause, leeway.CauseCapacityShortfall)
	}
	if len(east.ComputeClasses) != 1 {
		t.Fatalf("east.ComputeClasses = %d, want 1", len(east.ComputeClasses))
	}
	if len(east.RankFindings) == 0 {
		t.Fatal("expected at least 1 rank finding in gke-prod-east, got 0")
	}

	// Verify Cluster 2 (gke-prod-west) has 0 findings but includes evaluated Subjects (--all)
	west := rep.Clusters[1]
	if west.Status != StatusOK {
		t.Fatalf("west.Status = %q (%s), want %q", west.Status, west.Error, StatusOK)
	}
	if len(west.TopologyFindings) != 0 {
		t.Errorf("west.TopologyFindings = %d, want 0", len(west.TopologyFindings))
	}
	if len(west.Subjects) == 0 {
		t.Error("expected west.Subjects to be populated when All=true")
	}

	// Verify Cluster 3 (gke-unreachable)
	dead := rep.Clusters[2]
	if dead.Status != StatusError {
		t.Errorf("dead.Status = %q, want %q", dead.Status, StatusError)
	}

	// Verify JSON serialization round-trips cleanly
	raw, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var decoded Report
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if decoded.Summary.TopologyFindings != rep.Summary.TopologyFindings {
		t.Errorf("round-trip TopologyFindings = %d, want %d", decoded.Summary.TopologyFindings, rep.Summary.TopologyFindings)
	}
}

// TestCollect_CauseAttribution exercises one two-zone cluster holding four
// workloads, each skewed by a different mechanism, and checks that the
// snapshot pipeline attributes each one to the right SuspectedCause and
// RemediationKind:
//
//   - affinity-pinned-api: a zone topologySpreadConstraint contradicted by a
//     required nodeAffinity on the same key → constraint_ignored, manifest_patch.
//   - zonal-pv-store: StatefulSet replicas held in one zone by zonal
//     PersistentVolumes → volume_pinning, storage_topology.
//   - cpu-starved-worker: a DoNotSchedule constraint whose remaining replicas
//     are Pending on Insufficient cpu → domain_capacity_shortfall, cluster_capacity.
//   - pool-pinned-filler: a nodeSelector onto a single-zone node pool, which
//     is honoured as intent and must produce no finding at all.
func TestCollect_CauseAttribution(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	client := buildAttributionCluster()

	targets := []clusters.Cluster{
		{Identity: clusters.Identity{Cluster: "gke-two-zone"}, Profile: "flag"},
	}

	rep, err := Collect(context.Background(), targets, Options{
		Now:     now,
		Timeout: 10 * time.Second,
		ClientBuilder: func(c clusters.Cluster) (kubernetes.Interface, dynamic.Interface, error) {
			return client, nil, nil
		},
	})
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(rep.Clusters) != 1 || rep.Clusters[0].Status != StatusOK {
		t.Fatalf("Clusters = %+v, want 1 OK cluster", rep.Clusters)
	}

	byWorkload := make(map[string]leeway.Finding)
	for _, f := range rep.Clusters[0].TopologyFindings {
		if f.TopologyKey == corev1.LabelTopologyZone {
			byWorkload[f.Subject.Name] = f
		}
	}

	if _, bad := byWorkload["pool-pinned-filler"]; bad {
		t.Errorf("pool-pinned-filler should not be flagged as topology drift (intentionally pinned to default-pool)")
	}

	// 1. affinity-pinned-api: required nodeAffinity conflicts with the zone spread constraint.
	apiFinding, ok := byWorkload["affinity-pinned-api"]
	if !ok {
		t.Fatalf("expected topology finding for affinity-pinned-api, got findings: %+v", byWorkload)
	}
	if apiFinding.SuspectedCause != leeway.CauseConstraintIgnored {
		t.Errorf("affinity-pinned-api SuspectedCause = %q, want %q", apiFinding.SuspectedCause, leeway.CauseConstraintIgnored)
	}
	if apiFinding.Remediation == nil || apiFinding.Remediation.Kind != leeway.RemediationManifestPatch {
		t.Errorf("affinity-pinned-api Remediation = %+v, want %q", apiFinding.Remediation, leeway.RemediationManifestPatch)
	}

	// 2. zonal-pv-store: replicas pinned to one zone by zonal PersistentVolumes.
	storeFinding, ok := byWorkload["zonal-pv-store"]
	if !ok {
		t.Fatalf("expected topology finding for zonal-pv-store, got findings: %+v", byWorkload)
	}
	if storeFinding.SuspectedCause != leeway.CauseVolumePinning {
		t.Errorf("zonal-pv-store SuspectedCause = %q, want %q", storeFinding.SuspectedCause, leeway.CauseVolumePinning)
	}
	if storeFinding.Remediation == nil || storeFinding.Remediation.Kind != leeway.RemediationStorageTopology {
		t.Errorf("zonal-pv-store Remediation = %+v, want %q", storeFinding.Remediation, leeway.RemediationStorageTopology)
	}

	// 3. cpu-starved-worker: DoNotSchedule constraint with replicas Pending on Insufficient cpu.
	workerFinding, ok := byWorkload["cpu-starved-worker"]
	if !ok {
		t.Fatalf("expected topology finding for cpu-starved-worker, got findings: %+v", byWorkload)
	}
	if workerFinding.SuspectedCause != leeway.CauseCapacityShortfall {
		t.Errorf("cpu-starved-worker SuspectedCause = %q, want %q", workerFinding.SuspectedCause, leeway.CauseCapacityShortfall)
	}
	if workerFinding.Remediation == nil || workerFinding.Remediation.Kind != leeway.RemediationClusterCapacity {
		t.Errorf("cpu-starved-worker Remediation = %+v, want %q", workerFinding.Remediation, leeway.RemediationClusterCapacity)
	}
}

func buildAttributionCluster() kubernetes.Interface {
	nodeA := makeReadyNode("gke-two-zone-default-pool-1", "us-central1-a", "us-central1", "", "", "e2-small")
	nodeA.Labels["cloud.google.com/gke-nodepool"] = "default-pool"
	nodeA.Labels["topology.gke.io/zone"] = "us-central1-a"
	nodeA.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("940m")

	nodeB := makeReadyNode("gke-two-zone-second-zone-pool-1", "us-central1-b", "us-central1", "", "", "e2-standard-2")
	nodeB.Labels["cloud.google.com/gke-nodepool"] = "second-zone-pool"
	nodeB.Labels["topology.gke.io/zone"] = "us-central1-b"
	nodeB.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("1930m")

	isController := true
	ns := "topology"

	fillerRS := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pool-pinned-filler-rs",
			Namespace: ns,
			UID:       types.UID("rs-filler"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "pool-pinned-filler",
				UID:        types.UID("dep-filler"),
				Controller: &isController,
			}},
		},
	}
	apiRS := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "affinity-pinned-api-rs",
			Namespace: ns,
			UID:       types.UID("rs-api"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "affinity-pinned-api",
				UID:        types.UID("dep-api"),
				Controller: &isController,
			}},
		},
	}
	workerRS := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cpu-starved-worker-rs",
			Namespace: ns,
			UID:       types.UID("rs-worker"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "cpu-starved-worker",
				UID:        types.UID("dep-worker"),
				Controller: &isController,
			}},
		},
	}

	var objs []runtime.Object
	objs = append(objs, nodeA, nodeB, fillerRS, apiRS, workerRS)

	// 1. pool-pinned-filler: 6 pods in us-central1-a via nodeSelector on gke-nodepool
	for i := 0; i < 6; i++ {
		objs = append(objs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pool-pinned-filler-" + string(rune('a'+i)),
				Namespace: ns,
				UID:       types.UID("filler-" + string(rune('a'+i))),
				Labels:    map[string]string{"app": "pool-pinned-filler"},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "pool-pinned-filler-rs",
					UID:        types.UID("rs-filler"),
					Controller: &isController,
				}},
			},
			Spec: corev1.PodSpec{
				NodeName:     nodeA.Name,
				NodeSelector: map[string]string{"cloud.google.com/gke-nodepool": "default-pool"},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		})
	}

	// 2. affinity-pinned-api: 2 pods in us-central1-a with conflicting zone nodeAffinity + zone TSC
	for i := 0; i < 2; i++ {
		objs = append(objs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "affinity-pinned-api-" + string(rune('a'+i)),
				Namespace: ns,
				UID:       types.UID("api-" + string(rune('a'+i))),
				Labels:    map[string]string{"app": "affinity-pinned-api"},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "affinity-pinned-api-rs",
					UID:        types.UID("rs-api"),
					Controller: &isController,
				}},
			},
			Spec: corev1.PodSpec{
				NodeName: nodeA.Name,
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{{
								MatchExpressions: []corev1.NodeSelectorRequirement{{
									Key:      corev1.LabelTopologyZone,
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"us-central1-a"},
								}},
							}},
						},
					},
				},
				TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
					MaxSkew:           1,
					TopologyKey:       corev1.LabelTopologyZone,
					WhenUnsatisfiable: corev1.ScheduleAnyway,
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "affinity-pinned-api"},
					},
				}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		})
	}

	// 3. zonal-pv-store: StatefulSet with 2 pods bound to GKE PD CSI zonal PVs (topology.gke.io/zone)
	for i := 0; i < 2; i++ {
		suffix := string(rune('0' + i))
		pvcName := "data-zonal-pv-store-" + suffix
		pvName := "pv-zonal-pv-store-" + suffix
		objs = append(objs,
			&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: ns},
				Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: pvName},
			},
			&corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: pvName},
				Spec: corev1.PersistentVolumeSpec{
					NodeAffinity: &corev1.VolumeNodeAffinity{
						Required: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{{
								MatchExpressions: []corev1.NodeSelectorRequirement{{
									Key:      "topology.gke.io/zone",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{"us-central1-a"},
								}},
							}},
						},
					},
				},
			},
			&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "zonal-pv-store-" + suffix,
					Namespace: ns,
					UID:       types.UID("store-" + suffix),
					Labels:    map[string]string{"app": "zonal-pv-store"},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "apps/v1",
						Kind:       "StatefulSet",
						Name:       "zonal-pv-store",
						UID:        types.UID("sts-store"),
						Controller: &isController,
					}},
				},
				Spec: corev1.PodSpec{
					NodeName: nodeA.Name,
					Volumes: []corev1.Volume{{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
						},
					}},
					TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
						MaxSkew:           1,
						TopologyKey:       corev1.LabelHostname,
						WhenUnsatisfiable: corev1.ScheduleAnyway,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "zonal-pv-store"},
						},
					}},
				},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			},
		)
	}

	// 4. cpu-starved-worker: 1 Running pod in us-central1-b + 3 Pending Unschedulable pods
	objs = append(objs, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cpu-starved-worker-running",
			Namespace: ns,
			UID:       types.UID("worker-run-0"),
			Labels:    map[string]string{"app": "cpu-starved-worker"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       "cpu-starved-worker-rs",
				UID:        types.UID("rs-worker"),
				Controller: &isController,
			}},
		},
		Spec: corev1.PodSpec{
			NodeName: nodeB.Name,
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
				MaxSkew:           1,
				TopologyKey:       corev1.LabelTopologyZone,
				WhenUnsatisfiable: corev1.DoNotSchedule,
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "cpu-starved-worker"},
				},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	})
	for i := 0; i < 3; i++ {
		objs = append(objs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cpu-starved-worker-pending-" + string(rune('a'+i)),
				Namespace: ns,
				UID:       types.UID("worker-pend-" + string(rune('a'+i))),
				Labels:    map[string]string{"app": "cpu-starved-worker"},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "cpu-starved-worker-rs",
					UID:        types.UID("rs-worker"),
					Controller: &isController,
				}},
			},
			Spec: corev1.PodSpec{
				TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
					MaxSkew:           1,
					TopologyKey:       corev1.LabelTopologyZone,
					WhenUnsatisfiable: corev1.DoNotSchedule,
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "cpu-starved-worker"},
					},
				}},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				Conditions: []corev1.PodCondition{{
					Type:    corev1.PodScheduled,
					Status:  corev1.ConditionFalse,
					Reason:  corev1.PodReasonUnschedulable,
					Message: "0/2 nodes are available: 1 Insufficient cpu, 1 node(s) didn't match pod topology spread constraints.",
				}},
			},
		})
	}

	return kubefake.NewSimpleClientset(objs...)
}

func buildSkewedCluster() (kubernetes.Interface, dynamic.Interface) {
	nodes := []runtime.Object{
		makeReadyNode("node-a1", "us-east1-a", "us-east1", "gold", "0", "n2-standard-4"),
		makeReadyNode("node-a2", "us-east1-a", "us-east1", "gold", "0", "n2-standard-4"),
		makeReadyNode("node-b1", "us-east1-b", "us-east1", "gold", "1", "e2-standard-4"),
		makeReadyNode("node-b2", "us-east1-b", "us-east1", "gold", "1", "e2-standard-4"),
		makeReadyNode("node-c1", "us-east1-c", "us-east1", "gold", "0", "n2-standard-4"),
		makeReadyNode("node-c2", "us-east1-c", "us-east1", "gold", "0", "n2-standard-4"),
	}

	isController := true
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "checkout-rs",
			Namespace: "prod",
			UID:       types.UID("rs-1"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "checkout",
				UID:        types.UID("dep-1"),
				Controller: &isController,
			}},
		},
	}

	var pods []runtime.Object
	for i := 0; i < 6; i++ {
		nodeName := "node-b1"
		if i%2 == 1 {
			nodeName = "node-b2"
		}
		pods = append(pods, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "checkout-pod-" + string(rune('a'+i)),
				Namespace: "prod",
				UID:       types.UID("pod-" + string(rune('a'+i))),
				Labels:    map[string]string{"app": "checkout"},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "checkout-rs",
					UID:        types.UID("rs-1"),
					Controller: &isController,
				}},
			},
			Spec: corev1.PodSpec{
				NodeName: nodeName,
				NodeSelector: map[string]string{
					"cloud.google.com/compute-class": "gold",
				},
				TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
					MaxSkew:           1,
					TopologyKey:       corev1.LabelTopologyZone,
					WhenUnsatisfiable: corev1.DoNotSchedule,
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "checkout"},
					},
				}},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
			},
		})
	}

	// Add 1 pending unschedulable pod citing Insufficient cpu
	pods = append(pods, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "checkout-pod-pending",
			Namespace: "prod",
			UID:       types.UID("pod-pending-1"),
			Labels:    map[string]string{"app": "checkout"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       "checkout-rs",
				UID:        types.UID("rs-1"),
				Controller: &isController,
			}},
		},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{
				"cloud.google.com/compute-class": "gold",
			},
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
				MaxSkew:           1,
				TopologyKey:       corev1.LabelTopologyZone,
				WhenUnsatisfiable: corev1.DoNotSchedule,
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": "checkout"},
				},
			}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type:    corev1.PodScheduled,
				Status:  corev1.ConditionFalse,
				Reason:  corev1.PodReasonUnschedulable,
				Message: "0/6 nodes are available: 2 Insufficient cpu, 4 node(s) didn't match pod topology spread constraints.",
			}},
		},
	})

	var objs []runtime.Object
	objs = append(objs, nodes...)
	objs = append(objs, rs)
	objs = append(objs, pods...)
	client := kubefake.NewSimpleClientset(objs...)
	client.Resources = []*metav1.APIResourceList{{
		GroupVersion: "cloud.google.com/v1",
		APIResources: []metav1.APIResource{{
			Name:       "computeclasses",
			Kind:       "ComputeClass",
			Namespaced: false,
		}},
	}}

	cc := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "cloud.google.com/v1",
			"kind":       "ComputeClass",
			"metadata": map[string]any{
				"name": "gold",
			},
			"spec": map[string]any{
				"priorities": []any{
					map[string]any{"machineFamily": "n2"},
					map[string]any{"machineFamily": "e2"},
				},
			},
		},
	}
	scheme := runtime.NewScheme()
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		computeclass.ComputeClassGVR(): "ComputeClassList",
	}, cc)

	return client, dyn
}

func buildBalancedCluster() kubernetes.Interface {
	nodes := []runtime.Object{
		makeReadyNode("node-a", "us-west1-a", "us-west1", "", "", "n2-standard-4"),
		makeReadyNode("node-b", "us-west1-b", "us-west1", "", "", "n2-standard-4"),
		makeReadyNode("node-c", "us-west1-c", "us-west1", "", "", "n2-standard-4"),
	}
	isController := true
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "frontend-rs",
			Namespace: "default",
			UID:       types.UID("rs-west"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "frontend",
				UID:        types.UID("dep-west"),
				Controller: &isController,
			}},
		},
	}

	zones := []string{"node-a", "node-b", "node-c", "node-a", "node-b", "node-c"}
	var pods []runtime.Object
	for i, n := range zones {
		pods = append(pods, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "frontend-" + string(rune('a'+i)),
				Namespace: "default",
				UID:       types.UID("west-pod-" + string(rune('a'+i))),
				Labels:    map[string]string{"app": "frontend"},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "frontend-rs",
					UID:        types.UID("rs-west"),
					Controller: &isController,
				}},
			},
			Spec: corev1.PodSpec{
				NodeName: n,
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
			},
		})
	}
	var objs []runtime.Object
	objs = append(objs, nodes...)
	objs = append(objs, rs)
	objs = append(objs, pods...)
	return kubefake.NewSimpleClientset(objs...)
}

func makeReadyNode(name, zone, region, cc, priorityIdx, instanceType string) *corev1.Node {
	labels := map[string]string{
		corev1.LabelTopologyZone:   zone,
		corev1.LabelTopologyRegion: region,
		corev1.LabelHostname:       name,
	}
	if cc != "" {
		labels["cloud.google.com/compute-class"] = cc
	}
	if instanceType != "" {
		labels[corev1.LabelInstanceTypeStable] = instanceType
	}
	var annotations map[string]string
	if priorityIdx != "" {
		annotations = map[string]string{"ccc_priority_index": priorityIdx}
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      labels,
			Annotations: annotations,
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{
				Type:   corev1.NodeReady,
				Status: corev1.ConditionTrue,
			}},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("16Gi"),
			},
		},
	}
}
