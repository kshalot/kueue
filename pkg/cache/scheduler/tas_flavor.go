/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"slices"
	"sync"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/resources"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
	"sigs.k8s.io/kueue/pkg/workload"

	"context"
	"k8s.io/apimachinery/pkg/runtime"
	framework "k8s.io/kube-scheduler/framework"
	schedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	cache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/tainttoleration"
	fwkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	schedulerMetrics "k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/profile"
	"sigs.k8s.io/scheduler-library/pkg/snapshot"
)

// usageOp indicates whether we should add or subtract the usage.
type usageOp int

const (
	// add usage to the cache
	add usageOp = iota
	// subtract usage from the cache
	subtract
)

var importMetricsOnce sync.Once

func (u usageOp) asSignedOne() int {
	if u == add {
		return 1
	}
	return -1
}

type flavorInformation struct {
	// Name indicates the name of the topology specified in the
	// ResourceFlavor spec.topologyName field.
	TopologyName kueue.TopologyReference

	// nodeLabels is a map of nodeLabels defined in the ResourceFlavor object.
	NodeLabels map[string]string
	// tolerations represents the list of tolerations specified for the resource
	// flavor
	Tolerations []corev1.Toleration
}

type topologyInformation struct {
	// levels is a list of levels defined in the Topology object referenced
	// by the flavor corresponding to the cache.
	Levels []string
}

type TASFlavorCache struct {
	sync.RWMutex

	client client.Client

	// topology represents the part of the Topology specification, e.g. the list
	// of topology levels, relevant for TAS-scheduling.
	topology topologyInformation

	// flavor represents the part of the ResourceFlavor specification, e.g. the
	// list of node labels and tolerations, relevant for TAS-scheduling.
	flavor flavorInformation

	// usage maintains the usage per topology domain
	usage map[utiltas.TopologyDomainID]resources.Requests

	// wlUsage tracks the usage coming from workloads, so that we can make the
	// usage removal indempotent - skip if it was not added.
	wlUsage map[workload.Reference][]workload.TopologyDomainRequests

	// nonTasUsageCache maintains the usage coming from non-TAS pods,
	// e.g. static Pods or DaemonSet pods.
	nonTasUsageCache *nonTasUsageCache
}

func (t *tasCache) NewTASFlavorCache(topologyInfo topologyInformation,
	flavorInfo flavorInformation) *TASFlavorCache {
	return &TASFlavorCache{
		client:           t.client,
		topology:         topologyInfo,
		flavor:           flavorInfo,
		usage:            make(map[utiltas.TopologyDomainID]resources.Requests),
		wlUsage:          make(map[workload.Reference][]workload.TopologyDomainRequests),
		nonTasUsageCache: t.nonTasUsageCache,
	}
}

func (c *TASFlavorCache) NodeLabels() map[string]string {
	return c.flavor.NodeLabels
}

func (c *TASFlavorCache) Topology() kueue.TopologyReference {
	return c.flavor.TopologyName
}

func (c *TASFlavorCache) TopologyLevels() []string {
	return c.topology.Levels
}

func (c *TASFlavorCache) snapshot(log logr.Logger, nodes []*nodeInfo) *TASFlavorSnapshot {
	c.RLock()
	defer c.RUnlock()
	log.V(3).Info("Constructing TAS snapshot", "nodeLabels", c.flavor.NodeLabels,
		"levels", c.topology.Levels, "nodeCount", len(nodes))

	var wasSnapshot *snapshot.ClusterSnapshot
	if features.Enabled(features.WorkloadAwareScheduler) {
		importMetricsOnce.Do(func() {
			schedulerMetrics.Register()
		})
		var v1Nodes []*corev1.Node
		for _, n := range nodes {
			v1Nodes = append(v1Nodes, n.toNode())
		}
		upstreamCache := cache.NewSnapshot(nil, v1Nodes)

		registry := fwkruntime.Registry{
			tainttoleration.Name: func(ctx context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
				return tainttoleration.New(ctx, obj, handle, feature.Features{})
			},
			nodeaffinity.Name: func(ctx context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
				var args runtime.Object
				if obj != nil {
					args = obj
				} else {
					args = &schedulerconfig.NodeAffinityArgs{}
				}
				return nodeaffinity.New(ctx, args, handle, feature.Features{})
			},
			queuesort.Name: func(ctx context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
				return queuesort.New(ctx, obj, handle)
			},
			defaultbinder.Name: func(ctx context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
				return defaultbinder.New(ctx, obj, handle)
			},
		}

		cfg := &schedulerconfig.KubeSchedulerProfile{
			SchedulerName: "default-scheduler",
			Plugins: &schedulerconfig.Plugins{
				QueueSort: schedulerconfig.PluginSet{
					Enabled: []schedulerconfig.Plugin{{Name: queuesort.Name}},
				},
				Bind: schedulerconfig.PluginSet{
					Enabled: []schedulerconfig.Plugin{{Name: defaultbinder.Name}},
				},
				Filter: schedulerconfig.PluginSet{
					Enabled: []schedulerconfig.Plugin{
						{Name: tainttoleration.Name},
						{Name: nodeaffinity.Name},
					},
				},
				PreFilter: schedulerconfig.PluginSet{
					Enabled: []schedulerconfig.Plugin{
						{Name: nodeaffinity.Name},
					},
				},
			},
		}

		fwk, err := fwkruntime.NewFramework(context.Background(), registry, cfg, fwkruntime.WithSnapshotSharedLister(upstreamCache))
		if err != nil {
			log.Error(err, "Failed to initialize scheduler framework for TAS WAS integration")
		} else {
			pm := profile.Map{
				"default-scheduler": fwk,
			}
			wasSnapshot = snapshot.NewClusterSnapshot(upstreamCache, pm)
		}
	}

	snap := newTASFlavorSnapshot(log, c.flavor.TopologyName, c.topology.Levels, c.flavor.Tolerations, wasSnapshot)
	nodeToDomain := make(map[string]utiltas.TopologyDomainID)
	for _, node := range nodes {
		nodeToDomain[node.Name] = snap.addNode(node)
	}
	snap.initialize()
	for domainID, usage := range c.usage {
		snap.addTASUsage(domainID, usage)
	}
	for nodeName, usage := range c.nonTasUsageCache.usagePerNode() {
		if domainID, ok := nodeToDomain[nodeName]; ok {
			snap.addNonTASUsage(domainID, usage)
		}
	}
	return snap
}

func (c *TASFlavorCache) addUsage(key workload.Reference, topologyRequests []workload.TopologyDomainRequests) {
	c.wlUsage[key] = slices.Clone(topologyRequests)
	c.updateUsage(topologyRequests, add)
}

func (c *TASFlavorCache) removeUsage(key workload.Reference) {
	value, found := c.wlUsage[key]
	if !found {
		return
	}
	c.updateUsage(value, subtract)
	delete(c.wlUsage, key)
}

func (c *TASFlavorCache) updateUsage(topologyRequests []workload.TopologyDomainRequests, op usageOp) {
	c.Lock()
	defer c.Unlock()
	for _, tr := range topologyRequests {
		domainID := utiltas.DomainID(tr.Values)
		_, found := c.usage[domainID]
		if !found {
			c.usage[domainID] = resources.Requests{}
		}
		if op == subtract {
			c.usage[domainID].Sub(tr.TotalRequests())
			c.usage[domainID].Sub(resources.Requests{corev1.ResourcePods: int64(tr.Count)})
		} else {
			c.usage[domainID].Add(tr.TotalRequests())
			c.usage[domainID].Add(resources.Requests{corev1.ResourcePods: int64(tr.Count)})
		}
	}
}

type nodeInfo struct {
	// Name holds the node's name, used to evaluate node affinity.
	Name string

	// Labels are used to match Topology levels and NodeSelectors.
	Labels map[string]string

	// Taints are used to check tolerations.
	Taints []corev1.Taint

	// Allocatable capacity from Status.Allocatable.
	Allocatable corev1.ResourceList
}

func newNodeInfo(node *corev1.Node) *nodeInfo {
	return &nodeInfo{
		Name:        node.Name,
		Labels:      node.Labels,
		Taints:      node.Spec.Taints,
		Allocatable: node.Status.Allocatable,
	}
}

func (ni *nodeInfo) toNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   ni.Name,
			Labels: ni.Labels,
		},
		Spec: corev1.NodeSpec{
			Taints: ni.Taints,
		},
		Status: corev1.NodeStatus{
			Allocatable: ni.Allocatable,
		},
	}
}
