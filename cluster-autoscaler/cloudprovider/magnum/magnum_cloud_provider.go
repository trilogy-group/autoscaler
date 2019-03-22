/*
Copyright 2019 The Kubernetes Authors.

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

package magnum

import (
	"io"
	"os"
	"sync"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/config/dynamic"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/klog"
)

const (
	// ProviderName is the cloud provider name for Magnum
	ProviderName = "magnum"
)

// magnumCloudProvider implements CloudProvider interface from cluster-autoscaler/cloudprovider module.
type magnumCloudProvider struct {
	magnumManager   *magnumManager
	resourceLimiter *cloudprovider.ResourceLimiter
	nodeGroups      []magnumNodeGroup
}

func buildMagnumCloudProvider(magnumManager magnumManager, resourceLimiter *cloudprovider.ResourceLimiter) (cloudprovider.CloudProvider, error) {
	os := &magnumCloudProvider{
		magnumManager:   &magnumManager,
		resourceLimiter: resourceLimiter,
		nodeGroups:      []magnumNodeGroup{},
	}
	return os, nil
}

// Name returns the name of the cloud provider.
func (os *magnumCloudProvider) Name() string {
	return ProviderName
}

// NodeGroups returns all node groups managed by this cloud provider.
func (os *magnumCloudProvider) NodeGroups() []cloudprovider.NodeGroup {
	groups := make([]cloudprovider.NodeGroup, len(os.nodeGroups))
	for i, group := range os.nodeGroups {
		groups[i] = &group
	}
	return groups
}

// AddNodeGroup appends a node group to the list of node groups managed by this cloud provider.
func (os *magnumCloudProvider) AddNodeGroup(group magnumNodeGroup) {
	os.nodeGroups = append(os.nodeGroups, group)
}

// NodeGroupForNode returns the node group that a given node belongs to.
//
// Since only a single node group is currently supported, the first node group is always returned.
func (os *magnumCloudProvider) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	// TODO: wait for magnum nodegroup support
	return &(os.nodeGroups[0]), nil
}

// Pricing is not implemented.
func (os *magnumCloudProvider) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
	return nil, cloudprovider.ErrNotImplemented
}

// GetAvailableMachineTypes is not implemented.
func (os *magnumCloudProvider) GetAvailableMachineTypes() ([]string, error) {
	return []string{}, nil
}

// NewNodeGroup is not implemented.
func (os *magnumCloudProvider) NewNodeGroup(machineType string, labels map[string]string, systemLabels map[string]string,
	taints []apiv1.Taint, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

// GetResourceLimiter returns resource constraints for the cloud provider
func (os *magnumCloudProvider) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	return os.resourceLimiter, nil
}

// GetInstanceID gets the instance ID for the specified node.
func (os *magnumCloudProvider) GetInstanceID(node *apiv1.Node) string {
	return node.Spec.ProviderID
}

// Refresh is called before every autoscaler main loop.
//
// Currently only prints debug information.
func (os *magnumCloudProvider) Refresh() error {
	for _, nodegroup := range os.nodeGroups {
		klog.V(3).Info(nodegroup.Debug())
	}
	return nil
}

// Cleanup currently does nothing.
func (os *magnumCloudProvider) Cleanup() error {
	return nil
}

// BuildMagnum is called by the autoscaler to build a magnum cloud provider.
//
// The magnumManager is created here, and the node groups are created
// based on the specs provided via the command line parameters.
func BuildMagnum(opts config.AutoscalingOptions, do cloudprovider.NodeGroupDiscoveryOptions, rl *cloudprovider.ResourceLimiter) cloudprovider.CloudProvider {
	var config io.ReadCloser

	// Should be loaded with --cloud-config /etc/kubernetes/kube_openstack_config from master node
	if opts.CloudConfig != "" {
		var err error
		config, err = os.Open(opts.CloudConfig)
		if err != nil {
			klog.Fatalf("Couldn't open cloud provider configuration %s: %#v", opts.CloudConfig, err)
		}
		defer config.Close()
	}

	manager, err := createMagnumManager(config, do, opts)
	if err != nil {
		klog.Fatalf("Failed to create magnum manager: %v", err)
	}

	provider, err := buildMagnumCloudProvider(manager, rl)
	if err != nil {
		klog.Fatalf("Failed to create magnum cloud provider: %v", err)
	}

	// TODO: When magnum node group support is available then 0 NodeGroupSpecs will be a valid input.
	if len(do.NodeGroupSpecs) == 0 {
		klog.Fatalf("Must specify at least one node group with --nodes=<min>:<max>:<name>,...")
	}

	// TODO: Temporary, only makes sense to have one nodegroup until magnum nodegroups are implemented
	// Node groups will be available in k8s_fedora_atomic_v2 driver https://review.openstack.org/#/c/629274/
	if len(do.NodeGroupSpecs) > 1 {
		klog.Fatalf("Magnum autoscaler only supports a single nodegroup for now")
	}

	clusterUpdateLock := sync.Mutex{}

	for _, nodegroupSpec := range do.NodeGroupSpecs {
		spec, err := dynamic.SpecFromString(nodegroupSpec, scaleToZeroSupported)
		if err != nil {
			klog.Fatalf("Could not parse node group spec %s: %v", nodegroupSpec, err)
		}

		ng := magnumNodeGroup{
			magnumManager:       manager,
			id:                  spec.Name,
			clusterUpdateMutex:  &clusterUpdateLock,
			minSize:             spec.MinSize,
			maxSize:             spec.MaxSize,
			targetSize:          new(int),
			waitTimeStep:        waitForStatusTimeStep,
			deleteBatchingDelay: deleteNodesBatchingDelay,
		}
		*ng.targetSize, err = ng.magnumManager.nodeGroupSize(ng.id)
		if err != nil {
			klog.Fatalf("Could not set current nodes in node group: %v", err)
		}
		provider.(*magnumCloudProvider).AddNodeGroup(ng)
	}

	return provider
}
