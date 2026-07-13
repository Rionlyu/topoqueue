package scheduler

import (
	"fmt"
	"math"

	"github.com/Rionlyu/topoqueue/internal/model"
)

type pendingRelease struct {
	nodeIndex int
	resources model.Resources
}

// releaseAllocation returns the resources represented by allocation to the
// nodes that supplied them. It validates the complete release before changing
// any node so an invalid allocation cannot partially mutate scheduler state.
func releaseAllocation(nodes []nodeState, job model.Job, allocation []Allocation) error {
	if job.Replicas <= 0 {
		return fmt.Errorf("release job %q: replicas must be greater than zero, got %d", job.Name, job.Replicas)
	}
	if job.ResourcesPerReplica.CPU < 0 {
		return fmt.Errorf("release job %q: resources per replica CPU must be non-negative, got %d", job.Name, job.ResourcesPerReplica.CPU)
	}
	if job.ResourcesPerReplica.GPU < 0 {
		return fmt.Errorf("release job %q: resources per replica GPU must be non-negative, got %d", job.Name, job.ResourcesPerReplica.GPU)
	}
	if job.ResourcesPerReplica.CPU == 0 && job.ResourcesPerReplica.GPU == 0 {
		return fmt.Errorf("release job %q: at least one resource per replica must be positive", job.Name)
	}

	nodeIndexes := make(map[string]int, len(nodes))
	for index, node := range nodes {
		if _, exists := nodeIndexes[node.name]; exists {
			return fmt.Errorf("release job %q: scheduler state contains duplicate node %q", job.Name, node.name)
		}
		if err := validateNodeResourceState(node); err != nil {
			return fmt.Errorf("release job %q: node %q: %w", job.Name, node.name, err)
		}
		nodeIndexes[node.name] = index
	}

	releases := make([]pendingRelease, 0, len(allocation))
	seenNodes := make(map[string]struct{}, len(allocation))
	var allocatedReplicas int64
	for allocationIndex, assigned := range allocation {
		if assigned.Replicas <= 0 {
			return fmt.Errorf("release job %q allocation at index %d for node %q: replicas must be greater than zero, got %d", job.Name, allocationIndex, assigned.NodeName, assigned.Replicas)
		}
		nodeIndex, exists := nodeIndexes[assigned.NodeName]
		if !exists {
			return fmt.Errorf("release job %q allocation at index %d: unknown node %q", job.Name, allocationIndex, assigned.NodeName)
		}
		if _, duplicate := seenNodes[assigned.NodeName]; duplicate {
			return fmt.Errorf("release job %q allocation at index %d: duplicate node %q", job.Name, allocationIndex, assigned.NodeName)
		}
		seenNodes[assigned.NodeName] = struct{}{}

		replicaCount := int64(assigned.Replicas)
		if allocatedReplicas > math.MaxInt64-replicaCount {
			return fmt.Errorf("release job %q: allocated replica total exceeds %d", job.Name, int64(math.MaxInt64))
		}
		allocatedReplicas += replicaCount

		released, err := checkedResourcesForReplicas(job.ResourcesPerReplica, replicaCount)
		if err != nil {
			return fmt.Errorf("release job %q allocation for node %q: %w", job.Name, assigned.NodeName, err)
		}
		node := nodes[nodeIndex]
		if released.CPU > node.capacity.CPU-node.remaining.CPU {
			return fmt.Errorf("release job %q allocation for node %q: released CPU %d exceeds consumed CPU %d", job.Name, assigned.NodeName, released.CPU, node.capacity.CPU-node.remaining.CPU)
		}
		if released.GPU > node.capacity.GPU-node.remaining.GPU {
			return fmt.Errorf("release job %q allocation for node %q: released GPU %d exceeds consumed GPU %d", job.Name, assigned.NodeName, released.GPU, node.capacity.GPU-node.remaining.GPU)
		}
		releases = append(releases, pendingRelease{nodeIndex: nodeIndex, resources: released})
	}

	if allocatedReplicas != int64(job.Replicas) {
		return fmt.Errorf("release job %q: allocation represents %d replicas, want exactly %d", job.Name, allocatedReplicas, job.Replicas)
	}

	for _, release := range releases {
		nodes[release.nodeIndex].remaining.CPU += release.resources.CPU
		nodes[release.nodeIndex].remaining.GPU += release.resources.GPU
	}
	return nil
}

func validateNodeResourceState(node nodeState) error {
	if node.capacity.CPU < 0 || node.capacity.GPU < 0 {
		return fmt.Errorf("capacity must be non-negative, got CPU %d and GPU %d", node.capacity.CPU, node.capacity.GPU)
	}
	if node.remaining.CPU < 0 || node.remaining.GPU < 0 {
		return fmt.Errorf("remaining capacity must be non-negative, got CPU %d and GPU %d", node.remaining.CPU, node.remaining.GPU)
	}
	if node.remaining.CPU > node.capacity.CPU || node.remaining.GPU > node.capacity.GPU {
		return fmt.Errorf("remaining capacity CPU %d and GPU %d exceeds original capacity CPU %d and GPU %d", node.remaining.CPU, node.remaining.GPU, node.capacity.CPU, node.capacity.GPU)
	}
	return nil
}

func checkedResourcesForReplicas(perReplica model.Resources, replicas int64) (model.Resources, error) {
	if replicas <= 0 {
		return model.Resources{}, fmt.Errorf("replicas must be greater than zero, got %d", replicas)
	}
	if perReplica.CPU > 0 && replicas > math.MaxInt64/perReplica.CPU {
		return model.Resources{}, fmt.Errorf("released CPU exceeds %d", int64(math.MaxInt64))
	}
	if perReplica.GPU > 0 && replicas > math.MaxInt64/perReplica.GPU {
		return model.Resources{}, fmt.Errorf("released GPU exceeds %d", int64(math.MaxInt64))
	}
	return model.Resources{
		CPU: perReplica.CPU * replicas,
		GPU: perReplica.GPU * replicas,
	}, nil
}
