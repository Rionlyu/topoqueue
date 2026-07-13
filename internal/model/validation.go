package model

import (
	"fmt"
	"math"
	"sort"
)

// ValidateCluster checks names and resource capacities in a cluster snapshot.
func ValidateCluster(cluster Cluster) error {
	seenNames := make(map[string]int, len(cluster.Nodes))
	var total Resources
	for index, node := range cluster.Nodes {
		if node.Name == "" {
			return fmt.Errorf("node at index %d: name must not be empty", index)
		}
		if firstIndex, exists := seenNames[node.Name]; exists {
			return fmt.Errorf("node %q at index %d: duplicate name (first used at index %d)", node.Name, index, firstIndex)
		}
		seenNames[node.Name] = index

		if node.Capacity.CPU < 0 {
			return fmt.Errorf("node %q: capacity.cpu must be non-negative, got %d", node.Name, node.Capacity.CPU)
		}
		if node.Capacity.GPU < 0 {
			return fmt.Errorf("node %q: capacity.gpu must be non-negative, got %d", node.Name, node.Capacity.GPU)
		}
		if total.CPU > math.MaxInt64-node.Capacity.CPU {
			return fmt.Errorf("node %q: cluster total capacity.cpu exceeds %d", node.Name, int64(math.MaxInt64))
		}
		if total.GPU > math.MaxInt64-node.Capacity.GPU {
			return fmt.Errorf("node %q: cluster total capacity.gpu exceeds %d", node.Name, int64(math.MaxInt64))
		}
		total.CPU += node.Capacity.CPU
		total.GPU += node.Capacity.GPU
	}
	return nil
}

// ValidateJobs checks names, replica counts, and per-replica requests in queue order.
func ValidateJobs(jobSet JobSet) error {
	seenNames := make(map[string]int, len(jobSet.Jobs))
	for index, job := range jobSet.Jobs {
		if job.Name == "" {
			return fmt.Errorf("job at index %d: name must not be empty", index)
		}
		if firstIndex, exists := seenNames[job.Name]; exists {
			return fmt.Errorf("job %q at index %d: duplicate name (first used at index %d)", job.Name, index, firstIndex)
		}
		seenNames[job.Name] = index

		if job.Replicas <= 0 {
			return fmt.Errorf("job %q: replicas must be greater than zero, got %d", job.Name, job.Replicas)
		}
		if job.ResourcesPerReplica.CPU < 0 {
			return fmt.Errorf("job %q: resourcesPerReplica.cpu must be non-negative, got %d", job.Name, job.ResourcesPerReplica.CPU)
		}
		if job.ResourcesPerReplica.GPU < 0 {
			return fmt.Errorf("job %q: resourcesPerReplica.gpu must be non-negative, got %d", job.Name, job.ResourcesPerReplica.GPU)
		}
		if job.ResourcesPerReplica.CPU == 0 && job.ResourcesPerReplica.GPU == 0 {
			return fmt.Errorf("job %q: at least one resource per replica must be positive", job.Name)
		}
		replicas := int64(job.Replicas)
		if job.ResourcesPerReplica.CPU > 0 && replicas > math.MaxInt64/job.ResourcesPerReplica.CPU {
			return fmt.Errorf("job %q: total requested CPU exceeds %d", job.Name, int64(math.MaxInt64))
		}
		if job.ResourcesPerReplica.GPU > 0 && replicas > math.MaxInt64/job.ResourcesPerReplica.GPU {
			return fmt.Errorf("job %q: total requested GPU exceeds %d", job.Name, int64(math.MaxInt64))
		}
	}
	return nil
}

// ValidateTimedJobs checks the static job fields and lifecycle timing in input
// order. Required-field presence is enforced by LoadTimedJobs because zero is
// a valid arrival tick in the public value model.
func ValidateTimedJobs(jobSet TimedJobSet) error {
	if err := ValidateJobs(jobSet.StaticJobs()); err != nil {
		return err
	}

	for _, job := range jobSet.Jobs {
		if job.ArrivalTick < 0 {
			return fmt.Errorf("job %q: arrivalTick must be non-negative, got %d", job.Name, job.ArrivalTick)
		}
		if job.DurationTicks <= 0 {
			return fmt.Errorf("job %q: durationTicks must be greater than zero, got %d", job.Name, job.DurationTicks)
		}
	}
	return nil
}

// ValidateTopologyRequirements verifies that every node has every topology key
// required by the jobs.
func ValidateTopologyRequirements(cluster Cluster, jobSet JobSet) error {
	requiredBy := make(map[string][]string)
	for _, job := range jobSet.Jobs {
		if job.RequiredTopology != "" {
			requiredBy[job.RequiredTopology] = append(requiredBy[job.RequiredTopology], job.Name)
		}
	}

	keys := make([]string, 0, len(requiredBy))
	for key := range requiredBy {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, node := range cluster.Nodes {
		for _, key := range keys {
			value, exists := node.Topology[key]
			if !exists {
				return fmt.Errorf("node %q: missing topology key %q required by job %q", node.Name, key, requiredBy[key][0])
			}
			if value == "" {
				return fmt.Errorf("node %q: topology key %q required by job %q must have a non-empty value", node.Name, key, requiredBy[key][0])
			}
		}
	}
	return nil
}
