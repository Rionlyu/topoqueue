package scheduler

import (
	"context"
	"fmt"
	"sort"

	"github.com/example/topoqueue/internal/model"
)

type placementResult struct {
	domain     string
	allocation []Allocation
	reason     *Reason
}

type topologyDomain struct {
	name        string
	nodeIndexes []int
	slots       int64
}

type packedNode struct {
	index    int
	name     string
	replicas int
}

type placementCandidate struct {
	domain    string
	slots     int64
	packed    []packedNode
	nodesUsed int
}

func placeJob(ctx context.Context, nodes []nodeState, job model.Job) (placementResult, error) {
	requested := int64(job.Replicas)
	nodeSlots := make([]int64, len(nodes))
	var clusterSlots int64
	for index := range nodes {
		if err := ctx.Err(); err != nil {
			return placementResult{}, err
		}
		nodeSlots[index] = availableReplicaSlots(nodes[index].remaining, job.ResourcesPerReplica)
		clusterSlots += nodeSlots[index]
	}

	domains, err := buildDomains(ctx, nodes, nodeSlots, job.RequiredTopology)
	if err != nil {
		return placementResult{}, err
	}
	largestDomain, largestSlots := largestAvailableDomain(domains)

	var candidates []placementCandidate
	for _, domain := range domains {
		if err := ctx.Err(); err != nil {
			return placementResult{}, err
		}
		if domain.slots < requested {
			continue
		}
		packed, err := compactPacking(ctx, nodes, nodeSlots, domain.nodeIndexes, job.Replicas)
		if err != nil {
			return placementResult{}, err
		}
		candidates = append(candidates, placementCandidate{
			domain:    domain.name,
			slots:     domain.slots,
			packed:    packed,
			nodesUsed: len(packed),
		})
	}

	if len(candidates) == 0 {
		if clusterSlots < requested {
			available := clusterSlots
			return placementResult{reason: &Reason{
				Code:                  ReasonInsufficientCapacity,
				Message:               fmt.Sprintf("requested %d replicas, but only %d cluster-wide replica slots are available", job.Replicas, clusterSlots),
				RequestedReplicas:     job.Replicas,
				AvailableClusterSlots: &available,
			}}, nil
		}
		if job.RequiredTopology == "" {
			return placementResult{}, fmt.Errorf("global domain has %d slots for %d replicas but produced no placement", clusterSlots, job.Replicas)
		}
		total := clusterSlots
		largest := largestSlots
		return placementResult{reason: &Reason{
			Code:                ReasonTopologyFragmented,
			Message:             fmt.Sprintf("requested %d replicas in one %q domain; %d slots are available cluster-wide, but the largest domain %q has %d", job.Replicas, job.RequiredTopology, clusterSlots, largestDomain, largestSlots),
			RequestedReplicas:   job.Replicas,
			TopologyKey:         job.RequiredTopology,
			TotalAvailableSlots: &total,
			LargestDomain:       largestDomain,
			LargestDomainSlots:  &largest,
		}}, nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].slots != candidates[j].slots {
			return candidates[i].slots < candidates[j].slots
		}
		if candidates[i].nodesUsed != candidates[j].nodesUsed {
			return candidates[i].nodesUsed < candidates[j].nodesUsed
		}
		return candidates[i].domain < candidates[j].domain
	})
	selected := candidates[0]
	allocations := make([]Allocation, 0, len(selected.packed))
	for _, packed := range selected.packed {
		consumed := resourcesForReplicas(job.ResourcesPerReplica, packed.replicas)
		nodes[packed.index].remaining.CPU -= consumed.CPU
		nodes[packed.index].remaining.GPU -= consumed.GPU
		allocations = append(allocations, Allocation{NodeName: packed.name, Replicas: packed.replicas})
	}
	sort.Slice(allocations, func(i, j int) bool {
		return allocations[i].NodeName < allocations[j].NodeName
	})

	return placementResult{domain: selected.domain, allocation: allocations}, nil
}

func buildDomains(ctx context.Context, nodes []nodeState, nodeSlots []int64, topologyKey string) ([]topologyDomain, error) {
	if topologyKey == "" {
		indexes := make([]int, len(nodes))
		var slots int64
		for index := range nodes {
			indexes[index] = index
			slots += nodeSlots[index]
		}
		return []topologyDomain{{name: GlobalTopologyDomain, nodeIndexes: indexes, slots: slots}}, nil
	}

	indexesByDomain := make(map[string][]int)
	for index, node := range nodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		domainName := node.topology[topologyKey]
		indexesByDomain[domainName] = append(indexesByDomain[domainName], index)
	}
	domainNames := make([]string, 0, len(indexesByDomain))
	for domainName := range indexesByDomain {
		domainNames = append(domainNames, domainName)
	}
	sort.Strings(domainNames)

	domains := make([]topologyDomain, 0, len(domainNames))
	for _, domainName := range domainNames {
		domain := topologyDomain{name: domainName, nodeIndexes: indexesByDomain[domainName]}
		for _, index := range domain.nodeIndexes {
			domain.slots += nodeSlots[index]
		}
		domains = append(domains, domain)
	}
	return domains, nil
}

func largestAvailableDomain(domains []topologyDomain) (string, int64) {
	if len(domains) == 0 {
		return "", 0
	}
	name := domains[0].name
	slots := domains[0].slots
	for _, domain := range domains[1:] {
		if domain.slots > slots {
			name = domain.name
			slots = domain.slots
		}
	}
	return name, slots
}

func compactPacking(ctx context.Context, nodes []nodeState, nodeSlots []int64, indexes []int, replicas int) ([]packedNode, error) {
	ordered := make([]packedNode, 0, len(indexes))
	for _, index := range indexes {
		if nodeSlots[index] == 0 {
			continue
		}
		ordered = append(ordered, packedNode{index: index, name: nodes[index].name})
	}
	sort.Slice(ordered, func(i, j int) bool {
		leftSlots := nodeSlots[ordered[i].index]
		rightSlots := nodeSlots[ordered[j].index]
		if leftSlots != rightSlots {
			return leftSlots > rightSlots
		}
		return ordered[i].name < ordered[j].name
	})

	remaining := replicas
	packed := make([]packedNode, 0, len(ordered))
	for _, node := range ordered {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if remaining == 0 {
			break
		}
		count := int64(remaining)
		if nodeSlots[node.index] < count {
			count = nodeSlots[node.index]
		}
		node.replicas = int(count)
		packed = append(packed, node)
		remaining -= node.replicas
	}
	if remaining != 0 {
		return nil, fmt.Errorf("domain advertised enough capacity but has %d unplaced replicas", remaining)
	}
	return packed, nil
}

func availableReplicaSlots(remaining, perReplica model.Resources) int64 {
	var slots int64
	limited := false
	if perReplica.CPU > 0 {
		slots = remaining.CPU / perReplica.CPU
		limited = true
	}
	if perReplica.GPU > 0 {
		gpuSlots := remaining.GPU / perReplica.GPU
		if !limited || gpuSlots < slots {
			slots = gpuSlots
		}
		limited = true
	}
	if !limited {
		return 0
	}
	return slots
}
