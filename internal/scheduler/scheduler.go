// Package scheduler simulates deterministic queue policies against a cluster snapshot.
package scheduler

import (
	"context"
	"fmt"
	"sort"

	"github.com/Rionlyu/topoqueue/internal/model"
)

// Policy names a supported queue policy.
type Policy string

const (
	// PolicyStrictFIFO stops attempting jobs after the first placement failure.
	PolicyStrictFIFO Policy = "strict-fifo"
	// PolicyBackfill continues attempting jobs after a placement failure.
	PolicyBackfill Policy = "backfill"
)

// JobStatus describes the admission decision for one job.
type JobStatus string

const (
	// StatusAdmitted means every replica was placed.
	StatusAdmitted JobStatus = "admitted"
	// StatusPending means the job was not placed.
	StatusPending JobStatus = "pending"
)

// ReasonCode identifies why a job remains pending.
type ReasonCode string

const (
	// ReasonInsufficientCapacity means too few replica slots exist cluster-wide.
	ReasonInsufficientCapacity ReasonCode = "insufficient_capacity"
	// ReasonTopologyFragmented means enough slots exist globally but not in one domain.
	ReasonTopologyFragmented ReasonCode = "topology_fragmented"
	// ReasonHeadOfLineBlocked means strict FIFO did not attempt a later job.
	ReasonHeadOfLineBlocked ReasonCode = "head_of_line_blocked"
)

// GlobalTopologyDomain is reported when a job has no topology requirement.
const GlobalTopologyDomain = "global"

// Allocation records how many replicas were assigned to a node.
type Allocation struct {
	NodeName string `json:"nodeName"`
	Replicas int    `json:"replicas"`
}

// Reason contains a stable code, an explanatory message, and code-specific details.
// Pointer fields distinguish a meaningful zero from a detail that does not apply.
type Reason struct {
	Code                  ReasonCode `json:"code"`
	Message               string     `json:"message"`
	RequestedReplicas     int        `json:"requestedReplicas,omitempty"`
	AvailableClusterSlots *int64     `json:"availableClusterWideReplicaSlots,omitempty"`
	TopologyKey           string     `json:"topologyKey,omitempty"`
	TotalAvailableSlots   *int64     `json:"totalAvailableReplicaSlots,omitempty"`
	LargestDomain         string     `json:"largestTopologyDomain,omitempty"`
	LargestDomainSlots    *int64     `json:"largestTopologyDomainReplicaSlots,omitempty"`
	BlockingJob           string     `json:"blockingJob,omitempty"`
}

// JobResult is the admission decision and resource accounting for one job.
type JobResult struct {
	JobName                string          `json:"jobName"`
	Status                 JobStatus       `json:"status"`
	SelectedTopologyDomain string          `json:"selectedTopologyDomain,omitempty"`
	Allocation             []Allocation    `json:"allocation"`
	Reason                 *Reason         `json:"reason,omitempty"`
	RequestedResources     model.Resources `json:"requestedResources"`
	AllocatedResources     model.Resources `json:"allocatedResources"`
}

// PolicyResult contains ordered job decisions and aggregate resource accounting.
type PolicyResult struct {
	Policy                    Policy          `json:"policy"`
	Jobs                      []JobResult     `json:"jobs"`
	AdmittedJobCount          int             `json:"admittedJobCount"`
	PendingJobCount           int             `json:"pendingJobCount"`
	HeadOfLineBlockedJobCount int             `json:"headOfLineBlockedJobCount"`
	UsedResources             model.Resources `json:"usedResources"`
	TotalResources            model.Resources `json:"totalResources"`
}

// ParsePolicy converts a CLI policy name to a Policy.
func ParsePolicy(value string) (Policy, error) {
	policy := Policy(value)
	switch policy {
	case PolicyStrictFIFO, PolicyBackfill:
		return policy, nil
	default:
		return "", fmt.Errorf("unsupported policy %q (supported: %s, %s)", value, PolicyStrictFIFO, PolicyBackfill)
	}
}

// Schedule evaluates jobs in input order with a single deterministic policy run.
func Schedule(ctx context.Context, cluster model.Cluster, jobs model.JobSet, policy Policy) (PolicyResult, error) {
	if ctx == nil {
		return PolicyResult{}, fmt.Errorf("schedule policy %q: nil context", policy)
	}
	if _, err := ParsePolicy(string(policy)); err != nil {
		return PolicyResult{}, fmt.Errorf("schedule: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return PolicyResult{}, fmt.Errorf("schedule policy %q: %w", policy, err)
	}
	if err := model.ValidateCluster(cluster); err != nil {
		return PolicyResult{}, fmt.Errorf("schedule policy %q: validate cluster: %w", policy, err)
	}
	if err := model.ValidateJobs(jobs); err != nil {
		return PolicyResult{}, fmt.Errorf("schedule policy %q: validate jobs: %w", policy, err)
	}
	if err := model.ValidateTopologyRequirements(cluster, jobs); err != nil {
		return PolicyResult{}, fmt.Errorf("schedule policy %q: validate topology requirements: %w", policy, err)
	}

	nodes := makeNodeStates(cluster)
	result := PolicyResult{
		Policy:         policy,
		Jobs:           make([]JobResult, 0, len(jobs.Jobs)),
		TotalResources: totalResources(cluster),
	}
	blockingJob := ""

	for _, job := range jobs.Jobs {
		if err := ctx.Err(); err != nil {
			return PolicyResult{}, fmt.Errorf("schedule policy %q before job %q: %w", policy, job.Name, err)
		}

		jobResult := JobResult{
			JobName:            job.Name,
			Status:             StatusPending,
			Allocation:         make([]Allocation, 0),
			RequestedResources: resourcesForReplicas(job.ResourcesPerReplica, job.Replicas),
		}

		if blockingJob != "" {
			jobResult.Reason = headOfLineReason(blockingJob)
			result.PendingJobCount++
			result.HeadOfLineBlockedJobCount++
			result.Jobs = append(result.Jobs, jobResult)
			continue
		}

		placed, err := placeJob(ctx, nodes, job)
		if err != nil {
			return PolicyResult{}, fmt.Errorf("schedule policy %q job %q: %w", policy, job.Name, err)
		}
		if placed.reason != nil {
			jobResult.Reason = placed.reason
			result.PendingJobCount++
			if policy == PolicyStrictFIFO {
				blockingJob = job.Name
			}
			result.Jobs = append(result.Jobs, jobResult)
			continue
		}

		jobResult.Status = StatusAdmitted
		jobResult.SelectedTopologyDomain = placed.domain
		jobResult.Allocation = placed.allocation
		jobResult.AllocatedResources = jobResult.RequestedResources
		result.AdmittedJobCount++
		result.UsedResources = addResources(result.UsedResources, jobResult.AllocatedResources)
		result.Jobs = append(result.Jobs, jobResult)
	}

	return result, nil
}

type nodeState struct {
	name      string
	topology  map[string]string
	capacity  model.Resources
	remaining model.Resources
}

func makeNodeStates(cluster model.Cluster) []nodeState {
	nodes := make([]nodeState, len(cluster.Nodes))
	for index, node := range cluster.Nodes {
		nodes[index] = nodeState{
			name:      node.Name,
			topology:  node.Topology,
			capacity:  node.Capacity,
			remaining: node.Capacity,
		}
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].name < nodes[j].name
	})
	return nodes
}

func totalResources(cluster model.Cluster) model.Resources {
	var total model.Resources
	for _, node := range cluster.Nodes {
		total = addResources(total, node.Capacity)
	}
	return total
}

func resourcesForReplicas(perReplica model.Resources, replicas int) model.Resources {
	return model.Resources{
		CPU: perReplica.CPU * int64(replicas),
		GPU: perReplica.GPU * int64(replicas),
	}
}

func addResources(left, right model.Resources) model.Resources {
	return model.Resources{
		CPU: left.CPU + right.CPU,
		GPU: left.GPU + right.GPU,
	}
}

func headOfLineReason(blockingJob string) *Reason {
	return &Reason{
		Code:        ReasonHeadOfLineBlocked,
		Message:     fmt.Sprintf("head_of_line_blocked_by=%s", blockingJob),
		BlockingJob: blockingJob,
	}
}
