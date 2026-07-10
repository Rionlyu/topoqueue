package scheduler_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/example/topoqueue/internal/model"
	"github.com/example/topoqueue/internal/scheduler"
)

func TestSchedulePlacementOutcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		cluster          model.Cluster
		job              model.Job
		wantStatus       scheduler.JobStatus
		wantReason       scheduler.ReasonCode
		wantDomain       string
		wantClusterSlots int64
		wantLargest      string
		wantLargestSlots int64
	}{
		{
			name: "job fits in one rack",
			cluster: model.Cluster{Nodes: []model.Node{
				node("node-a1", "rack-a", 8, 2),
				node("node-a2", "rack-a", 8, 2),
				node("node-b1", "rack-b", 8, 2),
			}},
			job:        job("fit", 3, 2, 1, "rack"),
			wantStatus: scheduler.StatusAdmitted,
			wantDomain: "rack-a",
		},
		{
			name: "total capacity is topology fragmented",
			cluster: model.Cluster{Nodes: []model.Node{
				node("node-b", "rack-b", 8, 2),
				node("node-a", "rack-a", 8, 2),
			}},
			job:              job("fragmented", 3, 2, 1, "rack"),
			wantStatus:       scheduler.StatusPending,
			wantReason:       scheduler.ReasonTopologyFragmented,
			wantClusterSlots: 4,
			wantLargest:      "rack-a",
			wantLargestSlots: 2,
		},
		{
			name: "genuinely insufficient cluster capacity",
			cluster: model.Cluster{Nodes: []model.Node{
				node("node-a", "rack-a", 4, 1),
				node("node-b", "rack-b", 4, 1),
			}},
			job:              job("too-large", 3, 2, 1, "rack"),
			wantStatus:       scheduler.StatusPending,
			wantReason:       scheduler.ReasonInsufficientCapacity,
			wantClusterSlots: 2,
		},
		{
			name: "CPU is limiting",
			cluster: model.Cluster{Nodes: []model.Node{
				node("node-a", "rack-a", 6, 100),
			}},
			job:              job("cpu-bound", 2, 4, 1, "rack"),
			wantStatus:       scheduler.StatusPending,
			wantReason:       scheduler.ReasonInsufficientCapacity,
			wantClusterSlots: 1,
		},
		{
			name: "GPU is limiting",
			cluster: model.Cluster{Nodes: []model.Node{
				node("node-a", "rack-a", 100, 1),
			}},
			job:              job("gpu-bound", 2, 1, 1, "rack"),
			wantStatus:       scheduler.StatusPending,
			wantReason:       scheduler.ReasonInsufficientCapacity,
			wantClusterSlots: 1,
		},
		{
			name: "zero CPU request is ignored",
			cluster: model.Cluster{Nodes: []model.Node{
				node("node-a", "rack-a", 0, 2),
			}},
			job:        job("gpu-only", 2, 0, 1, "rack"),
			wantStatus: scheduler.StatusAdmitted,
			wantDomain: "rack-a",
		},
		{
			name: "zero GPU request is ignored",
			cluster: model.Cluster{Nodes: []model.Node{
				node("node-a", "rack-a", 8, 0),
			}},
			job:        job("cpu-only", 2, 4, 0, "rack"),
			wantStatus: scheduler.StatusAdmitted,
			wantDomain: "rack-a",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result := mustSchedule(t, test.cluster, model.JobSet{Jobs: []model.Job{test.job}}, scheduler.PolicyBackfill)
			got := result.Jobs[0]
			if got.Status != test.wantStatus {
				t.Fatalf("status = %q, want %q", got.Status, test.wantStatus)
			}
			if got.SelectedTopologyDomain != test.wantDomain {
				t.Errorf("selected domain = %q, want %q", got.SelectedTopologyDomain, test.wantDomain)
			}
			if test.wantReason == "" {
				if got.Reason != nil {
					t.Fatalf("reason = %#v, want nil", got.Reason)
				}
				return
			}
			if got.Reason == nil {
				t.Fatalf("reason = nil, want %q", test.wantReason)
			}
			if got.Reason.Code != test.wantReason {
				t.Errorf("reason code = %q, want %q", got.Reason.Code, test.wantReason)
			}
			if got.Reason.RequestedReplicas != test.job.Replicas {
				t.Errorf("requested replicas = %d, want %d", got.Reason.RequestedReplicas, test.job.Replicas)
			}
			switch test.wantReason {
			case scheduler.ReasonInsufficientCapacity:
				if got.Reason.AvailableClusterSlots == nil || *got.Reason.AvailableClusterSlots != test.wantClusterSlots {
					t.Errorf("available cluster slots = %v, want %d", got.Reason.AvailableClusterSlots, test.wantClusterSlots)
				}
			case scheduler.ReasonTopologyFragmented:
				if got.Reason.TopologyKey != "rack" {
					t.Errorf("topology key = %q, want rack", got.Reason.TopologyKey)
				}
				if got.Reason.TotalAvailableSlots == nil || *got.Reason.TotalAvailableSlots != test.wantClusterSlots {
					t.Errorf("total slots = %v, want %d", got.Reason.TotalAvailableSlots, test.wantClusterSlots)
				}
				if got.Reason.LargestDomain != test.wantLargest {
					t.Errorf("largest domain = %q, want %q", got.Reason.LargestDomain, test.wantLargest)
				}
				if got.Reason.LargestDomainSlots == nil || *got.Reason.LargestDomainSlots != test.wantLargestSlots {
					t.Errorf("largest domain slots = %v, want %d", got.Reason.LargestDomainSlots, test.wantLargestSlots)
				}
			}
		})
	}
}

func TestSchedulePolicySemantics(t *testing.T) {
	t.Parallel()
	cluster, jobs := exampleInputs()

	tests := []struct {
		name            string
		policy          scheduler.Policy
		wantStatuses    []scheduler.JobStatus
		wantReasons     []scheduler.ReasonCode
		wantAdmitted    int
		wantPending     int
		wantHeadBlocked int
	}{
		{
			name:            "strict FIFO creates head-of-line blocking",
			policy:          scheduler.PolicyStrictFIFO,
			wantStatuses:    []scheduler.JobStatus{scheduler.StatusAdmitted, scheduler.StatusPending, scheduler.StatusPending},
			wantReasons:     []scheduler.ReasonCode{"", scheduler.ReasonTopologyFragmented, scheduler.ReasonHeadOfLineBlocked},
			wantAdmitted:    1,
			wantPending:     2,
			wantHeadBlocked: 1,
		},
		{
			name:            "backfill admits smaller later job",
			policy:          scheduler.PolicyBackfill,
			wantStatuses:    []scheduler.JobStatus{scheduler.StatusAdmitted, scheduler.StatusPending, scheduler.StatusAdmitted},
			wantReasons:     []scheduler.ReasonCode{"", scheduler.ReasonTopologyFragmented, ""},
			wantAdmitted:    2,
			wantPending:     1,
			wantHeadBlocked: 0,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result := mustSchedule(t, cluster, jobs, test.policy)
			if result.AdmittedJobCount != test.wantAdmitted || result.PendingJobCount != test.wantPending || result.HeadOfLineBlockedJobCount != test.wantHeadBlocked {
				t.Fatalf("counts = admitted %d, pending %d, head-blocked %d; want %d, %d, %d", result.AdmittedJobCount, result.PendingJobCount, result.HeadOfLineBlockedJobCount, test.wantAdmitted, test.wantPending, test.wantHeadBlocked)
			}
			if result.UsedResources != (model.Resources{CPU: int64(test.wantAdmitted * 16), GPU: int64(test.wantAdmitted * 4)}) {
				t.Errorf("used resources = %+v", result.UsedResources)
			}
			for index, got := range result.Jobs {
				if got.Status != test.wantStatuses[index] {
					t.Errorf("job %q status = %q, want %q", got.JobName, got.Status, test.wantStatuses[index])
				}
				wantReason := test.wantReasons[index]
				if wantReason == "" {
					if got.Reason != nil {
						t.Errorf("job %q reason = %#v, want nil", got.JobName, got.Reason)
					}
					continue
				}
				if got.Reason == nil || got.Reason.Code != wantReason {
					t.Errorf("job %q reason = %#v, want %q", got.JobName, got.Reason, wantReason)
				}
			}
			if test.policy == scheduler.PolicyStrictFIFO {
				reason := result.Jobs[2].Reason
				if reason.BlockingJob != "train-xl" || reason.Message != "head_of_line_blocked_by=train-xl" {
					t.Errorf("head-of-line reason = %#v", reason)
				}
			}
		})
	}
}

func TestScheduleDomainSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		cluster        model.Cluster
		job            model.Job
		wantDomain     string
		wantAllocation []scheduler.Allocation
	}{
		{
			name: "smallest fitting domain preserves larger domain",
			cluster: model.Cluster{Nodes: []model.Node{
				node("node-b", "rack-b", 16, 4),
				node("node-a", "rack-a", 8, 2),
			}},
			job:            job("select", 2, 2, 1, "rack"),
			wantDomain:     "rack-a",
			wantAllocation: []scheduler.Allocation{{NodeName: "node-a", Replicas: 2}},
		},
		{
			name: "fewer packed nodes wins equal-slot domain tie",
			cluster: model.Cluster{Nodes: []model.Node{
				node("node-a1", "rack-a", 8, 2),
				node("node-a2", "rack-a", 8, 2),
				node("node-b", "rack-b", 16, 4),
			}},
			job:            job("compact", 3, 2, 1, "rack"),
			wantDomain:     "rack-b",
			wantAllocation: []scheduler.Allocation{{NodeName: "node-b", Replicas: 3}},
		},
		{
			name: "domain tie breaks lexicographically",
			cluster: model.Cluster{Nodes: []model.Node{
				node("node-z", "rack-b", 16, 4),
				node("node-y", "rack-a", 16, 4),
			}},
			job:            job("domain-tie", 2, 2, 1, "rack"),
			wantDomain:     "rack-a",
			wantAllocation: []scheduler.Allocation{{NodeName: "node-y", Replicas: 2}},
		},
		{
			name: "node tie breaks lexicographically",
			cluster: model.Cluster{Nodes: []model.Node{
				node("node-b", "rack-a", 8, 2),
				node("node-a", "rack-a", 8, 2),
			}},
			job:            job("node-tie", 2, 2, 1, "rack"),
			wantDomain:     "rack-a",
			wantAllocation: []scheduler.Allocation{{NodeName: "node-a", Replicas: 2}},
		},
		{
			name: "higher-slot node packs first and allocation output is sorted",
			cluster: model.Cluster{Nodes: []model.Node{
				node("node-a", "rack-a", 8, 2),
				node("node-z", "rack-a", 16, 4),
			}},
			job:        job("node-slots", 5, 2, 1, "rack"),
			wantDomain: "rack-a",
			wantAllocation: []scheduler.Allocation{
				{NodeName: "node-a", Replicas: 1},
				{NodeName: "node-z", Replicas: 4},
			},
		},
		{
			name: "job without topology uses global domain",
			cluster: model.Cluster{Nodes: []model.Node{
				node("node-b", "rack-b", 8, 2),
				node("node-a", "rack-a", 8, 2),
			}},
			job:            job("global", 3, 2, 1, ""),
			wantDomain:     scheduler.GlobalTopologyDomain,
			wantAllocation: []scheduler.Allocation{{NodeName: "node-a", Replicas: 2}, {NodeName: "node-b", Replicas: 1}},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result := mustSchedule(t, test.cluster, model.JobSet{Jobs: []model.Job{test.job}}, scheduler.PolicyBackfill)
			got := result.Jobs[0]
			if got.SelectedTopologyDomain != test.wantDomain {
				t.Errorf("domain = %q, want %q", got.SelectedTopologyDomain, test.wantDomain)
			}
			if !reflect.DeepEqual(got.Allocation, test.wantAllocation) {
				t.Errorf("allocation = %#v, want %#v", got.Allocation, test.wantAllocation)
			}
		})
	}
}

func TestCompareUsesIndependentStateAndStableOrder(t *testing.T) {
	t.Parallel()
	cluster, jobs := exampleInputs()
	originalCluster := cluster.Clone()
	originalJobs := jobs.Clone()

	results, err := scheduler.Compare(context.Background(), cluster, jobs)
	if err != nil {
		t.Fatalf("Compare() error = %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	if results[0].Policy != scheduler.PolicyBackfill || results[1].Policy != scheduler.PolicyStrictFIFO {
		t.Fatalf("policy order = %q, %q", results[0].Policy, results[1].Policy)
	}

	backfill := mustSchedule(t, cluster, jobs, scheduler.PolicyBackfill)
	strict := mustSchedule(t, cluster, jobs, scheduler.PolicyStrictFIFO)
	if !reflect.DeepEqual(results[0], backfill) || !reflect.DeepEqual(results[1], strict) {
		t.Errorf("Compare() results differ from independent runs")
	}
	if !reflect.DeepEqual(cluster, originalCluster) {
		t.Errorf("Compare() mutated cluster: got %#v, want %#v", cluster, originalCluster)
	}
	if !reflect.DeepEqual(jobs, originalJobs) {
		t.Errorf("Compare() mutated jobs: got %#v, want %#v", jobs, originalJobs)
	}
}

func TestRepeatedSchedulingIsDeeplyEqual(t *testing.T) {
	t.Parallel()
	cluster, jobs := exampleInputs()
	want := mustSchedule(t, cluster, jobs, scheduler.PolicyBackfill)
	for iteration := 0; iteration < 50; iteration++ {
		got := mustSchedule(t, cluster, jobs, scheduler.PolicyBackfill)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration %d differs:\ngot:  %#v\nwant: %#v", iteration, got, want)
		}
	}
}

func TestAdmittedAllocationsRespectCapacityAndAccounting(t *testing.T) {
	t.Parallel()
	cluster, jobs := exampleInputs()

	for _, policy := range []scheduler.Policy{scheduler.PolicyBackfill, scheduler.PolicyStrictFIFO} {
		policy := policy
		t.Run(string(policy), func(t *testing.T) {
			t.Parallel()
			result := mustSchedule(t, cluster, jobs, policy)
			assertCapacityAndAccounting(t, cluster, jobs, result)
		})
	}
}

func TestScheduleRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	validCluster := model.Cluster{Nodes: []model.Node{node("node-a", "rack-a", 8, 2)}}
	validJobs := model.JobSet{Jobs: []model.Job{job("work", 1, 1, 1, "rack")}}

	tests := []struct {
		name        string
		cluster     model.Cluster
		jobs        model.JobSet
		wantMessage string
	}{
		{
			name: "duplicate node names",
			cluster: model.Cluster{Nodes: []model.Node{
				node("duplicate", "rack-a", 8, 2),
				node("duplicate", "rack-b", 8, 2),
			}},
			jobs:        validJobs,
			wantMessage: "duplicate name",
		},
		{
			name:    "duplicate job names",
			cluster: validCluster,
			jobs: model.JobSet{Jobs: []model.Job{
				job("duplicate", 1, 1, 1, "rack"),
				job("duplicate", 1, 1, 1, "rack"),
			}},
			wantMessage: "duplicate name",
		},
		{
			name: "missing required topology labels",
			cluster: model.Cluster{Nodes: []model.Node{{
				Name:     "node-a",
				Topology: map[string]string{"zone": "zone-a"},
				Capacity: model.Resources{CPU: 8, GPU: 2},
			}}},
			jobs:        validJobs,
			wantMessage: "missing topology key \"rack\"",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := scheduler.Schedule(context.Background(), test.cluster, test.jobs, scheduler.PolicyBackfill)
			if err == nil {
				t.Fatal("Schedule() error = nil")
			}
			if !strings.Contains(err.Error(), test.wantMessage) {
				t.Errorf("Schedule() error = %q, want substring %q", err, test.wantMessage)
			}
		})
	}
}

func TestScheduleHonorsCanceledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cluster, jobs := exampleInputs()
	_, err := scheduler.Schedule(ctx, cluster, jobs, scheduler.PolicyBackfill)
	if err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("Schedule() error = %v, want context canceled", err)
	}
}

func assertCapacityAndAccounting(t *testing.T, cluster model.Cluster, jobs model.JobSet, result scheduler.PolicyResult) {
	t.Helper()
	capacities := make(map[string]model.Resources, len(cluster.Nodes))
	usedByNode := make(map[string]model.Resources, len(cluster.Nodes))
	jobsByName := make(map[string]model.Job, len(jobs.Jobs))
	for _, node := range cluster.Nodes {
		capacities[node.Name] = node.Capacity
	}
	for _, queuedJob := range jobs.Jobs {
		jobsByName[queuedJob.Name] = queuedJob
	}

	var admittedResources model.Resources
	for _, decision := range result.Jobs {
		if decision.Status != scheduler.StatusAdmitted {
			if decision.AllocatedResources != (model.Resources{}) {
				t.Errorf("pending job %q allocated resources %+v", decision.JobName, decision.AllocatedResources)
			}
			continue
		}
		queuedJob := jobsByName[decision.JobName]
		replicaTotal := 0
		previousNode := ""
		for _, allocation := range decision.Allocation {
			if previousNode != "" && allocation.NodeName <= previousNode {
				t.Errorf("job %q allocation is not sorted: %#v", decision.JobName, decision.Allocation)
			}
			previousNode = allocation.NodeName
			replicaTotal += allocation.Replicas
			used := usedByNode[allocation.NodeName]
			used.CPU += queuedJob.ResourcesPerReplica.CPU * int64(allocation.Replicas)
			used.GPU += queuedJob.ResourcesPerReplica.GPU * int64(allocation.Replicas)
			usedByNode[allocation.NodeName] = used
			capacity, exists := capacities[allocation.NodeName]
			if !exists {
				t.Errorf("job %q allocated unknown node %q", decision.JobName, allocation.NodeName)
			}
			if used.CPU > capacity.CPU || used.GPU > capacity.GPU {
				t.Errorf("node %q used %+v exceeds capacity %+v", allocation.NodeName, used, capacity)
			}
		}
		if replicaTotal != queuedJob.Replicas {
			t.Errorf("job %q allocated %d replicas, want %d", decision.JobName, replicaTotal, queuedJob.Replicas)
		}
		if decision.AllocatedResources != decision.RequestedResources {
			t.Errorf("job %q allocated resources %+v, requested %+v", decision.JobName, decision.AllocatedResources, decision.RequestedResources)
		}
		admittedResources.CPU += decision.AllocatedResources.CPU
		admittedResources.GPU += decision.AllocatedResources.GPU
	}

	if admittedResources != result.UsedResources {
		t.Errorf("summed admitted resources = %+v, result used = %+v", admittedResources, result.UsedResources)
	}
	if result.UsedResources.CPU > result.TotalResources.CPU || result.UsedResources.GPU > result.TotalResources.GPU {
		t.Errorf("used resources %+v exceed total %+v", result.UsedResources, result.TotalResources)
	}
}

func mustSchedule(t *testing.T, cluster model.Cluster, jobs model.JobSet, policy scheduler.Policy) scheduler.PolicyResult {
	t.Helper()
	result, err := scheduler.Schedule(context.Background(), cluster, jobs, policy)
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}
	return result
}

func exampleInputs() (model.Cluster, model.JobSet) {
	cluster := model.Cluster{Nodes: []model.Node{
		node("node-a1", "rack-a", 32, 4),
		node("node-a2", "rack-a", 32, 4),
		node("node-b1", "rack-b", 32, 4),
		node("node-b2", "rack-b", 32, 4),
	}}
	jobs := model.JobSet{Jobs: []model.Job{
		job("warmup", 4, 4, 1, "rack"),
		job("train-xl", 10, 4, 1, "rack"),
		job("batch", 4, 4, 1, "rack"),
	}}
	return cluster, jobs
}

func node(name, rack string, cpu, gpu int64) model.Node {
	return model.Node{
		Name:     name,
		Topology: map[string]string{"zone": "zone-a", "rack": rack},
		Capacity: model.Resources{CPU: cpu, GPU: gpu},
	}
}

func job(name string, replicas int, cpu, gpu int64, topology string) model.Job {
	return model.Job{
		Name:                name,
		Replicas:            replicas,
		ResourcesPerReplica: model.Resources{CPU: cpu, GPU: gpu},
		RequiredTopology:    topology,
	}
}
