package scheduler

import (
	"context"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/Rionlyu/topoqueue/internal/model"
)

func TestReleaseAllocationExactlyRestoresPlacedResources(t *testing.T) {
	t.Parallel()

	cluster := model.Cluster{Nodes: []model.Node{
		{Name: "node-b", Capacity: model.Resources{CPU: 2, GPU: 1}},
		{Name: "node-a", Capacity: model.Resources{CPU: 4, GPU: 2}},
	}}
	job := model.Job{
		Name:                "training",
		Replicas:            3,
		ResourcesPerReplica: model.Resources{CPU: 2, GPU: 1},
	}
	nodes := makeNodeStates(cluster)
	placed, err := placeJob(context.Background(), nodes, job)
	if err != nil {
		t.Fatalf("placeJob() error = %v", err)
	}
	if placed.reason != nil {
		t.Fatalf("placeJob() reason = %#v, want admitted", placed.reason)
	}
	if err := releaseAllocation(nodes, job, placed.allocation); err != nil {
		t.Fatalf("releaseAllocation() error = %v", err)
	}

	for _, node := range nodes {
		if node.remaining != node.capacity {
			t.Errorf("node %q remaining = %+v, want original capacity %+v", node.name, node.remaining, node.capacity)
		}
	}
}

func TestReleaseAllocationSupportsSingleResourceJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		capacity   model.Resources
		perReplica model.Resources
	}{
		{
			name:       "CPU only",
			capacity:   model.Resources{CPU: 8},
			perReplica: model.Resources{CPU: 4},
		},
		{
			name:       "GPU only",
			capacity:   model.Resources{GPU: 4},
			perReplica: model.Resources{GPU: 2},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			job := model.Job{Name: "work", Replicas: 2, ResourcesPerReplica: test.perReplica}
			nodes := makeNodeStates(model.Cluster{Nodes: []model.Node{{Name: "node-a", Capacity: test.capacity}}})
			placed, err := placeJob(context.Background(), nodes, job)
			if err != nil {
				t.Fatalf("placeJob() error = %v", err)
			}
			if placed.reason != nil {
				t.Fatalf("placeJob() reason = %#v, want admitted", placed.reason)
			}
			if err := releaseAllocation(nodes, job, placed.allocation); err != nil {
				t.Fatalf("releaseAllocation() error = %v", err)
			}
			if nodes[0].remaining != test.capacity {
				t.Errorf("remaining = %+v, want %+v", nodes[0].remaining, test.capacity)
			}
		})
	}
}

func TestReleaseAllocationRejectsInvalidAllocationWithoutMutation(t *testing.T) {
	t.Parallel()

	validJob := model.Job{
		Name:                "work",
		Replicas:            2,
		ResourcesPerReplica: model.Resources{CPU: 2, GPU: 1},
	}
	validNodes := []nodeState{
		{name: "node-a", capacity: model.Resources{CPU: 8, GPU: 4}, remaining: model.Resources{CPU: 6, GPU: 3}},
		{name: "node-b", capacity: model.Resources{CPU: 8, GPU: 4}, remaining: model.Resources{CPU: 6, GPU: 3}},
	}
	tests := []struct {
		name       string
		nodes      []nodeState
		job        model.Job
		allocation []Allocation
		wantError  string
	}{
		{
			name:       "unknown node after valid entry",
			nodes:      validNodes,
			job:        validJob,
			allocation: []Allocation{{NodeName: "node-a", Replicas: 1}, {NodeName: "missing", Replicas: 1}},
			wantError:  "unknown node",
		},
		{
			name:       "duplicate node",
			nodes:      validNodes,
			job:        validJob,
			allocation: []Allocation{{NodeName: "node-a", Replicas: 1}, {NodeName: "node-a", Replicas: 1}},
			wantError:  "duplicate node",
		},
		{
			name:       "zero replicas",
			nodes:      validNodes,
			job:        validJob,
			allocation: []Allocation{{NodeName: "node-a", Replicas: 0}},
			wantError:  "replicas must be greater than zero",
		},
		{
			name:       "negative replicas",
			nodes:      validNodes,
			job:        validJob,
			allocation: []Allocation{{NodeName: "node-a", Replicas: -1}},
			wantError:  "replicas must be greater than zero",
		},
		{
			name:       "partial replica total",
			nodes:      validNodes,
			job:        validJob,
			allocation: []Allocation{{NodeName: "node-a", Replicas: 1}},
			wantError:  "want exactly 2",
		},
		{
			name: "resource multiplication overflow",
			nodes: []nodeState{{
				name: "node-a", capacity: model.Resources{CPU: math.MaxInt64}, remaining: model.Resources{},
			}},
			job: model.Job{
				Name: "overflow", Replicas: 2, ResourcesPerReplica: model.Resources{CPU: math.MaxInt64},
			},
			allocation: []Allocation{{NodeName: "node-a", Replicas: 2}},
			wantError:  "released CPU exceeds",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			nodes := append([]nodeState(nil), test.nodes...)
			before := append([]nodeState(nil), nodes...)
			err := releaseAllocation(nodes, test.job, test.allocation)
			if err == nil {
				t.Fatalf("releaseAllocation() error = nil, want containing %q", test.wantError)
			}
			if !strings.Contains(err.Error(), test.wantError) {
				t.Errorf("releaseAllocation() error = %q, want containing %q", err, test.wantError)
			}
			if !reflect.DeepEqual(nodes, before) {
				t.Errorf("releaseAllocation() mutated nodes on error:\ngot:  %#v\nwant: %#v", nodes, before)
			}
		})
	}
}

func TestReleaseAllocationRejectsOverReleaseWithoutPartialMutation(t *testing.T) {
	t.Parallel()

	job := model.Job{
		Name:                "work",
		Replicas:            2,
		ResourcesPerReplica: model.Resources{CPU: 2, GPU: 1},
	}
	nodes := []nodeState{
		{name: "node-a", capacity: model.Resources{CPU: 8, GPU: 4}, remaining: model.Resources{CPU: 6, GPU: 3}},
		{name: "node-b", capacity: model.Resources{CPU: 8, GPU: 4}, remaining: model.Resources{CPU: 7, GPU: 4}},
	}
	before := append([]nodeState(nil), nodes...)
	err := releaseAllocation(nodes, job, []Allocation{
		{NodeName: "node-a", Replicas: 1},
		{NodeName: "node-b", Replicas: 1},
	})
	if err == nil {
		t.Fatal("releaseAllocation() error = nil, want over-release error")
	}
	if !strings.Contains(err.Error(), "exceeds consumed") {
		t.Errorf("releaseAllocation() error = %q, want over-release detail", err)
	}
	if !reflect.DeepEqual(nodes, before) {
		t.Errorf("releaseAllocation() partially mutated nodes:\ngot:  %#v\nwant: %#v", nodes, before)
	}
}
