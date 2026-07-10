package model_test

import (
	"math"
	"strings"
	"testing"

	"github.com/Rionlyu/topoqueue/internal/model"
)

func TestValidateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cluster     model.Cluster
		wantErrText string
	}{
		{name: "empty cluster is valid", cluster: model.Cluster{}},
		{
			name: "valid",
			cluster: model.Cluster{Nodes: []model.Node{
				{Name: "node-a", Capacity: model.Resources{CPU: 1}},
				{Name: "node-b", Capacity: model.Resources{GPU: 1}},
			}},
		},
		{
			name:        "empty name",
			cluster:     model.Cluster{Nodes: []model.Node{{Capacity: model.Resources{CPU: 1}}}},
			wantErrText: "node at index 0: name must not be empty",
		},
		{
			name: "duplicate name",
			cluster: model.Cluster{Nodes: []model.Node{
				{Name: "node-a"}, {Name: "node-a"},
			}},
			wantErrText: `node "node-a" at index 1: duplicate name`,
		},
		{
			name:        "negative CPU",
			cluster:     model.Cluster{Nodes: []model.Node{{Name: "node-a", Capacity: model.Resources{CPU: -1}}}},
			wantErrText: `node "node-a": capacity.cpu must be non-negative`,
		},
		{
			name:        "negative GPU",
			cluster:     model.Cluster{Nodes: []model.Node{{Name: "node-a", Capacity: model.Resources{GPU: -1}}}},
			wantErrText: `node "node-a": capacity.gpu must be non-negative`,
		},
		{
			name: "aggregate capacity overflow",
			cluster: model.Cluster{Nodes: []model.Node{
				{Name: "node-a", Capacity: model.Resources{CPU: math.MaxInt64}},
				{Name: "node-b", Capacity: model.Resources{CPU: 1}},
			}},
			wantErrText: `node "node-b": cluster total capacity.cpu exceeds`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assertErrorContains(t, model.ValidateCluster(test.cluster), test.wantErrText)
		})
	}
}

func TestValidateJobs(t *testing.T) {
	t.Parallel()

	validJob := model.Job{Name: "job-a", Replicas: 1, ResourcesPerReplica: model.Resources{GPU: 1}}
	tests := []struct {
		name        string
		jobs        model.JobSet
		wantErrText string
	}{
		{name: "empty queue is valid", jobs: model.JobSet{}},
		{name: "valid", jobs: model.JobSet{Jobs: []model.Job{validJob}}},
		{
			name: "empty name",
			jobs: model.JobSet{Jobs: []model.Job{{
				Replicas: 1, ResourcesPerReplica: model.Resources{CPU: 1},
			}}},
			wantErrText: "job at index 0: name must not be empty",
		},
		{
			name: "duplicate name",
			jobs: model.JobSet{Jobs: []model.Job{
				validJob,
				{Name: "job-a", Replicas: 2, ResourcesPerReplica: model.Resources{CPU: 1}},
			}},
			wantErrText: `job "job-a" at index 1: duplicate name`,
		},
		{
			name: "zero replicas",
			jobs: model.JobSet{Jobs: []model.Job{{
				Name: "job-a", Replicas: 0, ResourcesPerReplica: model.Resources{CPU: 1},
			}}},
			wantErrText: `job "job-a": replicas must be greater than zero`,
		},
		{
			name: "negative replicas",
			jobs: model.JobSet{Jobs: []model.Job{{
				Name: "job-a", Replicas: -1, ResourcesPerReplica: model.Resources{CPU: 1},
			}}},
			wantErrText: `job "job-a": replicas must be greater than zero`,
		},
		{
			name: "negative CPU",
			jobs: model.JobSet{Jobs: []model.Job{{
				Name: "job-a", Replicas: 1, ResourcesPerReplica: model.Resources{CPU: -1, GPU: 1},
			}}},
			wantErrText: `job "job-a": resourcesPerReplica.cpu must be non-negative`,
		},
		{
			name: "negative GPU",
			jobs: model.JobSet{Jobs: []model.Job{{
				Name: "job-a", Replicas: 1, ResourcesPerReplica: model.Resources{CPU: 1, GPU: -1},
			}}},
			wantErrText: `job "job-a": resourcesPerReplica.gpu must be non-negative`,
		},
		{
			name: "no requested resources",
			jobs: model.JobSet{Jobs: []model.Job{{
				Name: "job-a", Replicas: 1,
			}}},
			wantErrText: `job "job-a": at least one resource per replica must be positive`,
		},
		{
			name: "total request overflow",
			jobs: model.JobSet{Jobs: []model.Job{{
				Name: "job-a", Replicas: 2, ResourcesPerReplica: model.Resources{CPU: math.MaxInt64},
			}}},
			wantErrText: `job "job-a": total requested CPU exceeds`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assertErrorContains(t, model.ValidateJobs(test.jobs), test.wantErrText)
		})
	}
}

func TestValidateTopologyRequirements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cluster     model.Cluster
		jobs        model.JobSet
		wantErrText string
	}{
		{
			name: "all required keys exist",
			cluster: model.Cluster{Nodes: []model.Node{
				{Name: "node-a", Topology: map[string]string{"rack": "rack-a"}},
				{Name: "node-b", Topology: map[string]string{"rack": "rack-b"}},
			}},
			jobs: model.JobSet{Jobs: []model.Job{{Name: "job-a", RequiredTopology: "rack"}}},
		},
		{
			name: "jobs without requirement need no labels",
			cluster: model.Cluster{Nodes: []model.Node{
				{Name: "node-a"},
			}},
			jobs: model.JobSet{Jobs: []model.Job{{Name: "job-a"}}},
		},
		{
			name: "missing required key",
			cluster: model.Cluster{Nodes: []model.Node{
				{Name: "node-a", Topology: map[string]string{"zone": "zone-a"}},
			}},
			jobs:        model.JobSet{Jobs: []model.Job{{Name: "training", RequiredTopology: "rack"}}},
			wantErrText: `node "node-a": missing topology key "rack" required by job "training"`,
		},
		{
			name: "empty required topology value",
			cluster: model.Cluster{Nodes: []model.Node{
				{Name: "node-a", Topology: map[string]string{"rack": ""}},
			}},
			jobs:        model.JobSet{Jobs: []model.Job{{Name: "training", RequiredTopology: "rack"}}},
			wantErrText: `node "node-a": topology key "rack" required by job "training" must have a non-empty value`,
		},
		{
			name: "keys are checked deterministically",
			cluster: model.Cluster{Nodes: []model.Node{
				{Name: "node-a"},
			}},
			jobs: model.JobSet{Jobs: []model.Job{
				{Name: "zone-job", RequiredTopology: "zone"},
				{Name: "rack-job", RequiredTopology: "rack"},
			}},
			wantErrText: `missing topology key "rack" required by job "rack-job"`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assertErrorContains(t, model.ValidateTopologyRequirements(test.cluster, test.jobs), test.wantErrText)
		})
	}
}

func assertErrorContains(t *testing.T, err error, want string) {
	t.Helper()

	if want == "" {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatalf("error = nil, want containing %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want containing %q", err, want)
	}
}
