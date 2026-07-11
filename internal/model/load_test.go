package model_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Rionlyu/topoqueue/internal/model"
)

func TestLoadCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contents    string
		want        model.Cluster
		wantErrText string
	}{
		{
			name: "valid",
			contents: `nodes:
  - name: node-a
    topology:
      rack: rack-a
    capacity:
      cpu: 32
      gpu: 4
`,
			want: model.Cluster{Nodes: []model.Node{{
				Name: "node-a", Topology: map[string]string{"rack": "rack-a"},
				Capacity: model.Resources{CPU: 32, GPU: 4},
			}}},
		},
		{
			name:     "explicit empty sequence",
			contents: "nodes: []\n",
			want:     model.Cluster{Nodes: []model.Node{}},
		},
		{
			name:        "missing nodes field",
			contents:    "{}\n",
			wantErrText: `top-level field "nodes" is required and must be a sequence`,
		},
		{
			name:        "null document",
			contents:    "null\n",
			wantErrText: `top-level field "nodes" is required and must be a sequence`,
		},
		{
			name:        "null nodes field",
			contents:    "nodes: null\n",
			wantErrText: `top-level field "nodes" is required and must be a sequence`,
		},
		{
			name: "unknown root field",
			contents: `nodes: []
version: 1
`,
			wantErrText: "field version not found",
		},
		{
			name: "unknown nested field",
			contents: `nodes:
  - name: node-a
    topology: {}
    capacity:
      cpu: 1
      gpu: 0
      memory: 8
`,
			wantErrText: "field memory not found",
		},
		{
			name: "multiple documents",
			contents: `nodes: []
---
nodes: []
`,
			wantErrText: "multiple YAML documents",
		},
		{
			name:        "empty file",
			contents:    "",
			wantErrText: "file is empty",
		},
		{
			name: "validation includes object",
			contents: `nodes:
  - name: node-a
    capacity:
      cpu: -1
      gpu: 0
`,
			wantErrText: `node "node-a": capacity.cpu must be non-negative`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := writeTempFile(t, "cluster.yaml", test.contents)
			got, err := model.LoadCluster(path)
			if test.wantErrText != "" {
				if err == nil {
					t.Fatalf("LoadCluster() error = nil, want containing %q", test.wantErrText)
				}
				if !strings.Contains(err.Error(), path) {
					t.Errorf("LoadCluster() error = %q, want file path %q", err, path)
				}
				if !strings.Contains(err.Error(), test.wantErrText) {
					t.Errorf("LoadCluster() error = %q, want containing %q", err, test.wantErrText)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadCluster() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("LoadCluster() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestLoadJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contents    string
		want        model.JobSet
		wantErrText string
	}{
		{
			name: "preserves order",
			contents: `jobs:
  - name: first
    replicas: 2
    resourcesPerReplica:
      cpu: 1
      gpu: 0
  - name: second
    replicas: 1
    resourcesPerReplica:
      cpu: 0
      gpu: 1
    requiredTopology: rack
`,
			want: model.JobSet{Jobs: []model.Job{
				{Name: "first", Replicas: 2, ResourcesPerReplica: model.Resources{CPU: 1}},
				{Name: "second", Replicas: 1, ResourcesPerReplica: model.Resources{GPU: 1}, RequiredTopology: "rack"},
			}},
		},
		{
			name:     "explicit empty sequence",
			contents: "jobs: []\n",
			want:     model.JobSet{Jobs: []model.Job{}},
		},
		{
			name:        "missing jobs field",
			contents:    "{}\n",
			wantErrText: `top-level field "jobs" is required and must be a sequence`,
		},
		{
			name:        "null document",
			contents:    "null\n",
			wantErrText: `top-level field "jobs" is required and must be a sequence`,
		},
		{
			name:        "null jobs field",
			contents:    "jobs: null\n",
			wantErrText: `top-level field "jobs" is required and must be a sequence`,
		},
		{
			name: "unknown job field",
			contents: `jobs:
  - name: job-a
    replicas: 1
    priority: 10
    resourcesPerReplica:
      cpu: 1
      gpu: 0
`,
			wantErrText: "field priority not found",
		},
		{
			name: "validation includes object",
			contents: `jobs:
  - name: job-a
    replicas: 0
    resourcesPerReplica:
      cpu: 1
      gpu: 0
`,
			wantErrText: `job "job-a": replicas must be greater than zero`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := writeTempFile(t, "jobs.yaml", test.contents)
			got, err := model.LoadJobs(path)
			if test.wantErrText != "" {
				if err == nil {
					t.Fatalf("LoadJobs() error = nil, want containing %q", test.wantErrText)
				}
				if !strings.Contains(err.Error(), path) {
					t.Errorf("LoadJobs() error = %q, want file path %q", err, path)
				}
				if !strings.Contains(err.Error(), test.wantErrText) {
					t.Errorf("LoadJobs() error = %q, want containing %q", err, test.wantErrText)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadJobs() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("LoadJobs() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestLoadMissingFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "missing.yaml")
	_, err := model.LoadCluster(path)
	if err == nil {
		t.Fatal("LoadCluster() error = nil, want missing-file error")
	}
	for _, text := range []string{"read cluster file", path} {
		if !strings.Contains(err.Error(), text) {
			t.Errorf("LoadCluster() error = %q, want containing %q", err, text)
		}
	}
}

func writeTempFile(t *testing.T, name, contents string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return path
}
