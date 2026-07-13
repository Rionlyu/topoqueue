package model_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Rionlyu/topoqueue/internal/model"
)

func TestLoadTimedJobsPreservesOrder(t *testing.T) {
	t.Parallel()

	path := writeTempFile(t, "timed-jobs.yaml", `jobs:
  - name: first
    arrivalTick: 0
    durationTicks: 8
    replicas: 2
    resourcesPerReplica:
      cpu: 1
      gpu: 0
  - name: second
    arrivalTick: 3
    durationTicks: 2
    replicas: 1
    resourcesPerReplica:
      cpu: 0
      gpu: 1
    requiredTopology: rack
`)

	want := model.TimedJobSet{Jobs: []model.TimedJob{
		{
			Job: model.Job{
				Name: "first", Replicas: 2,
				ResourcesPerReplica: model.Resources{CPU: 1},
			},
			ArrivalTick: 0, DurationTicks: 8,
		},
		{
			Job: model.Job{
				Name: "second", Replicas: 1,
				ResourcesPerReplica: model.Resources{GPU: 1},
				RequiredTopology:    "rack",
			},
			ArrivalTick: 3, DurationTicks: 2,
		},
	}}

	got, err := model.LoadTimedJobs(path)
	if err != nil {
		t.Fatalf("LoadTimedJobs() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("LoadTimedJobs() = %#v, want %#v", got, want)
	}
}

func TestLoadTimedJobsAcceptsExplicitEmptySequence(t *testing.T) {
	t.Parallel()

	path := writeTempFile(t, "timed-jobs.yaml", "jobs: []\n")
	got, err := model.LoadTimedJobs(path)
	if err != nil {
		t.Fatalf("LoadTimedJobs() error = %v", err)
	}
	if !reflect.DeepEqual(got, model.TimedJobSet{Jobs: []model.TimedJob{}}) {
		t.Errorf("LoadTimedJobs() = %#v, want explicit empty job sequence", got)
	}
}

func TestLoadTimedJobsRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contents    string
		wantErrText string
	}{
		{
			name:        "missing jobs field",
			contents:    "{}\n",
			wantErrText: `top-level field "jobs" is required and must be a sequence`,
		},
		{
			name:        "null jobs field",
			contents:    "jobs: null\n",
			wantErrText: `top-level field "jobs" is required and must be a sequence`,
		},
		{
			name: "missing arrival tick",
			contents: `jobs:
  - name: job-a
    durationTicks: 1
    replicas: 1
    resourcesPerReplica: {cpu: 1, gpu: 0}
`,
			wantErrText: "arrivalTick is required",
		},
		{
			name: "missing duration",
			contents: `jobs:
  - name: job-a
    arrivalTick: 0
    replicas: 1
    resourcesPerReplica: {cpu: 1, gpu: 0}
`,
			wantErrText: "durationTicks is required",
		},
		{
			name: "negative arrival",
			contents: `jobs:
  - name: job-a
    arrivalTick: -1
    durationTicks: 1
    replicas: 1
    resourcesPerReplica: {cpu: 1, gpu: 0}
`,
			wantErrText: "arrivalTick must be non-negative",
		},
		{
			name: "zero duration",
			contents: `jobs:
  - name: job-a
    arrivalTick: 0
    durationTicks: 0
    replicas: 1
    resourcesPerReplica: {cpu: 1, gpu: 0}
`,
			wantErrText: "durationTicks must be greater than zero",
		},
		{
			name: "negative duration",
			contents: `jobs:
  - name: job-a
    arrivalTick: 0
    durationTicks: -1
    replicas: 1
    resourcesPerReplica: {cpu: 1, gpu: 0}
`,
			wantErrText: "durationTicks must be greater than zero",
		},
		{
			name: "duplicate names",
			contents: `jobs:
  - name: duplicate
    arrivalTick: 0
    durationTicks: 1
    replicas: 1
    resourcesPerReplica: {cpu: 1, gpu: 0}
  - name: duplicate
    arrivalTick: 1
    durationTicks: 1
    replicas: 1
    resourcesPerReplica: {cpu: 1, gpu: 0}
`,
			wantErrText: `job "duplicate" at index 1: duplicate name`,
		},
		{
			name: "unknown root field",
			contents: `jobs: []
version: 1
`,
			wantErrText: "field version not found",
		},
		{
			name: "unknown job field",
			contents: `jobs:
  - name: job-a
    arrivalTick: 0
    durationTicks: 1
    replicas: 1
    priority: 10
    resourcesPerReplica: {cpu: 1, gpu: 0}
`,
			wantErrText: "field priority not found",
		},
		{
			name: "unknown resource field",
			contents: `jobs:
  - name: job-a
    arrivalTick: 0
    durationTicks: 1
    replicas: 1
    resourcesPerReplica:
      cpu: 1
      gpu: 0
      memory: 8
`,
			wantErrText: "field memory not found",
		},
		{
			name: "multiple documents",
			contents: `jobs: []
---
jobs: []
`,
			wantErrText: "multiple YAML documents",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := writeTempFile(t, "timed-jobs.yaml", test.contents)
			_, err := model.LoadTimedJobs(path)
			if err == nil {
				t.Fatalf("LoadTimedJobs() error = nil, want containing %q", test.wantErrText)
			}
			if !strings.Contains(err.Error(), path) {
				t.Errorf("LoadTimedJobs() error = %q, want file path %q", err, path)
			}
			if !strings.Contains(err.Error(), test.wantErrText) {
				t.Errorf("LoadTimedJobs() error = %q, want containing %q", err, test.wantErrText)
			}
		})
	}
}

func TestValidateTimedJobsReusesStaticValidation(t *testing.T) {
	t.Parallel()

	jobs := model.TimedJobSet{Jobs: []model.TimedJob{
		{
			Job: model.Job{
				Name: "job-a", Replicas: 1,
				ResourcesPerReplica: model.Resources{CPU: 1},
			},
			DurationTicks: 1,
		},
		{
			Job: model.Job{
				Name: "job-a", Replicas: 1,
				ResourcesPerReplica: model.Resources{CPU: 1},
			},
			DurationTicks: 1,
		},
	}}

	err := model.ValidateTimedJobs(jobs)
	if err == nil || !strings.Contains(err.Error(), "duplicate name") {
		t.Fatalf("ValidateTimedJobs() error = %v, want duplicate-name error", err)
	}
}

func TestStaticLoadJobsRejectsTimedFields(t *testing.T) {
	t.Parallel()

	path := writeTempFile(t, "jobs.yaml", `jobs:
  - name: job-a
    arrivalTick: 0
    durationTicks: 1
    replicas: 1
    resourcesPerReplica: {cpu: 1, gpu: 0}
`)
	_, err := model.LoadJobs(path)
	if err == nil || !strings.Contains(err.Error(), "field arrivalTick not found") {
		t.Fatalf("LoadJobs() error = %v, want rejection of timed fields", err)
	}
}

func TestTimedJobSetCloneAndConversionAreIndependent(t *testing.T) {
	t.Parallel()

	original := model.TimedJobSet{Jobs: []model.TimedJob{{
		Job: model.Job{
			Name: "job-a", Replicas: 1,
			ResourcesPerReplica: model.Resources{CPU: 2},
		},
		ArrivalTick: 1, DurationTicks: 3,
	}}}

	clone := original.Clone()
	static := original.StaticJobs()
	clone.Jobs[0].Name = "clone"
	clone.Jobs[0].ResourcesPerReplica.CPU = 4
	static.Jobs[0].Name = "static"
	static.Jobs[0].ResourcesPerReplica.CPU = 8

	if original.Jobs[0].Name != "job-a" || original.Jobs[0].ResourcesPerReplica.CPU != 2 {
		t.Fatalf("copy mutation changed original: %#v", original)
	}
	if clone.Jobs[0].Name != "clone" || clone.Jobs[0].ResourcesPerReplica.CPU != 4 {
		t.Errorf("clone mutation was not retained: %#v", clone)
	}
	if static.Jobs[0].Name != "static" || static.Jobs[0].ResourcesPerReplica.CPU != 8 {
		t.Errorf("converted job mutation was not retained: %#v", static)
	}
}
