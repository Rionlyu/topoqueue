package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Rionlyu/topoqueue/internal/model"
	"github.com/Rionlyu/topoqueue/internal/output"
	"github.com/Rionlyu/topoqueue/internal/scheduler"
)

func TestRunExamples(t *testing.T) {
	t.Parallel()

	clusterPath, jobsPath := examplePaths()
	t.Run("compare text", func(t *testing.T) {
		t.Parallel()
		stdout, stderr, err := execute(t, []string{
			"compare", "--cluster", clusterPath, "--jobs", jobsPath, "--output", "text",
		})
		if err != nil {
			t.Fatalf("run() error = %v; stderr = %s", err, stderr)
		}
		if stderr != "" {
			t.Errorf("stderr = %q, want empty", stderr)
		}
		normalized := strings.Join(strings.Fields(stdout), " ")
		wanted := "POLICY ADMITTED PENDING GPU USED HEAD-OF-LINE BLOCKED backfill 2 1 8/16 0 strict-fifo 1 2 4/16 1"
		if !strings.Contains(normalized, wanted) {
			t.Errorf("comparison summary missing %q:\n%s", wanted, stdout)
		}
		if strings.Index(stdout, "backfill") >= strings.Index(stdout, "strict-fifo") {
			t.Errorf("policies are not rendered in deterministic name order:\n%s", stdout)
		}
		if want := readREADMEExample(t); stdout != want {
			t.Errorf("comparison output differs from README example:\ngot:\n%s\nwant:\n%s", stdout, want)
		}
	})

	t.Run("schedule JSON", func(t *testing.T) {
		t.Parallel()
		stdout, stderr, err := execute(t, []string{
			"schedule", "--cluster", clusterPath, "--jobs", jobsPath,
			"--policy", "backfill", "--output", "json",
		})
		if err != nil {
			t.Fatalf("run() error = %v; stderr = %s", err, stderr)
		}
		if stderr != "" {
			t.Errorf("stderr = %q, want empty", stderr)
		}
		var got scheduler.PolicyResult
		if err := json.Unmarshal([]byte(stdout), &got); err != nil {
			t.Fatalf("decode schedule JSON: %v\n%s", err, stdout)
		}
		if got.Policy != scheduler.PolicyBackfill {
			t.Errorf("policy = %q, want %q", got.Policy, scheduler.PolicyBackfill)
		}
		if got.AdmittedJobCount != 2 || got.PendingJobCount != 1 || got.HeadOfLineBlockedJobCount != 0 {
			t.Errorf("counts = admitted %d, pending %d, blocked %d; want 2, 1, 0", got.AdmittedJobCount, got.PendingJobCount, got.HeadOfLineBlockedJobCount)
		}
		if got.UsedResources != (model.Resources{CPU: 32, GPU: 8}) {
			t.Errorf("used resources = %+v, want CPU 32 and GPU 8", got.UsedResources)
		}
		if len(got.Jobs) != 3 || got.Jobs[0].Status != scheduler.StatusAdmitted || got.Jobs[1].Reason == nil || got.Jobs[1].Reason.Code != scheduler.ReasonTopologyFragmented || got.Jobs[2].Status != scheduler.StatusAdmitted {
			t.Errorf("unexpected ordered job decisions: %#v", got.Jobs)
		}
	})
}

func TestRunRejectsInvalidArguments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "command required", args: nil, wantErr: "a command is required"},
		{name: "schedule cluster required", args: []string{"schedule"}, wantErr: "--cluster is required"},
		{name: "compare jobs required", args: []string{"compare", "--cluster", "cluster.yaml"}, wantErr: "--jobs is required"},
		{name: "simulate cluster required", args: []string{"simulate"}, wantErr: "--cluster is required"},
		{name: "simulate jobs required", args: []string{"simulate", "--cluster", "cluster.yaml"}, wantErr: "--jobs is required"},
		{
			name:    "unsupported policy",
			args:    []string{"schedule", "--cluster", "cluster.yaml", "--jobs", "jobs.yaml", "--policy", "largest-first"},
			wantErr: `unsupported policy "largest-first"`,
		},
		{
			name:    "unsupported schedule output",
			args:    []string{"schedule", "--cluster", "cluster.yaml", "--jobs", "jobs.yaml", "--output", "yaml"},
			wantErr: `unsupported output "yaml"`,
		},
		{
			name:    "unsupported compare output",
			args:    []string{"compare", "--cluster", "cluster.yaml", "--jobs", "jobs.yaml", "--output", "yaml"},
			wantErr: `unsupported output "yaml"`,
		},
		{
			name:    "unsupported simulation policy",
			args:    []string{"simulate", "--cluster", "cluster.yaml", "--jobs", "jobs.yaml", "--policy", "largest-first"},
			wantErr: `unsupported policy "largest-first"`,
		},
		{
			name:    "unsupported simulation output",
			args:    []string{"simulate", "--cluster", "cluster.yaml", "--jobs", "jobs.yaml", "--output", "yaml"},
			wantErr: `unsupported output "yaml"`,
		},
		{
			name:    "simulation positional arguments",
			args:    []string{"simulate", "workload", "--cluster", "cluster.yaml", "--jobs", "jobs.yaml"},
			wantErr: "unexpected positional arguments",
		},
		{name: "unknown command", args: []string{"admit"}, wantErr: `unknown command "admit"`},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := execute(t, test.args)
			if err == nil {
				t.Fatalf("run() error = nil, want containing %q", test.wantErr)
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Errorf("run() error = %q, want containing %q", err, test.wantErr)
			}
		})
	}
}

func TestRunSimulateModes(t *testing.T) {
	t.Parallel()

	clusterPath, _ := examplePaths()
	timedJobsPath := writeCLIFile(t, "timed-jobs.yaml", canonicalTimedJobsYAML)

	t.Run("default strict FIFO JSON", func(t *testing.T) {
		stdout, stderr, err := execute(t, []string{
			"simulate", "--cluster", clusterPath, "--jobs", timedJobsPath, "--output", "json",
		})
		if err != nil {
			t.Fatalf("run() error = %v; stderr = %s", err, stderr)
		}
		var result scheduler.SimulationResult
		if err := json.Unmarshal([]byte(stdout), &result); err != nil {
			t.Fatalf("decode simulation JSON: %v\n%s", err, stdout)
		}
		if result.Policy != scheduler.PolicyStrictFIFO || result.MakespanTicks != 14 || result.TotalQueueDelayTicks != 17 {
			t.Errorf("strict simulation summary = %#v", result)
		}
	})

	t.Run("explicit backfill text", func(t *testing.T) {
		stdout, stderr, err := execute(t, []string{
			"simulate", "--cluster", clusterPath, "--jobs", timedJobsPath,
			"--policy", "backfill", "--output", "text",
		})
		if err != nil {
			t.Fatalf("run() error = %v; stderr = %s", err, stderr)
		}
		for _, wanted := range []string{"backfill", "EVENT TIMELINE", "JOB LIFECYCLE", "2.33"} {
			if !strings.Contains(stdout, wanted) {
				t.Errorf("simulation text missing %q:\n%s", wanted, stdout)
			}
		}
	})

	t.Run("all policies JSON", func(t *testing.T) {
		stdout, stderr, err := execute(t, []string{
			"simulate", "--cluster", clusterPath, "--jobs", timedJobsPath,
			"--policy", "all", "--output", "json",
		})
		if err != nil {
			t.Fatalf("run() error = %v; stderr = %s", err, stderr)
		}
		var comparison output.SimulationComparisonJSON
		if err := json.Unmarshal([]byte(stdout), &comparison); err != nil {
			t.Fatalf("decode simulation comparison JSON: %v\n%s", err, stdout)
		}
		if len(comparison.Policies) != 2 || comparison.Policies[0].Policy != scheduler.PolicyBackfill || comparison.Policies[1].Policy != scheduler.PolicyStrictFIFO {
			t.Errorf("comparison policies = %#v", comparison.Policies)
		}
	})

	t.Run("all policies text", func(t *testing.T) {
		stdout, stderr, err := execute(t, []string{
			"simulate", "--cluster", clusterPath, "--jobs", timedJobsPath,
			"--policy", "all", "--output", "text",
		})
		if err != nil {
			t.Fatalf("run() error = %v; stderr = %s", err, stderr)
		}
		if strings.Count(stdout, "EVENT TIMELINE") != 2 || !strings.Contains(stdout, "2.33") || !strings.Contains(stdout, "5.67") {
			t.Errorf("unexpected all-policy text:\n%s", stdout)
		}
	})
}

func TestRunSimulateHelpAndCancellation(t *testing.T) {
	t.Parallel()

	stdout, stderr, err := execute(t, []string{"help"})
	if err != nil || stderr != "" || !strings.Contains(stdout, "simulate") {
		t.Fatalf("root help = stdout %q, stderr %q, error %v", stdout, stderr, err)
	}
	stdout, stderr, err = execute(t, []string{"simulate", "-h"})
	if err != nil || stdout != "" {
		t.Fatalf("simulate help = stdout %q, stderr %q, error %v", stdout, stderr, err)
	}
	for _, flagName := range []string{"-cluster", "-jobs", "-policy", "-output"} {
		if !strings.Contains(stderr, flagName) {
			t.Errorf("simulate help missing %q:\n%s", flagName, stderr)
		}
	}

	clusterPath, _ := examplePaths()
	timedJobsPath := writeCLIFile(t, "timed-jobs.yaml", canonicalTimedJobsYAML)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var canceledStdout, canceledStderr bytes.Buffer
	err = run(ctx, []string{"simulate", "--cluster", clusterPath, "--jobs", timedJobsPath}, &canceledStdout, &canceledStderr)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled simulate error = %v, want context.Canceled", err)
	}
}

func TestRunKeepsStaticAndTimedLoadersSeparate(t *testing.T) {
	t.Parallel()

	clusterPath, staticJobsPath := examplePaths()
	timedJobsPath := writeCLIFile(t, "timed-jobs.yaml", canonicalTimedJobsYAML)
	_, _, err := execute(t, []string{
		"schedule", "--cluster", clusterPath, "--jobs", timedJobsPath,
	})
	if err == nil || !strings.Contains(err.Error(), "field arrivalTick not found") {
		t.Fatalf("schedule timed workload error = %v", err)
	}
	_, _, err = execute(t, []string{
		"simulate", "--cluster", clusterPath, "--jobs", staticJobsPath,
	})
	if err == nil || !strings.Contains(err.Error(), "arrivalTick is required") {
		t.Fatalf("simulate static workload error = %v", err)
	}
	if _, err := scheduler.ParsePolicy("all"); err == nil {
		t.Fatal("scheduler.ParsePolicy(all) error = nil, want all to remain CLI-only")
	}
}

func TestRunRejectsUnknownYAMLField(t *testing.T) {
	t.Parallel()

	clusterPath := filepath.Join(t.TempDir(), "cluster.yaml")
	contents := []byte("nodes: []\nunknown: true\n")
	if err := os.WriteFile(clusterPath, contents, 0o600); err != nil {
		t.Fatalf("write cluster fixture: %v", err)
	}
	_, jobsPath := examplePaths()
	_, _, err := execute(t, []string{
		"schedule", "--cluster", clusterPath, "--jobs", jobsPath,
	})
	if err == nil {
		t.Fatal("run() error = nil, want strict YAML error")
	}
	for _, wanted := range []string{clusterPath, "field unknown not found"} {
		if !strings.Contains(err.Error(), wanted) {
			t.Errorf("run() error = %q, want containing %q", err, wanted)
		}
	}
}

func execute(t *testing.T, args []string) (string, string, error) {
	t.Helper()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run(context.Background(), args, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

func examplePaths() (string, string) {
	return filepath.Join("..", "..", "examples", "cluster.yaml"), filepath.Join("..", "..", "examples", "jobs.yaml")
}

func readREADMEExample(t *testing.T) string {
	t.Helper()

	contents, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	if err != nil {
		t.Fatalf("read README example: %v", err)
	}
	const startMarker = "This is the output of `make demo` using the checked-in example files:\n\n```text\n"
	start := strings.Index(string(contents), startMarker)
	if start == -1 {
		t.Fatal("README comparison example start marker not found")
	}
	start += len(startMarker)
	end := strings.Index(string(contents[start:]), "\n```")
	if end == -1 {
		t.Fatal("README comparison example closing fence not found")
	}
	return string(contents[start:start+end]) + "\n"
}

func writeCLIFile(t *testing.T, name, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write CLI fixture: %v", err)
	}
	return path
}

const canonicalTimedJobsYAML = `jobs:
  - name: warmup
    arrivalTick: 0
    durationTicks: 8
    replicas: 8
    resourcesPerReplica: {cpu: 4, gpu: 1}
    requiredTopology: zone
  - name: train-xl
    arrivalTick: 1
    durationTicks: 4
    replicas: 16
    resourcesPerReplica: {cpu: 4, gpu: 1}
    requiredTopology: zone
  - name: batch
    arrivalTick: 2
    durationTicks: 2
    replicas: 4
    resourcesPerReplica: {cpu: 4, gpu: 1}
    requiredTopology: rack
`
