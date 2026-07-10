package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Rionlyu/topoqueue/internal/model"
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
	return string(contents[start : start+end]) + "\n"
}
