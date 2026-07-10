package output_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/Rionlyu/topoqueue/internal/model"
	"github.com/Rionlyu/topoqueue/internal/output"
	"github.com/Rionlyu/topoqueue/internal/scheduler"
)

func TestTextOutputIsStableAndContainsExpectedFields(t *testing.T) {
	t.Parallel()

	policy := policyFixture(scheduler.PolicyBackfill)
	comparison := []scheduler.PolicyResult{
		policyFixture(scheduler.PolicyStrictFIFO),
		policy,
	}
	tests := []struct {
		name   string
		write  func(io.Writer) error
		wanted []string
	}{
		{
			name:  "policy",
			write: func(w io.Writer) error { return output.WritePolicyText(w, policy) },
			wanted: []string{
				"POLICY", "ADMITTED", "CPU USED", "GPU USED", "backfill", "16/64", "4/8",
				"warmup", "rack-a", "node-a1=4", "train-xl", "topology_fragmented: fragmented",
			},
		},
		{
			name:  "comparison",
			write: func(w io.Writer) error { return output.WriteComparisonText(w, comparison) },
			wanted: []string{
				"HEAD-OF-LINE BLOCKED", "backfill", "strict-fifo", "4/8", "warmup", "train-xl",
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			first := render(t, test.write)
			second := render(t, test.write)
			if first != second {
				t.Fatal("text output differs across identical renders")
			}
			for _, wanted := range test.wanted {
				if !strings.Contains(first, wanted) {
					t.Errorf("text output missing %q:\n%s", wanted, first)
				}
			}
			if test.name == "comparison" && strings.Index(first, "backfill") >= strings.Index(first, "strict-fifo") {
				t.Errorf("comparison policies are not sorted:\n%s", first)
			}
		})
	}
}

func TestJSONOutputIsStableAndStructured(t *testing.T) {
	t.Parallel()

	policy := policyFixture(scheduler.PolicyBackfill)
	strictPolicy := policyFixture(scheduler.PolicyStrictFIFO)
	comparison := output.ComparisonJSON{Policies: []scheduler.PolicyResult{
		policy,
		strictPolicy,
	}}
	tests := []struct {
		name   string
		write  func(io.Writer) error
		decode func([]byte) error
		wanted []string
	}{
		{
			name:  "policy",
			write: func(w io.Writer) error { return output.WritePolicyJSON(w, policy) },
			decode: func(data []byte) error {
				var got scheduler.PolicyResult
				if err := json.Unmarshal(data, &got); err != nil {
					return err
				}
				if !reflect.DeepEqual(got, policy) {
					return fmt.Errorf("got %#v, want %#v", got, policy)
				}
				return nil
			},
			wanted: []string{
				`"policy": "backfill"`, `"jobs": [`, `"allocation": [`, `"reason": {`,
				`"code": "topology_fragmented"`, `"requestedResources": {`, `"allocatedResources": {`,
			},
		},
		{
			name:  "comparison",
			write: func(w io.Writer) error {
				return output.WriteComparisonJSON(w, []scheduler.PolicyResult{strictPolicy, policy})
			},
			decode: func(data []byte) error {
				var got output.ComparisonJSON
				if err := json.Unmarshal(data, &got); err != nil {
					return err
				}
				if !reflect.DeepEqual(got, comparison) {
					return fmt.Errorf("got %#v, want %#v", got, comparison)
				}
				return nil
			},
			wanted: []string{`"policies": [`, `"policy": "backfill"`, `"policy": "strict-fifo"`},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			first := render(t, test.write)
			second := render(t, test.write)
			if first != second {
				t.Fatal("JSON output differs across identical renders")
			}
			if !strings.HasSuffix(first, "\n") || !strings.Contains(first, "\n  ") {
				t.Errorf("JSON output is not indented with a trailing newline:\n%s", first)
			}
			for _, wanted := range test.wanted {
				if !strings.Contains(first, wanted) {
					t.Errorf("JSON output missing %q:\n%s", wanted, first)
				}
			}
			if err := test.decode([]byte(first)); err != nil {
				t.Errorf("decoded JSON mismatch: %v", err)
			}
		})
	}
}

func render(t *testing.T, write func(io.Writer) error) string {
	t.Helper()

	var buffer bytes.Buffer
	if err := write(&buffer); err != nil {
		t.Fatalf("render output: %v", err)
	}
	return buffer.String()
}

func policyFixture(policy scheduler.Policy) scheduler.PolicyResult {
	available := int64(6)
	largest := int64(3)
	return scheduler.PolicyResult{
		Policy: policy,
		Jobs: []scheduler.JobResult{
			{
				JobName:                "warmup",
				Status:                 scheduler.StatusAdmitted,
				SelectedTopologyDomain: "rack-a",
				Allocation:             []scheduler.Allocation{{NodeName: "node-a1", Replicas: 4}},
				RequestedResources:     model.Resources{CPU: 16, GPU: 4},
				AllocatedResources:     model.Resources{CPU: 16, GPU: 4},
			},
			{
				JobName:            "train-xl",
				Status:             scheduler.StatusPending,
				Allocation:         []scheduler.Allocation{},
				RequestedResources: model.Resources{CPU: 40, GPU: 10},
				Reason: &scheduler.Reason{
					Code:                scheduler.ReasonTopologyFragmented,
					Message:             "fragmented",
					RequestedReplicas:   10,
					TopologyKey:         "rack",
					TotalAvailableSlots: &available,
					LargestDomain:       "rack-a",
					LargestDomainSlots:  &largest,
				},
			},
		},
		AdmittedJobCount:          1,
		PendingJobCount:           1,
		HeadOfLineBlockedJobCount: 0,
		UsedResources:             model.Resources{CPU: 16, GPU: 4},
		TotalResources:            model.Resources{CPU: 64, GPU: 8},
	}
}
