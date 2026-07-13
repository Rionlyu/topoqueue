package output_test

import (
	"context"
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

func TestSimulationTextOutputIsStableOrderedAndComplete(t *testing.T) {
	t.Parallel()

	cluster, jobs := simulationInputs()
	strict := mustSimulation(t, cluster, jobs, scheduler.PolicyStrictFIFO)
	backfill := mustSimulation(t, cluster, jobs, scheduler.PolicyBackfill)

	// Exercise the allocation formatter's defensive sorting and prove that
	// rendering does not reorder the result owned by the caller.
	reverseAllocations(strict.Events[1].Allocation)
	reverseAllocations(strict.Jobs[0].Allocation)
	strictBefore := cloneSimulationResult(t, strict)

	strictText := renderRepeatedly(t, func(w io.Writer) error {
		return output.WriteSimulationText(w, strict)
	})
	for _, wanted := range []string{
		"POLICY", "COMPLETED", "UNSCHEDULED", "START", "END", "MAKESPAN",
		"TOTAL WAIT", "AVERAGE WAIT", "MAX WAIT", "strict-fifo", "5.67",
		"EVENT TIMELINE", "TICK", "TYPE", "arrived", "admitted", "completed",
		"JOB LIFECYCLE", "ARRIVAL", "DURATION", "COMPLETION", "REQUESTED CPU",
		"REQUESTED GPU", "warmup", "train-xl", "batch",
		"node-a1=4,node-a2=4",
	} {
		if !strings.Contains(strictText, wanted) {
			t.Errorf("strict simulation text missing %q:\n%s", wanted, strictText)
		}
	}
	wantStrictEvents := []string{
		"0 arrived warmup", "0 admitted warmup", "1 arrived train-xl",
		"2 arrived batch", "8 completed warmup", "8 admitted train-xl",
		"12 completed train-xl", "12 admitted batch", "14 completed batch",
	}
	if got := renderedEventOrder(t, strictText); !reflect.DeepEqual(got, wantStrictEvents) {
		t.Errorf("strict rendered event order = %#v, want %#v", got, wantStrictEvents)
	}
	if got := renderedLifecycleOrder(t, strictText); !reflect.DeepEqual(got, []string{"warmup", "train-xl", "batch"}) {
		t.Errorf("strict rendered lifecycle order = %#v", got)
	}
	if !reflect.DeepEqual(strict, strictBefore) {
		t.Errorf("WriteSimulationText() mutated its result:\ngot:  %#v\nwant: %#v", strict, strictBefore)
	}

	comparisonInput := []scheduler.SimulationResult{strict, backfill}
	comparisonBefore := cloneSimulationResults(t, comparisonInput)
	comparisonText := renderRepeatedly(t, func(w io.Writer) error {
		return output.WriteSimulationComparisonText(w, comparisonInput)
	})
	lines := strings.Split(comparisonText, "\n")
	if len(lines) < 3 {
		t.Fatalf("simulation comparison text is too short:\n%s", comparisonText)
	}
	if got := strings.Fields(lines[1]); len(got) == 0 || got[0] != string(scheduler.PolicyBackfill) {
		t.Errorf("first comparison policy row = %q, want backfill", lines[1])
	}
	if got := strings.Fields(lines[2]); len(got) == 0 || got[0] != string(scheduler.PolicyStrictFIFO) {
		t.Errorf("second comparison policy row = %q, want strict-fifo", lines[2])
	}
	for _, wanted := range []string{"COMPLETED", "MAKESPAN", "AVERAGE WAIT", "MAX WAIT", "2.33", "5.67"} {
		if !strings.Contains(comparisonText, wanted) {
			t.Errorf("simulation comparison text missing %q:\n%s", wanted, comparisonText)
		}
	}
	if strings.Count(comparisonText, "EVENT TIMELINE") != 2 || strings.Count(comparisonText, "JOB LIFECYCLE") != 2 {
		t.Errorf("comparison does not contain both full policy details:\n%s", comparisonText)
	}
	if !reflect.DeepEqual(comparisonInput, comparisonBefore) {
		t.Errorf("WriteSimulationComparisonText() mutated its input")
	}
}

func TestSimulationTextUsesDashesForAbsentValues(t *testing.T) {
	t.Parallel()

	cluster := model.Cluster{Nodes: []model.Node{{
		Name: "node-a", Capacity: model.Resources{GPU: 1},
	}}}
	jobs := model.TimedJobSet{Jobs: []model.TimedJob{{
		Job: model.Job{
			Name: "impossible", Replicas: 2,
			ResourcesPerReplica: model.Resources{GPU: 1},
		},
		DurationTicks: 1,
	}}}
	result := mustSimulation(t, cluster, jobs, scheduler.PolicyBackfill)
	text := renderRepeatedly(t, func(w io.Writer) error {
		return output.WriteSimulationText(w, result)
	})
	normalized := strings.Join(strings.Fields(text), " ")
	wanted := "impossible unscheduled 0 1 - - - - - 0 2 insufficient_capacity:"
	if !strings.Contains(normalized, wanted) {
		t.Errorf("unscheduled lifecycle does not use dashes for absent values; want %q in:\n%s", wanted, text)
	}
}

func TestSimulationJSONOutputRoundTripsStablyWithoutMutation(t *testing.T) {
	t.Parallel()

	cluster, jobs := simulationInputs()
	backfill := mustSimulation(t, cluster, jobs, scheduler.PolicyBackfill)
	strict := mustSimulation(t, cluster, jobs, scheduler.PolicyStrictFIFO)

	backfillBefore := cloneSimulationResult(t, backfill)
	policyJSON := renderRepeatedly(t, func(w io.Writer) error {
		return output.WriteSimulationJSON(w, backfill)
	})
	if !strings.HasSuffix(policyJSON, "\n") || !strings.Contains(policyJSON, "\n  ") {
		t.Errorf("simulation JSON is not indented with a trailing newline:\n%s", policyJSON)
	}
	for _, wanted := range []string{
		`"policy": "backfill"`, `"events": [`, `"type": "admitted"`,
		`"jobs": [`, `"startTick": 2`, `"queueDelayTicks": 7`,
		`"averageQueueDelayTicks": 2.3333333333333335`,
	} {
		if !strings.Contains(policyJSON, wanted) {
			t.Errorf("simulation JSON missing %q:\n%s", wanted, policyJSON)
		}
	}
	var decodedPolicy scheduler.SimulationResult
	if err := json.Unmarshal([]byte(policyJSON), &decodedPolicy); err != nil {
		t.Fatalf("decode simulation JSON: %v", err)
	}
	if !reflect.DeepEqual(decodedPolicy, backfill) {
		t.Errorf("decoded simulation JSON mismatch:\ngot:  %#v\nwant: %#v", decodedPolicy, backfill)
	}
	if !reflect.DeepEqual(backfill, backfillBefore) {
		t.Errorf("WriteSimulationJSON() mutated its result")
	}

	comparisonInput := []scheduler.SimulationResult{strict, backfill}
	comparisonBefore := cloneSimulationResults(t, comparisonInput)
	comparisonJSON := renderRepeatedly(t, func(w io.Writer) error {
		return output.WriteSimulationComparisonJSON(w, comparisonInput)
	})
	var decodedComparison output.SimulationComparisonJSON
	if err := json.Unmarshal([]byte(comparisonJSON), &decodedComparison); err != nil {
		t.Fatalf("decode simulation comparison JSON: %v", err)
	}
	wantComparison := output.SimulationComparisonJSON{Policies: []scheduler.SimulationResult{backfill, strict}}
	if !reflect.DeepEqual(decodedComparison, wantComparison) {
		t.Errorf("decoded comparison JSON mismatch:\ngot:  %#v\nwant: %#v", decodedComparison, wantComparison)
	}
	backfillIndex := strings.Index(comparisonJSON, `"policy": "backfill"`)
	strictIndex := strings.Index(comparisonJSON, `"policy": "strict-fifo"`)
	if backfillIndex < 0 || strictIndex < 0 || backfillIndex >= strictIndex {
		t.Errorf("comparison JSON policies are not sorted:\n%s", comparisonJSON)
	}
	if !strings.HasSuffix(comparisonJSON, "\n") || !strings.Contains(comparisonJSON, "\n  ") {
		t.Errorf("comparison JSON is not indented with a trailing newline:\n%s", comparisonJSON)
	}
	if !reflect.DeepEqual(comparisonInput, comparisonBefore) {
		t.Errorf("WriteSimulationComparisonJSON() mutated its input")
	}
}

func renderRepeatedly(t *testing.T, write func(io.Writer) error) string {
	t.Helper()
	first := render(t, write)
	for iteration := 1; iteration < 3; iteration++ {
		if got := render(t, write); got != first {
			t.Fatalf("render %d differs from the first render:\nfirst:\n%s\ngot:\n%s", iteration+1, first, got)
		}
	}
	return first
}

func renderedEventOrder(t *testing.T, text string) []string {
	t.Helper()
	lines := strings.Split(text, "\n")
	heading := lineIndex(lines, "EVENT TIMELINE")
	if heading < 0 || heading+1 >= len(lines) {
		t.Fatalf("EVENT TIMELINE section missing:\n%s", text)
	}
	events := make([]string, 0)
	for _, line := range lines[heading+2:] {
		if line == "" {
			break
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			t.Fatalf("malformed event line %q", line)
		}
		events = append(events, fmt.Sprintf("%s %s %s", fields[0], fields[1], fields[2]))
	}
	return events
}

func renderedLifecycleOrder(t *testing.T, text string) []string {
	t.Helper()
	lines := strings.Split(text, "\n")
	heading := lineIndex(lines, "JOB LIFECYCLE")
	if heading < 0 || heading+1 >= len(lines) {
		t.Fatalf("JOB LIFECYCLE section missing:\n%s", text)
	}
	jobs := make([]string, 0)
	for _, line := range lines[heading+2:] {
		if line == "" {
			break
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			t.Fatalf("malformed lifecycle line %q", line)
		}
		jobs = append(jobs, fields[0])
	}
	return jobs
}

func lineIndex(lines []string, wanted string) int {
	for index, line := range lines {
		if line == wanted {
			return index
		}
	}
	return -1
}

func cloneSimulationResult(t *testing.T, result scheduler.SimulationResult) scheduler.SimulationResult {
	t.Helper()
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("clone simulation result: encode: %v", err)
	}
	var clone scheduler.SimulationResult
	if err := json.Unmarshal(data, &clone); err != nil {
		t.Fatalf("clone simulation result: decode: %v", err)
	}
	return clone
}

func cloneSimulationResults(t *testing.T, results []scheduler.SimulationResult) []scheduler.SimulationResult {
	t.Helper()
	clones := make([]scheduler.SimulationResult, len(results))
	for index, result := range results {
		clones[index] = cloneSimulationResult(t, result)
	}
	return clones
}

func reverseAllocations(allocations []scheduler.Allocation) {
	for left, right := 0, len(allocations)-1; left < right; left, right = left+1, right-1 {
		allocations[left], allocations[right] = allocations[right], allocations[left]
	}
}

func mustSimulation(
	t *testing.T,
	cluster model.Cluster,
	jobs model.TimedJobSet,
	policy scheduler.Policy,
) scheduler.SimulationResult {
	t.Helper()
	result, err := scheduler.Simulate(context.Background(), cluster, jobs, policy)
	if err != nil {
		t.Fatalf("Simulate() error = %v", err)
	}
	return result
}

func simulationInputs() (model.Cluster, model.TimedJobSet) {
	cluster := model.Cluster{Nodes: []model.Node{
		simulationNode("node-a1", "rack-a"),
		simulationNode("node-a2", "rack-a"),
		simulationNode("node-b1", "rack-b"),
		simulationNode("node-b2", "rack-b"),
	}}
	jobs := model.TimedJobSet{Jobs: []model.TimedJob{
		simulationJob("warmup", 0, 8, 8, "zone"),
		simulationJob("train-xl", 1, 4, 16, "zone"),
		simulationJob("batch", 2, 2, 4, "rack"),
	}}
	return cluster, jobs
}

func simulationNode(name, rack string) model.Node {
	return model.Node{
		Name: name,
		Topology: map[string]string{
			"zone": "zone-a",
			"rack": rack,
		},
		Capacity: model.Resources{CPU: 32, GPU: 4},
	}
}

func simulationJob(name string, arrival, duration int64, replicas int, topology string) model.TimedJob {
	return model.TimedJob{
		Job: model.Job{
			Name:                name,
			Replicas:            replicas,
			ResourcesPerReplica: model.Resources{CPU: 4, GPU: 1},
			RequiredTopology:    topology,
		},
		ArrivalTick:   arrival,
		DurationTicks: duration,
	}
}
