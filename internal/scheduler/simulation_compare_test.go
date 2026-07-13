package scheduler_test

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/Rionlyu/topoqueue/internal/model"
	"github.com/Rionlyu/topoqueue/internal/scheduler"
)

func TestCompareSimulationsUsesIndependentStateAndStableOrder(t *testing.T) {
	t.Parallel()

	cluster, jobs := timedExampleInputs()
	originalCluster := cluster.Clone()
	originalJobs := jobs.Clone()

	results, err := scheduler.CompareSimulations(context.Background(), cluster, jobs)
	if err != nil {
		t.Fatalf("CompareSimulations() error = %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}
	if results[0].Policy != scheduler.PolicyBackfill || results[1].Policy != scheduler.PolicyStrictFIFO {
		t.Fatalf("policy order = %q, %q; want backfill, strict-fifo", results[0].Policy, results[1].Policy)
	}

	want := []scheduler.SimulationResult{
		mustSimulate(t, cluster, jobs, scheduler.PolicyBackfill),
		mustSimulate(t, cluster, jobs, scheduler.PolicyStrictFIFO),
	}
	if !reflect.DeepEqual(results, want) {
		t.Errorf("CompareSimulations() results differ from independent simulations")
	}
	if !reflect.DeepEqual(cluster, originalCluster) {
		t.Errorf("CompareSimulations() mutated cluster: got %#v, want %#v", cluster, originalCluster)
	}
	if !reflect.DeepEqual(jobs, originalJobs) {
		t.Errorf("CompareSimulations() mutated timed jobs: got %#v, want %#v", jobs, originalJobs)
	}
}

func TestCompareSimulationsIsRepeatable(t *testing.T) {
	t.Parallel()

	cluster, jobs := timedExampleInputs()
	want, err := scheduler.CompareSimulations(context.Background(), cluster, jobs)
	if err != nil {
		t.Fatalf("CompareSimulations() error = %v", err)
	}
	for iteration := 0; iteration < 50; iteration++ {
		got, err := scheduler.CompareSimulations(context.Background(), cluster, jobs)
		if err != nil {
			t.Fatalf("iteration %d: CompareSimulations() error = %v", iteration, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration %d differs:\ngot:  %#v\nwant: %#v", iteration, got, want)
		}
	}
}

func TestCompareSimulationsSelectsRuntimeErrorsDeterministically(t *testing.T) {
	t.Parallel()

	cluster := model.Cluster{Nodes: []model.Node{{
		Name: "node-a", Capacity: model.Resources{GPU: 1},
	}}}
	jobs := model.TimedJobSet{Jobs: []model.TimedJob{
		timedJob("overflow", math.MaxInt64, 1, 1, 0, 1, ""),
	}}
	const want = `compare simulation policy "backfill": simulate policy "backfill": tick 9223372036854775807 admit job "overflow": finish tick exceeds 9223372036854775807 for duration 1`

	for iteration := 0; iteration < 1_000; iteration++ {
		_, err := scheduler.CompareSimulations(context.Background(), cluster, jobs)
		if err == nil || err.Error() != want {
			t.Fatalf("iteration %d: CompareSimulations() error = %q, want %q", iteration, err, want)
		}
	}
}

func TestCompareSimulationsRejectsNilAndCanceledContexts(t *testing.T) {
	t.Parallel()

	cluster, jobs := timedExampleInputs()
	_, err := scheduler.CompareSimulations(nil, cluster, jobs)
	if err == nil || !strings.Contains(err.Error(), "nil context") {
		t.Fatalf("CompareSimulations(nil) error = %v, want nil-context error", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = scheduler.CompareSimulations(ctx, cluster, jobs)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("CompareSimulations(canceled) error = %v, want context.Canceled", err)
	}
}
