package scheduler_test

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/Rionlyu/topoqueue/internal/model"
	"github.com/Rionlyu/topoqueue/internal/scheduler"
)

func TestSimulateTimedExamplePolicies(t *testing.T) {
	t.Parallel()

	cluster, jobs := timedExampleInputs()
	tests := []struct {
		name            string
		policy          scheduler.Policy
		wantEvents      []string
		wantTicks       [][3]int64
		wantCompleted   int
		wantUnscheduled int
		wantEnd         int64
		wantMakespan    int64
		wantTotalWait   int64
		wantAverageWait float64
		wantMaxWait     int64
		wantBatchDomain string
	}{
		{
			name:   "strict FIFO",
			policy: scheduler.PolicyStrictFIFO,
			wantEvents: []string{
				"0 arrived warmup", "0 admitted warmup", "1 arrived train-xl",
				"2 arrived batch", "8 completed warmup", "8 admitted train-xl",
				"12 completed train-xl", "12 admitted batch", "14 completed batch",
			},
			wantTicks:       [][3]int64{{0, 8, 0}, {8, 12, 7}, {12, 14, 10}},
			wantCompleted:   3,
			wantEnd:         14,
			wantMakespan:    14,
			wantTotalWait:   17,
			wantAverageWait: float64(17) / 3,
			wantMaxWait:     10,
			wantBatchDomain: "rack-a",
		},
		{
			name:   "backfill",
			policy: scheduler.PolicyBackfill,
			wantEvents: []string{
				"0 arrived warmup", "0 admitted warmup", "1 arrived train-xl",
				"2 arrived batch", "2 admitted batch", "4 completed batch",
				"8 completed warmup", "8 admitted train-xl", "12 completed train-xl",
			},
			wantTicks:       [][3]int64{{0, 8, 0}, {8, 12, 7}, {2, 4, 0}},
			wantCompleted:   3,
			wantEnd:         12,
			wantMakespan:    12,
			wantTotalWait:   7,
			wantAverageWait: float64(7) / 3,
			wantMaxWait:     7,
			wantBatchDomain: "rack-b",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			result := mustSimulate(t, cluster, jobs, test.policy)
			if got := eventSignatures(result.Events); !reflect.DeepEqual(got, test.wantEvents) {
				t.Fatalf("events = %#v, want %#v", got, test.wantEvents)
			}
			if result.CompletedJobCount != test.wantCompleted || result.UnscheduledJobCount != test.wantUnscheduled {
				t.Errorf("counts = completed %d, unscheduled %d; want %d, %d", result.CompletedJobCount, result.UnscheduledJobCount, test.wantCompleted, test.wantUnscheduled)
			}
			if result.StartTick != 0 || result.EndTick != test.wantEnd || result.MakespanTicks != test.wantMakespan {
				t.Errorf("ticks = start %d, end %d, makespan %d; want 0, %d, %d", result.StartTick, result.EndTick, result.MakespanTicks, test.wantEnd, test.wantMakespan)
			}
			if result.TotalQueueDelayTicks != test.wantTotalWait || result.AverageQueueDelayTicks != test.wantAverageWait || result.MaximumQueueDelayTicks != test.wantMaxWait {
				t.Errorf("wait metrics = total %d, average %v, max %d; want %d, %v, %d", result.TotalQueueDelayTicks, result.AverageQueueDelayTicks, result.MaximumQueueDelayTicks, test.wantTotalWait, test.wantAverageWait, test.wantMaxWait)
			}
			for index, lifecycle := range result.Jobs {
				if lifecycle.Status != scheduler.LifecycleCompleted {
					t.Errorf("job %q status = %q, want completed", lifecycle.JobName, lifecycle.Status)
				}
				assertLifecycleTicks(t, lifecycle, test.wantTicks[index])
			}
			if got := result.Jobs[0].Allocation; !reflect.DeepEqual(got, []scheduler.Allocation{{NodeName: "node-a1", Replicas: 4}, {NodeName: "node-a2", Replicas: 4}}) {
				t.Errorf("warmup allocation = %#v", got)
			}
			warmupAdmission := result.Events[1]
			if warmupAdmission.Type != scheduler.EventAdmitted || warmupAdmission.SelectedTopologyDomain != "zone-a" || !reflect.DeepEqual(
				warmupAdmission.Allocation,
				[]scheduler.Allocation{{NodeName: "node-a1", Replicas: 4}, {NodeName: "node-a2", Replicas: 4}},
			) {
				t.Errorf("warmup admission event payload = %#v", warmupAdmission)
			}
			if result.Jobs[2].SelectedTopologyDomain != test.wantBatchDomain {
				t.Errorf("batch domain = %q, want %q", result.Jobs[2].SelectedTopologyDomain, test.wantBatchDomain)
			}
		})
	}
}

func TestSimulateProcessesCompletionBeforeSameTickArrivalAndAdmission(t *testing.T) {
	t.Parallel()

	cluster := model.Cluster{Nodes: []model.Node{{
		Name: "node-a", Capacity: model.Resources{CPU: 1, GPU: 1},
	}}}
	jobs := model.TimedJobSet{Jobs: []model.TimedJob{
		timedJob("first", 0, 2, 1, 1, 1, ""),
		timedJob("second", 2, 1, 1, 1, 1, ""),
	}}
	result := mustSimulate(t, cluster, jobs, scheduler.PolicyStrictFIFO)
	want := []string{
		"0 arrived first", "0 admitted first",
		"2 completed first", "2 arrived second", "2 admitted second",
		"3 completed second",
	}
	if got := eventSignatures(result.Events); !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %#v, want %#v", got, want)
	}
	assertLifecycleTicks(t, result.Jobs[1], [3]int64{2, 3, 0})
}

func TestSimulateReleasesAllSameTickCompletionsBeforeAdmission(t *testing.T) {
	t.Parallel()

	cluster := model.Cluster{Nodes: []model.Node{
		{Name: "node-a", Capacity: model.Resources{GPU: 1}},
		{Name: "node-b", Capacity: model.Resources{GPU: 1}},
	}}
	jobs := model.TimedJobSet{Jobs: []model.TimedJob{
		timedJob("first", 0, 2, 1, 0, 1, ""),
		timedJob("second", 0, 2, 1, 0, 1, ""),
		timedJob("whole-cluster", 1, 1, 2, 0, 1, ""),
	}}
	result := mustSimulate(t, cluster, jobs, scheduler.PolicyStrictFIFO)
	wantAtTwo := []string{
		"2 completed first",
		"2 completed second",
		"2 admitted whole-cluster",
	}
	gotEvents := eventSignatures(result.Events)
	if got := gotEvents[5:8]; !reflect.DeepEqual(got, wantAtTwo) {
		t.Fatalf("tick-two events = %#v, want %#v; all events: %#v", got, wantAtTwo, gotEvents)
	}
	if got := result.Jobs[2].Allocation; !reflect.DeepEqual(got, []scheduler.Allocation{
		{NodeName: "node-a", Replicas: 1},
		{NodeName: "node-b", Replicas: 1},
	}) {
		t.Errorf("whole-cluster allocation = %#v", got)
	}
}

func TestSimulateOrdersArrivalsAndCompletionsDeterministically(t *testing.T) {
	t.Parallel()

	cluster := model.Cluster{Nodes: []model.Node{
		{Name: "node-a", Capacity: model.Resources{GPU: 1}},
		{Name: "node-b", Capacity: model.Resources{GPU: 1}},
	}}

	t.Run("chronological arrivals and completion input index", func(t *testing.T) {
		jobs := model.TimedJobSet{Jobs: []model.TimedJob{
			timedJob("first-index", 1, 2, 1, 0, 1, ""),
			timedJob("second-index", 0, 3, 1, 0, 1, ""),
		}}
		result := mustSimulate(t, cluster, jobs, scheduler.PolicyBackfill)
		want := []string{
			"0 arrived second-index", "0 admitted second-index",
			"1 arrived first-index", "1 admitted first-index",
			"3 completed first-index", "3 completed second-index",
		}
		if got := eventSignatures(result.Events); !reflect.DeepEqual(got, want) {
			t.Fatalf("events = %#v, want %#v", got, want)
		}
	})

	t.Run("simultaneous arrivals retain input order", func(t *testing.T) {
		jobs := model.TimedJobSet{Jobs: []model.TimedJob{
			timedJob("z-job", 0, 1, 1, 0, 1, ""),
			timedJob("a-job", 0, 1, 1, 0, 1, ""),
		}}
		result := mustSimulate(t, cluster, jobs, scheduler.PolicyBackfill)
		wantPrefix := []string{
			"0 arrived z-job", "0 arrived a-job",
			"0 admitted z-job", "0 admitted a-job",
		}
		if got := eventSignatures(result.Events)[:4]; !reflect.DeepEqual(got, wantPrefix) {
			t.Fatalf("event prefix = %#v, want %#v", got, wantPrefix)
		}
	})
}

func TestSimulateTerminalPolicySemantics(t *testing.T) {
	t.Parallel()

	cluster := model.Cluster{Nodes: []model.Node{{Name: "node-a", Capacity: model.Resources{GPU: 1}}}}
	jobs := model.TimedJobSet{Jobs: []model.TimedJob{
		timedJob("impossible", 0, 1, 2, 0, 1, ""),
		timedJob("feasible", 0, 1, 1, 0, 1, ""),
	}}

	strict := mustSimulate(t, cluster, jobs, scheduler.PolicyStrictFIFO)
	if strict.CompletedJobCount != 0 || strict.UnscheduledJobCount != 2 {
		t.Fatalf("strict counts = completed %d, unscheduled %d", strict.CompletedJobCount, strict.UnscheduledJobCount)
	}
	if strict.Jobs[0].Reason == nil || strict.Jobs[0].Reason.Code != scheduler.ReasonInsufficientCapacity {
		t.Errorf("strict head reason = %#v", strict.Jobs[0].Reason)
	}
	if strict.Jobs[1].Reason == nil || strict.Jobs[1].Reason.Code != scheduler.ReasonHeadOfLineBlocked || strict.Jobs[1].Reason.BlockingJob != "impossible" {
		t.Errorf("strict follower reason = %#v", strict.Jobs[1].Reason)
	}

	backfill := mustSimulate(t, cluster, jobs, scheduler.PolicyBackfill)
	if backfill.CompletedJobCount != 1 || backfill.UnscheduledJobCount != 1 {
		t.Fatalf("backfill counts = completed %d, unscheduled %d", backfill.CompletedJobCount, backfill.UnscheduledJobCount)
	}
	if backfill.Jobs[0].Status != scheduler.LifecycleUnscheduled || backfill.Jobs[0].Reason == nil || backfill.Jobs[0].Reason.Code != scheduler.ReasonInsufficientCapacity {
		t.Errorf("backfill head lifecycle = %#v", backfill.Jobs[0])
	}
	if backfill.Jobs[1].Status != scheduler.LifecycleCompleted {
		t.Errorf("backfill follower lifecycle = %#v", backfill.Jobs[1])
	}
}

func TestSimulateRecomputesFinalReasonAfterAllResourcesAreReleased(t *testing.T) {
	t.Parallel()

	cluster := model.Cluster{Nodes: []model.Node{
		{Name: "node-a", Topology: map[string]string{"rack": "rack-a"}, Capacity: model.Resources{GPU: 1}},
		{Name: "node-b", Topology: map[string]string{"rack": "rack-b"}, Capacity: model.Resources{GPU: 1}},
	}}
	jobs := model.TimedJobSet{Jobs: []model.TimedJob{
		timedJob("running", 0, 2, 1, 0, 1, "rack"),
		timedJob("fragmented", 0, 1, 2, 0, 1, "rack"),
	}}

	result := mustSimulate(t, cluster, jobs, scheduler.PolicyBackfill)
	blocked := result.Jobs[1]
	if blocked.Status != scheduler.LifecycleUnscheduled || blocked.Reason == nil {
		t.Fatalf("terminal lifecycle = %#v", blocked)
	}
	if blocked.Reason.Code != scheduler.ReasonTopologyFragmented {
		t.Errorf("final reason = %#v, want topology fragmentation after release", blocked.Reason)
	}
	if blocked.Reason.TotalAvailableSlots == nil || *blocked.Reason.TotalAvailableSlots != 2 || blocked.Reason.LargestDomainSlots == nil || *blocked.Reason.LargestDomainSlots != 1 {
		t.Errorf("final topology capacity details = %#v", blocked.Reason)
	}
}

func TestSimulateEmptyInput(t *testing.T) {
	t.Parallel()

	result := mustSimulate(t, model.Cluster{}, model.TimedJobSet{}, scheduler.PolicyBackfill)
	if result.Events == nil || result.Jobs == nil {
		t.Fatalf("empty result slices must be non-nil: %#v", result)
	}
	want := scheduler.SimulationResult{
		Policy: scheduler.PolicyBackfill,
		Events: []scheduler.SimulationEvent{},
		Jobs:   []scheduler.JobLifecycle{},
	}
	if !reflect.DeepEqual(result, want) {
		t.Errorf("empty result = %#v, want %#v", result, want)
	}
}

func TestSimulateChecksFinishTickOverflowOnlyForFittingJobs(t *testing.T) {
	t.Parallel()

	cluster := model.Cluster{Nodes: []model.Node{{Name: "node-a", Capacity: model.Resources{GPU: 1}}}}
	overflowing := model.TimedJobSet{Jobs: []model.TimedJob{
		timedJob("blocker", 0, math.MaxInt64, 1, 0, 1, ""),
		timedJob("target", 1, 1, 1, 0, 1, ""),
	}}
	_, err := scheduler.Simulate(context.Background(), cluster, overflowing, scheduler.PolicyStrictFIFO)
	if err == nil || !strings.Contains(err.Error(), "target") || !strings.Contains(err.Error(), "finish tick exceeds") {
		t.Fatalf("Simulate() overflow error = %v", err)
	}

	lateImpossible := model.TimedJobSet{Jobs: []model.TimedJob{
		timedJob("impossible", math.MaxInt64, 1, 2, 0, 1, ""),
	}}
	result := mustSimulate(t, cluster, lateImpossible, scheduler.PolicyBackfill)
	if result.Jobs[0].Status != scheduler.LifecycleUnscheduled || result.StartTick != math.MaxInt64 || result.EndTick != math.MaxInt64 || result.MakespanTicks != 0 {
		t.Errorf("late impossible result = %#v", result)
	}

	finishesAtMaximum := model.TimedJobSet{Jobs: []model.TimedJob{
		timedJob("maximum", math.MaxInt64-1, 1, 1, 0, 1, ""),
	}}
	result = mustSimulate(t, cluster, finishesAtMaximum, scheduler.PolicyBackfill)
	if result.EndTick != math.MaxInt64 || result.MakespanTicks != 1 || result.Jobs[0].Status != scheduler.LifecycleCompleted {
		t.Errorf("maximum-tick result = %#v", result)
	}
}

func TestSimulateRejectsAggregateQueueDelayOverflow(t *testing.T) {
	t.Parallel()

	cluster := model.Cluster{Nodes: []model.Node{{Name: "node-a", Capacity: model.Resources{GPU: 2}}}}
	jobs := model.TimedJobSet{Jobs: []model.TimedJob{
		timedJob("blocker", 0, math.MaxInt64-1, 2, 0, 1, ""),
		timedJob("waiter-a", 0, 1, 1, 0, 1, ""),
		timedJob("waiter-b", 0, 1, 1, 0, 1, ""),
	}}
	_, err := scheduler.Simulate(context.Background(), cluster, jobs, scheduler.PolicyBackfill)
	if err == nil || !strings.Contains(err.Error(), "total queue delay exceeds") {
		t.Fatalf("Simulate() aggregate wait error = %v", err)
	}
}

func TestSimulateDoesNotMutateInputsAndIsRepeatable(t *testing.T) {
	t.Parallel()

	cluster, jobs := timedExampleInputs()
	wantCluster := cluster.Clone()
	wantJobs := jobs.Clone()
	want := mustSimulate(t, cluster, jobs, scheduler.PolicyBackfill)
	for iteration := 0; iteration < 50; iteration++ {
		got := mustSimulate(t, cluster, jobs, scheduler.PolicyBackfill)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration %d differs:\ngot:  %#v\nwant: %#v", iteration, got, want)
		}
	}
	if !reflect.DeepEqual(cluster, wantCluster) {
		t.Errorf("Simulate() mutated cluster: got %#v, want %#v", cluster, wantCluster)
	}
	if !reflect.DeepEqual(jobs, wantJobs) {
		t.Errorf("Simulate() mutated timed jobs: got %#v, want %#v", jobs, wantJobs)
	}
}

func TestSimulateHonorsCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cluster, jobs := timedExampleInputs()
	_, err := scheduler.Simulate(ctx, cluster, jobs, scheduler.PolicyBackfill)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Simulate() error = %v, want context canceled", err)
	}
}

func TestSimulateHonorsCancellationDuringActiveWork(t *testing.T) {
	t.Parallel()

	cluster, jobs := timedExampleInputs()
	ctx := &cancelAfterChecksContext{Context: context.Background(), cancelAt: 6}
	_, err := scheduler.Simulate(ctx, cluster, jobs, scheduler.PolicyBackfill)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Simulate() error = %v after %d checks, want context canceled", err, ctx.checks)
	}
	if ctx.checks < ctx.cancelAt {
		t.Errorf("context was not checked through active placement: got %d checks, want at least %d", ctx.checks, ctx.cancelAt)
	}
}

func TestSimulateRejectsInvalidTimedInputs(t *testing.T) {
	t.Parallel()

	validCluster := model.Cluster{Nodes: []model.Node{{Name: "node-a", Capacity: model.Resources{GPU: 1}}}}
	validJobs := model.TimedJobSet{Jobs: []model.TimedJob{
		timedJob("work", 0, 1, 1, 0, 1, ""),
	}}
	tests := []struct {
		name        string
		cluster     model.Cluster
		jobs        model.TimedJobSet
		policy      scheduler.Policy
		wantMessage string
	}{
		{
			name: "negative arrival", cluster: validCluster,
			jobs:   model.TimedJobSet{Jobs: []model.TimedJob{timedJob("work", -1, 1, 1, 0, 1, "")}},
			policy: scheduler.PolicyBackfill, wantMessage: "arrivalTick must be non-negative",
		},
		{
			name: "zero duration", cluster: validCluster,
			jobs:   model.TimedJobSet{Jobs: []model.TimedJob{timedJob("work", 0, 0, 1, 0, 1, "")}},
			policy: scheduler.PolicyBackfill, wantMessage: "durationTicks must be greater than zero",
		},
		{
			name: "missing topology label",
			cluster: model.Cluster{Nodes: []model.Node{{
				Name: "node-a", Topology: map[string]string{"zone": "zone-a"}, Capacity: model.Resources{GPU: 1},
			}}},
			jobs:   model.TimedJobSet{Jobs: []model.TimedJob{timedJob("work", 0, 1, 1, 0, 1, "rack")}},
			policy: scheduler.PolicyBackfill, wantMessage: `missing topology key "rack"`,
		},
		{
			name: "unsupported policy", cluster: validCluster, jobs: validJobs,
			policy: scheduler.Policy("all"), wantMessage: `unsupported policy "all"`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := scheduler.Simulate(context.Background(), test.cluster, test.jobs, test.policy)
			if err == nil || !strings.Contains(err.Error(), test.wantMessage) {
				t.Fatalf("Simulate() error = %v, want containing %q", err, test.wantMessage)
			}
		})
	}

	if _, err := scheduler.Simulate(nil, validCluster, validJobs, scheduler.PolicyBackfill); err == nil || !strings.Contains(err.Error(), "nil context") {
		t.Fatalf("Simulate(nil) error = %v, want nil-context error", err)
	}
}

func assertLifecycleTicks(t *testing.T, lifecycle scheduler.JobLifecycle, want [3]int64) {
	t.Helper()
	if lifecycle.StartTick == nil || lifecycle.CompletionTick == nil || lifecycle.QueueDelayTicks == nil {
		t.Fatalf("job %q lifecycle ticks = start %v, completion %v, delay %v", lifecycle.JobName, lifecycle.StartTick, lifecycle.CompletionTick, lifecycle.QueueDelayTicks)
	}
	got := [3]int64{*lifecycle.StartTick, *lifecycle.CompletionTick, *lifecycle.QueueDelayTicks}
	if got != want {
		t.Errorf("job %q lifecycle ticks = %v, want %v", lifecycle.JobName, got, want)
	}
}

func eventSignatures(events []scheduler.SimulationEvent) []string {
	signatures := make([]string, len(events))
	for index, event := range events {
		signatures[index] = strings.Join([]string{
			strconv.FormatInt(event.Tick, 10), string(event.Type), event.JobName,
		}, " ")
	}
	return signatures
}

func mustSimulate(t *testing.T, cluster model.Cluster, jobs model.TimedJobSet, policy scheduler.Policy) scheduler.SimulationResult {
	t.Helper()
	result, err := scheduler.Simulate(context.Background(), cluster, jobs, policy)
	if err != nil {
		t.Fatalf("Simulate() error = %v", err)
	}
	return result
}

func timedExampleInputs() (model.Cluster, model.TimedJobSet) {
	cluster := model.Cluster{Nodes: []model.Node{
		node("node-a1", "rack-a", 32, 4),
		node("node-a2", "rack-a", 32, 4),
		node("node-b1", "rack-b", 32, 4),
		node("node-b2", "rack-b", 32, 4),
	}}
	jobs := model.TimedJobSet{Jobs: []model.TimedJob{
		timedJob("warmup", 0, 8, 8, 4, 1, "zone"),
		timedJob("train-xl", 1, 4, 16, 4, 1, "zone"),
		timedJob("batch", 2, 2, 4, 4, 1, "rack"),
	}}
	return cluster, jobs
}

func timedJob(name string, arrival, duration int64, replicas int, cpu, gpu int64, topology string) model.TimedJob {
	return model.TimedJob{
		Job: model.Job{
			Name:                name,
			Replicas:            replicas,
			ResourcesPerReplica: model.Resources{CPU: cpu, GPU: gpu},
			RequiredTopology:    topology,
		},
		ArrivalTick:   arrival,
		DurationTicks: duration,
	}
}

type cancelAfterChecksContext struct {
	context.Context
	cancelAt int
	checks   int
}

func (c *cancelAfterChecksContext) Err() error {
	c.checks++
	if c.checks >= c.cancelAt {
		return context.Canceled
	}
	return c.Context.Err()
}
