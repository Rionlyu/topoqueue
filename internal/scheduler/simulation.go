package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/Rionlyu/topoqueue/internal/model"
)

// Simulate evaluates a timed workload with one deterministic queue policy.
func Simulate(ctx context.Context, cluster model.Cluster, jobs model.TimedJobSet, policy Policy) (SimulationResult, error) {
	if ctx == nil {
		return SimulationResult{}, fmt.Errorf("simulate policy %q: nil context", policy)
	}
	if _, err := ParsePolicy(string(policy)); err != nil {
		return SimulationResult{}, fmt.Errorf("simulate: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return SimulationResult{}, fmt.Errorf("simulate policy %q: %w", policy, err)
	}
	if err := model.ValidateCluster(cluster); err != nil {
		return SimulationResult{}, fmt.Errorf("simulate policy %q: validate cluster: %w", policy, err)
	}
	if err := model.ValidateTimedJobs(jobs); err != nil {
		return SimulationResult{}, fmt.Errorf("simulate policy %q: validate timed jobs: %w", policy, err)
	}
	if err := model.ValidateTopologyRequirements(cluster, jobs.StaticJobs()); err != nil {
		return SimulationResult{}, fmt.Errorf("simulate policy %q: validate topology requirements: %w", policy, err)
	}

	state := newSimulationState(cluster.Clone(), jobs.Clone(), policy)
	result, err := state.run(ctx)
	if err != nil {
		return SimulationResult{}, fmt.Errorf("simulate policy %q: %w", policy, err)
	}
	return result, nil
}

type simulationState struct {
	policy        Policy
	jobs          model.TimedJobSet
	nodes         []nodeState
	arrivalOrder  []int
	arrivalCursor int
	pending       []int
	running       runningJobHeap
	result        SimulationResult
}

func newSimulationState(cluster model.Cluster, jobs model.TimedJobSet, policy Policy) *simulationState {
	arrivalOrder := make([]int, len(jobs.Jobs))
	lifecycles := make([]JobLifecycle, len(jobs.Jobs))
	for index, job := range jobs.Jobs {
		arrivalOrder[index] = index
		lifecycles[index] = JobLifecycle{
			JobName:            job.Name,
			ArrivalTick:        job.ArrivalTick,
			DurationTicks:      job.DurationTicks,
			Allocation:         make([]Allocation, 0),
			RequestedResources: resourcesForReplicas(job.ResourcesPerReplica, job.Replicas),
		}
	}
	sort.Slice(arrivalOrder, func(i, j int) bool {
		left := jobs.Jobs[arrivalOrder[i]]
		right := jobs.Jobs[arrivalOrder[j]]
		if left.ArrivalTick != right.ArrivalTick {
			return left.ArrivalTick < right.ArrivalTick
		}
		return arrivalOrder[i] < arrivalOrder[j]
	})

	state := &simulationState{
		policy:       policy,
		jobs:         jobs,
		nodes:        makeNodeStates(cluster),
		arrivalOrder: arrivalOrder,
		pending:      make([]int, 0),
		running:      make(runningJobHeap, 0),
		result: SimulationResult{
			Policy: policy,
			Events: make([]SimulationEvent, 0),
			Jobs:   lifecycles,
		},
	}
	heap.Init(&state.running)
	return state
}

func (s *simulationState) run(ctx context.Context) (SimulationResult, error) {
	for s.arrivalCursor < len(s.arrivalOrder) || s.running.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return SimulationResult{}, err
		}
		tick, ok := s.nextEventTick()
		if !ok {
			return SimulationResult{}, fmt.Errorf("event state has future work but no next event")
		}
		if err := s.completeJobs(ctx, tick); err != nil {
			return SimulationResult{}, err
		}
		if err := s.arriveJobs(ctx, tick); err != nil {
			return SimulationResult{}, err
		}
		if err := s.admitJobs(ctx, tick); err != nil {
			return SimulationResult{}, err
		}
	}

	if err := ctx.Err(); err != nil {
		return SimulationResult{}, err
	}
	if err := s.terminalizePending(ctx); err != nil {
		return SimulationResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SimulationResult{}, err
	}
	if len(s.result.Events) > 0 {
		s.result.MakespanTicks = s.result.EndTick - s.result.StartTick
	}
	if s.result.CompletedJobCount > 0 {
		s.result.AverageQueueDelayTicks = float64(s.result.TotalQueueDelayTicks) / float64(s.result.CompletedJobCount)
	}
	if s.result.CompletedJobCount+s.result.UnscheduledJobCount != len(s.jobs.Jobs) {
		return SimulationResult{}, fmt.Errorf(
			"terminal lifecycle count is %d, want %d",
			s.result.CompletedJobCount+s.result.UnscheduledJobCount,
			len(s.jobs.Jobs),
		)
	}
	return s.result, nil
}

func (s *simulationState) nextEventTick() (int64, bool) {
	var tick int64
	found := false
	if s.arrivalCursor < len(s.arrivalOrder) {
		tick = s.jobs.Jobs[s.arrivalOrder[s.arrivalCursor]].ArrivalTick
		found = true
	}
	if s.running.Len() > 0 && (!found || s.running[0].finishTick < tick) {
		tick = s.running[0].finishTick
		found = true
	}
	return tick, found
}

func (s *simulationState) completeJobs(ctx context.Context, tick int64) error {
	for s.running.Len() > 0 && s.running[0].finishTick == tick {
		if err := ctx.Err(); err != nil {
			return err
		}
		running := heap.Pop(&s.running).(runningJob)
		job := s.jobs.Jobs[running.inputIndex]
		if err := releaseAllocation(s.nodes, job.Job, running.allocation); err != nil {
			return fmt.Errorf("tick %d complete job %q: %w", tick, job.Name, err)
		}

		lifecycle := &s.result.Jobs[running.inputIndex]
		if lifecycle.StartTick == nil || lifecycle.QueueDelayTicks == nil {
			return fmt.Errorf("tick %d complete job %q: missing admission lifecycle", tick, job.Name)
		}
		completionTick := tick
		lifecycle.CompletionTick = &completionTick
		lifecycle.Status = LifecycleCompleted

		delay := *lifecycle.QueueDelayTicks
		if delay < 0 || s.result.TotalQueueDelayTicks > math.MaxInt64-delay {
			return fmt.Errorf("tick %d complete job %q: total queue delay exceeds %d", tick, job.Name, int64(math.MaxInt64))
		}
		s.result.TotalQueueDelayTicks += delay
		if delay > s.result.MaximumQueueDelayTicks {
			s.result.MaximumQueueDelayTicks = delay
		}
		s.result.CompletedJobCount++
		s.appendEvent(SimulationEvent{Tick: tick, Type: EventCompleted, JobName: job.Name})
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (s *simulationState) arriveJobs(ctx context.Context, tick int64) error {
	for s.arrivalCursor < len(s.arrivalOrder) {
		inputIndex := s.arrivalOrder[s.arrivalCursor]
		job := s.jobs.Jobs[inputIndex]
		if job.ArrivalTick != tick {
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		s.pending = append(s.pending, inputIndex)
		s.arrivalCursor++
		s.appendEvent(SimulationEvent{Tick: tick, Type: EventArrived, JobName: job.Name})
	}
	return nil
}

func (s *simulationState) admitJobs(ctx context.Context, tick int64) error {
	switch s.policy {
	case PolicyStrictFIFO:
		for len(s.pending) > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			admitted, err := s.tryAdmit(ctx, tick, s.pending[0])
			if err != nil {
				return err
			}
			if !admitted {
				break
			}
			s.pending = s.pending[1:]
		}
	case PolicyBackfill:
		retained := make([]int, 0, len(s.pending))
		for _, inputIndex := range s.pending {
			if err := ctx.Err(); err != nil {
				return err
			}
			admitted, err := s.tryAdmit(ctx, tick, inputIndex)
			if err != nil {
				return err
			}
			if !admitted {
				retained = append(retained, inputIndex)
			}
		}
		s.pending = retained
	default:
		return fmt.Errorf("unsupported policy %q", s.policy)
	}
	return nil
}

func (s *simulationState) tryAdmit(ctx context.Context, tick int64, inputIndex int) (bool, error) {
	job := s.jobs.Jobs[inputIndex]
	if tick < job.ArrivalTick {
		return false, fmt.Errorf("tick %d admit job %q before arrival tick %d", tick, job.Name, job.ArrivalTick)
	}
	placed, err := placeJob(ctx, s.nodes, job.Job)
	if err != nil {
		return false, fmt.Errorf("tick %d place job %q: %w", tick, job.Name, err)
	}
	if placed.reason != nil {
		return false, nil
	}
	if tick > math.MaxInt64-job.DurationTicks {
		if releaseErr := releaseAllocation(s.nodes, job.Job, placed.allocation); releaseErr != nil {
			return false, fmt.Errorf(
				"tick %d admit job %q: finish tick overflow for duration %d; rollback allocation: %w",
				tick,
				job.Name,
				job.DurationTicks,
				releaseErr,
			)
		}
		return false, fmt.Errorf("tick %d admit job %q: finish tick exceeds %d for duration %d", tick, job.Name, int64(math.MaxInt64), job.DurationTicks)
	}
	finishTick := tick + job.DurationTicks
	queueDelay := tick - job.ArrivalTick
	allocation := cloneAllocations(placed.allocation)
	lifecycle := &s.result.Jobs[inputIndex]
	startTick := tick
	lifecycle.StartTick = &startTick
	lifecycle.QueueDelayTicks = &queueDelay
	lifecycle.SelectedTopologyDomain = placed.domain
	lifecycle.Allocation = cloneAllocations(allocation)
	heap.Push(&s.running, runningJob{
		inputIndex: inputIndex,
		finishTick: finishTick,
		allocation: cloneAllocations(allocation),
	})
	s.appendEvent(SimulationEvent{
		Tick:                   tick,
		Type:                   EventAdmitted,
		JobName:                job.Name,
		SelectedTopologyDomain: placed.domain,
		Allocation:             cloneAllocations(allocation),
	})
	return true, nil
}

func (s *simulationState) terminalizePending(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.arrivalCursor != len(s.arrivalOrder) || s.running.Len() != 0 {
		return fmt.Errorf("terminalize pending jobs while future or running work remains")
	}
	for _, node := range s.nodes {
		if err := validateNodeResourceState(node); err != nil {
			return fmt.Errorf("terminal node %q: %w", node.name, err)
		}
		if node.remaining != node.capacity {
			return fmt.Errorf("terminal node %q has remaining %+v, want original capacity %+v", node.name, node.remaining, node.capacity)
		}
	}
	if len(s.pending) == 0 {
		return nil
	}

	switch s.policy {
	case PolicyStrictFIFO:
		headIndex := s.pending[0]
		headReason, err := s.finalPlacementReason(ctx, headIndex)
		if err != nil {
			return err
		}
		s.markUnscheduled(headIndex, headReason)
		blockingJob := s.jobs.Jobs[headIndex].Name
		for _, inputIndex := range s.pending[1:] {
			if err := ctx.Err(); err != nil {
				return err
			}
			s.markUnscheduled(inputIndex, headOfLineReason(blockingJob))
		}
	case PolicyBackfill:
		for _, inputIndex := range s.pending {
			if err := ctx.Err(); err != nil {
				return err
			}
			reason, err := s.finalPlacementReason(ctx, inputIndex)
			if err != nil {
				return err
			}
			s.markUnscheduled(inputIndex, reason)
		}
	default:
		return fmt.Errorf("unsupported policy %q", s.policy)
	}
	return nil
}

func (s *simulationState) finalPlacementReason(ctx context.Context, inputIndex int) (*Reason, error) {
	job := s.jobs.Jobs[inputIndex]
	placed, err := placeJob(ctx, s.nodes, job.Job)
	if err != nil {
		return nil, fmt.Errorf("final placement for job %q: %w", job.Name, err)
	}
	if placed.reason != nil {
		return placed.reason, nil
	}
	if err := releaseAllocation(s.nodes, job.Job, placed.allocation); err != nil {
		return nil, fmt.Errorf("final placement invariant for job %q: unexpectedly fit and rollback failed: %w", job.Name, err)
	}
	return nil, fmt.Errorf("final placement invariant for job %q: unexpectedly fit on an empty cluster", job.Name)
}

func (s *simulationState) markUnscheduled(inputIndex int, reason *Reason) {
	lifecycle := &s.result.Jobs[inputIndex]
	lifecycle.Status = LifecycleUnscheduled
	lifecycle.Reason = reason
	s.result.UnscheduledJobCount++
}

func (s *simulationState) appendEvent(event SimulationEvent) {
	if len(s.result.Events) == 0 {
		s.result.StartTick = event.Tick
	}
	s.result.EndTick = event.Tick
	s.result.Events = append(s.result.Events, event)
}

func cloneAllocations(allocation []Allocation) []Allocation {
	if len(allocation) == 0 {
		return make([]Allocation, 0)
	}
	clone := make([]Allocation, len(allocation))
	copy(clone, allocation)
	return clone
}

type runningJob struct {
	inputIndex int
	finishTick int64
	allocation []Allocation
}

type runningJobHeap []runningJob

func (h runningJobHeap) Len() int { return len(h) }

func (h runningJobHeap) Less(i, j int) bool {
	if h[i].finishTick != h[j].finishTick {
		return h[i].finishTick < h[j].finishTick
	}
	return h[i].inputIndex < h[j].inputIndex
}

func (h runningJobHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *runningJobHeap) Push(value any) {
	*h = append(*h, value.(runningJob))
}

func (h *runningJobHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = runningJob{}
	*h = old[:last]
	return value
}
