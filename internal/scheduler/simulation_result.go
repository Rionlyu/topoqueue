package scheduler

import "github.com/Rionlyu/topoqueue/internal/model"

// SimulationEventType identifies a lifecycle transition in the deterministic
// event timeline.
type SimulationEventType string

const (
	// EventArrived records that a job entered the pending queue.
	EventArrived SimulationEventType = "arrived"
	// EventAdmitted records that a job was allocated resources and began running.
	EventAdmitted SimulationEventType = "admitted"
	// EventCompleted records that a job finished and released its allocation.
	EventCompleted SimulationEventType = "completed"
)

// LifecycleStatus identifies the terminal state of a simulated job.
type LifecycleStatus string

const (
	// LifecycleCompleted means the job was admitted and ran to completion.
	LifecycleCompleted LifecycleStatus = "completed"
	// LifecycleUnscheduled means the simulation ended while the job was pending.
	LifecycleUnscheduled LifecycleStatus = "unscheduled"
)

// SimulationEvent is one ordered entry in a policy simulation timeline.
// Admission events include the selected topology domain and allocation.
type SimulationEvent struct {
	Tick                   int64               `json:"tick"`
	Type                   SimulationEventType `json:"type"`
	JobName                string              `json:"jobName"`
	SelectedTopologyDomain string              `json:"selectedTopologyDomain,omitempty"`
	Allocation             []Allocation        `json:"allocation,omitempty"`
}

// JobLifecycle contains the terminal result and timing data for one timed job.
// Pointer tick fields distinguish an absent value from the meaningful tick zero.
type JobLifecycle struct {
	JobName                string          `json:"jobName"`
	Status                 LifecycleStatus `json:"status"`
	ArrivalTick            int64           `json:"arrivalTick"`
	DurationTicks          int64           `json:"durationTicks"`
	StartTick              *int64          `json:"startTick,omitempty"`
	CompletionTick         *int64          `json:"completionTick,omitempty"`
	QueueDelayTicks        *int64          `json:"queueDelayTicks,omitempty"`
	SelectedTopologyDomain string          `json:"selectedTopologyDomain,omitempty"`
	Allocation             []Allocation    `json:"allocation"`
	RequestedResources     model.Resources `json:"requestedResources"`
	Reason                 *Reason         `json:"reason,omitempty"`
}

// SimulationResult contains a deterministic event timeline, original-order job
// lifecycles, and exact aggregate metrics for one queue policy.
type SimulationResult struct {
	Policy                 Policy            `json:"policy"`
	Events                 []SimulationEvent `json:"events"`
	Jobs                   []JobLifecycle    `json:"jobs"`
	CompletedJobCount      int               `json:"completedJobCount"`
	UnscheduledJobCount    int               `json:"unscheduledJobCount"`
	StartTick              int64             `json:"startTick"`
	EndTick                int64             `json:"endTick"`
	MakespanTicks          int64             `json:"makespanTicks"`
	TotalQueueDelayTicks   int64             `json:"totalQueueDelayTicks"`
	AverageQueueDelayTicks float64           `json:"averageQueueDelayTicks"`
	MaximumQueueDelayTicks int64             `json:"maximumQueueDelayTicks"`
}
