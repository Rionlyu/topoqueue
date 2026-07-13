# Event-driven simulation

TopoQueue's `simulate` command models job lifecycles on a fixed cluster using
logical integer ticks. It reuses the same deterministic, topology-aware
placement rules as `schedule`, but adds arrivals, queueing, fixed-duration
execution, completion, and resource release.

```sh
topoqueue simulate \
  --cluster examples/cluster.yaml \
  --jobs examples/timed-jobs.yaml \
  --policy all \
  --output text
```

The supported policies are `strict-fifo`, `backfill`, and the CLI-only value
`all`. The default is `strict-fifo`. With `all`, TopoQueue evaluates both real
policies on independent state and orders their results by policy name.

## Timed workload input

Timed workloads use a separate strict loader and do not change the meaning of
the static jobs accepted by `schedule` and `compare`:

```yaml
jobs:
  - name: warmup
    arrivalTick: 0
    durationTicks: 8
    replicas: 8
    resourcesPerReplica:
      cpu: 4
      gpu: 1
    requiredTopology: zone
```

`arrivalTick` is required and must be non-negative. `durationTicks` is required
and must be greater than zero. The existing job rules still apply: names are
non-empty and unique, replicas are positive, CPU and GPU requests are
non-negative with at least one positive resource, and checked resource totals
must fit in a signed 64-bit integer. Every node must have a non-empty value for
each topology key required by the workload.

Decoding rejects unknown fields, missing top-level `jobs`, null collections,
and multiple YAML documents. Static `LoadJobs` continues to reject timed-only
fields such as `arrivalTick` and `durationTicks`.

## Logical ticks and event ordering

Ticks are discrete values, not wall-clock time. The simulator uses no timers,
sleeps, randomness, or goroutines per job. It advances directly to the next
arrival or completion and does not poll intermediate ticks.

At each event tick, processing order is exactly:

1. Complete all jobs finishing at that tick and release their resources.
2. Enqueue all jobs arriving at that tick.
3. Run one admission cycle for the selected policy.

Simultaneous completions are ordered by original workload index. Simultaneous
arrivals also preserve original input order. Running jobs are ordered by
`(finishTick, originalInputIndex)`, so heap behavior cannot change the emitted
timeline. Completion before arrival means a newly arriving job can immediately
use resources released at the same tick.

The timeline emits `arrived`, `admitted`, and `completed` events. Admission
events also contain the selected topology domain and the exact node allocation.

## Queue policies over time

Strict FIFO examines the pending head. If it fits, it is admitted and the new
head is examined. If it does not fit, the admission cycle stops immediately;
the head and every later job retain their queue positions. They are reconsidered
at the next arrival or completion event.

Backfill scans the pending queue once in order. Every job that currently fits
is admitted, while failures remain pending in their existing relative order.
The scan continues after a failure. One pass is sufficient because admission
only consumes resources and cannot make another job newly schedulable.

Neither policy reserves capacity for a pending job. Backfill can therefore
reduce idle capacity and queue delay, but it provides no fairness guarantee.

## Completion, release, and terminal jobs

Admission records how many replicas were placed on each node. Completion
releases exactly `replicas * resourcesPerReplica` for each recorded allocation,
not merely an aggregate estimate. A release is validated in full before any
node is changed: nodes must be known and unique, replica accounting must be
exact, arithmetic must be safe, and remaining resources may not exceed the
node's original capacity.

The simulation terminates when there are no future arrivals and no running
jobs. Any jobs still pending become `unscheduled`; an impossible job cannot
cause an infinite loop.

- Backfill gives every remaining job its actual final placement rejection
  reason against the fully released cluster.
- Strict FIFO gives the pending head its actual final reason. Every later job
  receives `head_of_line_blocked`, naming that head job.

Final reasons distinguish cluster-wide insufficient capacity from topology
fragmentation, just as static placement does.

## Results and metrics

A policy result contains three views of the same run:

- an ordered event timeline;
- one terminal lifecycle per job in original input order;
- aggregate policy metrics.

A completed lifecycle contains arrival, duration, start, completion, queue
delay, topology domain, allocation, and requested resources. An unscheduled
lifecycle has no start, completion, or queue-delay tick and includes its final
rejection reason. Pointer-valued tick fields in JSON preserve the distinction
between an absent value and the meaningful tick zero.

Metrics are defined as follows:

- `startTick` is the earliest processed event tick.
- `endTick` is the last processed event tick.
- `makespanTicks = endTick - startTick`.
- A completed job's `queueDelayTicks = startTick - arrivalTick`.
- `totalQueueDelayTicks` is the sum of queue delays for completed jobs only.
- `averageQueueDelayTicks` is that exact total divided by completed job count;
  it is zero when no job completes.
- `maximumQueueDelayTicks` is the largest completed-job delay and is zero when
  no job completes.

Unscheduled jobs do not contribute to wait aggregates. Empty workloads have
zero counts and metrics and empty event and lifecycle lists. Integer source
metrics remain exact; only human-readable average display is rounded.

Text output presents a summary, timeline, and lifecycle table. JSON uses the
same declared result structure, is indented, and ends with a newline. A policy
comparison first shows comparative completion and wait metrics, then each
policy's full details in deterministic policy-name order.

## Determinism

Identical inputs produce identical decisions and byte-stable output. TopoQueue
uses original input indexes for event ties, lexical names for deterministic
topology and node ties, sorted allocations, ordered lifecycle slices, and
independent mutable state for concurrent policy runs. Map iteration and
goroutine completion order do not determine output order.

## Complexity

Let `J` be the number of jobs, `N` the number of nodes, `D` the number of
topology domains, and `P = O(N log N + D log D)` the worst-case cost of one
placement attempt. There are at most `2J` distinct arrival/completion event
ticks.

Strict FIFO performs each successful admission once and at most one failed
head attempt per event tick, for `O(J * P + J log J)` worst-case time including
arrival sorting and the running-job heap. Backfill may rescan up to `J` pending
jobs at each event tick, giving `O(J^2 * P + J log J)` worst-case time. State,
results, heap entries, and placement scratch space require `O(N + J + D)`
space. Comparing both policies concurrently changes constant factors but not
these asymptotic bounds.

## Cancellation and checked arithmetic

Simulation and comparison accept `context.Context` and return contextual
cancellation errors at checked work boundaries. Independent comparison runs
are canceled together and do not share mutable state.

Input validation checks resource multiplication and aggregate cluster
capacity. At admission, `startTick + durationTicks` is checked before a running
job is recorded; a late job that cannot fit does not fail merely because it
would overflow if admitted. Aggregate queue-delay addition and resource release
are also checked. Arithmetic errors stop the run rather than wrapping values.

## Scope and limitations

TopoQueue is an educational simulator over a supplied snapshot. It is not a
Kubernetes scheduler, does not implement Kueue semantics, and is not intended
for production scheduling. Resources are whole integer CPU and GPU units;
replicas are identical; each job has one fixed duration and at most one required
topology key.

The simulator deliberately does not implement priorities, preemption, elastic
jobs, execution failures or restart policies, multiple queues, quotas, fair
sharing, reservations, live cluster watches, controllers, HTTP services,
persistence, databases, a UI, or real-time execution.
