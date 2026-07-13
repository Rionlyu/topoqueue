# Design

TopoQueue evaluates a fixed cluster snapshot with either an ordered static job
list or a timed workload. It is a small teaching tool, so the implementation
favors explicit state transitions and deterministic results over scheduler
extensibility.

## Simulation execution

Each policy simulation is single-threaded. Admission changes the remaining
capacity seen by later jobs, and processing that sequence in one goroutine
makes queue order and state ownership explicit. A run contains no internal
parallel placement work.

The `compare` command may evaluate policies concurrently because those runs are
independent. Each run receives its own copy of mutable scheduling state and
uses the same immutable input ordering. Cancellation is propagated with
`context.Context`. Policy results are sorted by policy name after all runs
complete, so goroutine completion order cannot affect output.

Timed-policy comparison uses the same ownership rule. Each goroutine receives
deep copies of the cluster and timed jobs, and each simulation remains
single-threaded. The CLI-only policy value `all` selects this comparison; it is
not a scheduler policy.

## Discrete-event lifecycle

Timed workloads use signed integer logical ticks. The engine visits only a tick
containing an arrival or completion and advances directly to the next such
tick. It uses no wall-clock time, sleeping, timers, or per-job goroutines.

At a tick, state changes occur in this fixed order:

1. all finishing jobs complete in original input order and release resources;
2. all arrivals enter the pending queue in original input order;
3. one admission cycle applies the selected queue policy.

Future arrivals are ordered by `(arrivalTick, inputIndex)`. Running jobs use a
min-heap ordered by `(finishTick, inputIndex)`. These explicit secondary keys
make simultaneous event order independent of map or heap implementation details.

Admission records the selected topology domain and exact per-node allocation.
Completion releases that allocation with the job's per-replica request; it does
not rerun placement to infer which nodes supplied resources. Release validates
the full allocation before mutation and prevents remaining capacity from
exceeding original node capacity.

## Deterministic placement

Jobs are considered in YAML order. For a topology-constrained job, placement
examines every value of the requested topology key. A fitting domain with the
fewest available replica slots is preferred, followed by the candidate using
the fewest nodes, then by topology-domain name in lexical order.

Within the selected domain, nodes are sorted by available replica slots in
descending order and then by node name in ascending order. Compact packing
fills each node before moving to the next. Map-derived values are converted to
sorted slices before they influence placement or output.

## Queue policies

Strict FIFO stops admission attempts at the first job that cannot fit. That job
keeps its capacity or topology rejection reason; every later job is pending
with a head-of-line-blocked reason naming the first blocked job.

Backfill records an unplaceable job as pending and continues through the queue.
This can admit a smaller later job, but it does not reorder jobs or reconsider
earlier decisions.

For timed strict FIFO, each event tick repeatedly admits the pending head until
one placement fails, then retains the entire remaining queue. Timed backfill
scans the pending queue once, admits every job that currently fits, and retains
failures in relative order. A single pass is sufficient because admission only
consumes resources. Pending jobs are retried after later arrivals or completions.

When there are no future arrivals or running jobs, pending work becomes
terminally unscheduled. Backfill records every job's actual placement rejection
against the fully released cluster. Strict FIFO records the head's actual reason
and marks later jobs as head-of-line blocked by that head. This terminal rule
prevents permanently impossible workloads from looping.

## Lifecycle metrics

Event timelines contain only `arrived`, `admitted`, and `completed` transitions.
Job lifecycles remain in input order and end as `completed` or `unscheduled`.
For completed jobs, queue delay is admission tick minus arrival tick.

The summary start is the earliest processed event tick, the end is the latest,
and makespan is `end - start`. Total, average, and maximum queue delay include
completed jobs only. Empty workloads report zero metrics. Integer totals remain
exact in scheduler results; only text output rounds average delay to two decimal
places.

## Rejection categories

Insufficient capacity means the cluster-wide number of currently feasible
replica slots is smaller than the requested replica count. This is a resource
shortfall regardless of topology.

Topology fragmentation means cluster-wide slots are sufficient, but no single
value of the required topology key has enough slots for all replicas. The
distinction exposes whether the job is blocked by total remaining resources or
by their distribution across topology domains.

Timed terminal reasons are recomputed after all running allocations have been
released. A job that was temporarily short of total capacity can therefore end
with a topology-fragmentation reason if the empty cluster has enough global
slots but no sufficiently large domain.

## Deliberate non-goals

TopoQueue is not a Kubernetes scheduler, a Kueue implementation, a cluster
controller, or a production scheduling system. It does not watch live cluster
state, preempt or resize jobs, model failures or retries, provide fairness or
quota guarantees, reserve resources, persist state, or implement Kubernetes
resource accounting and exact Kueue semantics. The model contains only integer
CPU and GPU capacity, identical fixed-duration replicas, and a single optional
topology key per job.
