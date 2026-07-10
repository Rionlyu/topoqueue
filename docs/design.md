# Design

TopoQueue evaluates a fixed cluster snapshot and an ordered job list. It is a
small teaching tool, so the implementation favors explicit state transitions
and deterministic results over scheduler extensibility.

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

## Rejection categories

Insufficient capacity means the cluster-wide number of currently feasible
replica slots is smaller than the requested replica count. This is a resource
shortfall regardless of topology.

Topology fragmentation means cluster-wide slots are sufficient, but no single
value of the required topology key has enough slots for all replicas. The
distinction exposes whether the job is blocked by total remaining resources or
by their distribution across topology domains.

## Deliberate non-goals

TopoQueue is not a Kubernetes scheduler, a Kueue implementation, a cluster
controller, or a production scheduling system. It does not watch live cluster
state, preempt or resize jobs, model failures, provide fairness guarantees, or
implement Kubernetes resource accounting and exact Kueue semantics. The model
contains only integer CPU and GPU capacity and a single optional topology key
per job.
