# TopoQueue

[![CI](https://github.com/Rionlyu/topoqueue/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Rionlyu/topoqueue/actions/workflows/ci.yml)

TopoQueue is a deterministic Go CLI for exploring how queue policy and
topology constraints affect accelerator-job admission.

> TopoQueue is an educational scheduling simulator. It is not a Kubernetes
> scheduler and does not implement Kueue semantics.

## Features

- Strict FIFO and backfill queue-policy simulations.
- Optional single-topology-domain placement for identical job replicas.
- Deterministic domain selection, compact node packing, and output ordering.
- Structured insufficient-capacity, topology-fragmentation, and
  head-of-line-blocking reasons.
- Human-readable text and stable, indented JSON output.
- Concurrent comparison of policies with isolated mutable scheduling state.
- Strict YAML decoding that rejects unknown fields.
- Discrete-event workload simulation with logical arrivals, fixed durations,
  completion-driven resource release, and terminal lifecycle results.

## Installation

TopoQueue requires Go 1.25 or newer.

Install with Go:

```sh
go install github.com/Rionlyu/topoqueue/cmd/topoqueue@latest
topoqueue --help
```

`go install` writes the binary to `GOBIN`, or to `$(go env GOPATH)/bin` when
`GOBIN` is unset. Ensure that directory is on `PATH`.

To build from a repository checkout instead:

```sh
git clone https://github.com/Rionlyu/topoqueue.git
cd topoqueue
make build
./bin/topoqueue --help
```

## Quick start

Build the CLI and run the included comparison:

```sh
make build
./bin/topoqueue compare \
  --cluster examples/cluster.yaml \
  --jobs examples/jobs.yaml \
  --output text
```

Run one policy without building a persistent binary:

```sh
go run ./cmd/topoqueue schedule \
  --cluster examples/cluster.yaml \
  --jobs examples/jobs.yaml \
  --policy strict-fifo \
  --output json
```

Simulate both policies over the checked-in timed workload:

```sh
./bin/topoqueue simulate \
  --cluster examples/cluster.yaml \
  --jobs examples/timed-jobs.yaml \
  --policy all \
  --output text
```

## Example comparison

This is the output of `make demo` using the checked-in example files:

```text
POLICY       ADMITTED  PENDING  GPU USED  HEAD-OF-LINE BLOCKED
backfill     2         1        8/16      0
strict-fifo  1         2        4/16      1

POLICY backfill (CPU 32/128, GPU 8/16)
JOB       STATUS    DOMAIN  ALLOCATION  CPU    GPU   REASON
warmup    admitted  rack-a  node-a1=4   16/16  4/4   -
train-xl  pending   -       -           0/40   0/10  topology_fragmented: requested 10 replicas in one "rack" domain; 12 slots are available cluster-wide, but the largest domain "rack-b" has 8
batch     admitted  rack-a  node-a2=4   16/16  4/4   -

POLICY strict-fifo (CPU 16/128, GPU 4/16)
JOB       STATUS    DOMAIN  ALLOCATION  CPU    GPU   REASON
warmup    admitted  rack-a  node-a1=4   16/16  4/4   -
train-xl  pending   -       -           0/40   0/10  topology_fragmented: requested 10 replicas in one "rack" domain; 12 slots are available cluster-wide, but the largest domain "rack-b" has 8
batch     pending   -       -           0/16   0/4   head_of_line_blocked: head_of_line_blocked_by=train-xl
```

The `CPU` and `GPU` columns in job decisions show allocated/requested units.

## Event-driven simulation

The `simulate` command uses a separate timed-jobs schema with required
`arrivalTick` and `durationTicks` fields. It advances directly between logical
event ticks; it does not sleep or use wall-clock time. At each tick it completes
and releases all finishing jobs, enqueues all arrivals, and then runs one
admission cycle.

The checked-in example demonstrates the difference over time:

| Policy | Completed | Unscheduled | Makespan | Total wait | Average wait | Maximum wait |
|---|---:|---:|---:|---:|---:|---:|
| backfill | 3 | 0 | 12 | 7 | 2.33 | 7 |
| strict-fifo | 3 | 0 | 14 | 17 | 5.67 | 10 |

Backfill starts `batch` at tick 2 while `train-xl` waits for the whole cluster.
Strict FIFO retains `train-xl` at the queue head, so `batch` starts only after
`train-xl` completes. See [Event-driven simulation](docs/event-driven-simulation.md)
for the input contract, event ordering, lifecycle fields, and metric definitions.

## Algorithm

Jobs are considered in YAML order. For each attempted job, TopoQueue computes
the number of whole replicas that fit on every node while ignoring resource
dimensions requested at zero.

Without a topology requirement, all nodes form one global domain. With a
requirement such as `rack`, each topology value is evaluated independently.
Among domains that fit the entire job, the scheduler selects the domain with
the fewest available slots, then the candidate using the fewest nodes, then the
lexicographically first domain. Within that domain it sorts nodes by available
slots descending and node name ascending, then packs replicas compactly.

Strict FIFO stops after the first unplaceable job and marks later jobs as
head-of-line blocked. Backfill records an unplaceable job and continues trying
later jobs. A comparison runs both simulations concurrently, but each policy
run is single-threaded and owns its scheduling state.

Timed simulation reuses the same placement algorithm. Arrivals are ordered by
logical tick and input position, and running jobs are ordered by completion tick
and input position. Completion releases the exact recorded per-node allocation.
Pending work is retried only at arrival and completion ticks. When neither can
occur again, remaining jobs become terminally unscheduled with final placement
reasons.

## Complexity

Let `N` be the number of nodes, `D` the number of topology domains, and `J` the
number of attempted jobs. One placement requires `O(N)` slot calculation plus
sorting nodes and domain candidates, for a worst-case bound of
`O(N log N + D log D)` time and `O(N + D)` temporary space. A full backfill run
is `O(J * (N log N + D log D))`; strict FIFO may stop before all `J` jobs.
Policy comparison performs two independent runs with the same per-run bounds.
For a timed workload, heap operations add `O(log J)` per admission/completion.
One admission cycle may inspect every pending job, and up to `O(J)` event ticks
can retry pending work, so the deliberately simple worst case is
`O(J^2 * (N log N + D log D) + J log J)` time and `O(N + D + J)` state.

## Limitations

- Cluster capacity is still an input snapshot; there is no API server, watch
  loop, or cluster controller.
- Resources are whole, non-negative CPU and GPU units; cluster and per-job
  totals must fit in a signed 64-bit integer.
- Replicas are identical, cannot be split across nodes, and support at most one
  required topology key per job.
- Timed jobs have fixed successful durations. There are no failures, failure
  retries, preemption, priorities, fairness, reservations, or elastic jobs.
- TopoQueue does not implement Kubernetes scheduling behavior or exact Kueue
  semantics.
- The simulator is educational and is not intended for production scheduling.

## Development

```sh
make fmt-check
make vet
make test
make build
make demo
make demo-simulate
make benchmark
```

## Project structure

```text
cmd/topoqueue/        CLI entry point and flag handling
internal/model/       YAML data model, strict loading, and validation
internal/scheduler/   policies, placement, comparison, tests, and benchmark
internal/output/      deterministic text and JSON renderers
examples/             example cluster and ordered jobs
docs/design.md        design decisions and deliberate non-goals
docs/event-driven-simulation.md  timed input, lifecycle, and metric semantics
.github/workflows/    formatting, vet, race-test, and build CI
```

TopoQueue is licensed under the Apache License 2.0. See `LICENSE`.
