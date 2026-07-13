# TopoQueue engineering guidance

## Product invariants

- Determinism is a product feature. Identical inputs must produce byte-for-byte stable text and JSON output.
- TopoQueue is an educational simulator. Never claim Kubernetes scheduler, Kueue, or production-scheduler compatibility.
- Preserve the behavior and stable output shapes of the existing `schedule` and `compare` commands.
- Use logical integer ticks for simulations. Do not use wall-clock time, sleeps, randomness, or background timers.
- Keep state ownership explicit. Independent policy runs must not share mutable scheduling state.
- Prefer the Go standard library. Do not add a dependency unless the feature cannot reasonably be implemented without it.
- Reject invalid or ambiguous input early, with contextual errors.
- Check arithmetic that can overflow.
- Keep unrelated refactors out of feature commits.

## Repository map

- `cmd/topoqueue/`: CLI parsing and command wiring
- `internal/model/`: strict YAML models, loading, cloning, and validation
- `internal/scheduler/`: placement, policies, comparisons, and simulation logic
- `internal/output/`: deterministic text and JSON renderers
- `examples/`: runnable scenarios
- `docs/`: design documents and executable plans

## Verification

After each milestone:

1. Run `gofmt` on changed Go files.
2. Run focused package tests.
3. Run `go test ./...` before committing.

Before declaring the branch complete, run:

```sh
make fmt-check
make vet
make test
make build
make demo
make demo-simulate
make benchmark
git diff --check main...HEAD
```

Fix failures before proceeding. Do not weaken, skip, or delete tests merely to make a check pass.

## Git discipline

- Work only on the current feature branch; never commit directly to `main`.
- Use the repository's existing commit-signing configuration. Do not change Git configuration or disable signing.
- Create coherent, reviewable commits. Target 8–12 commits for a large milestone, but never split changes merely to increase the count.
- Every commit must compile and pass `go test ./...`.
- Do not create empty commits.
- Do not amend or rewrite commits that existed before this feature branch.
- Do not push, force-push, tag, create a release, or open/merge a pull request.
- Finish with a clean worktree.

## ExecPlan

For the event-driven simulation milestone, maintain the living plan at:

`docs/exec-plans/v0.2-event-driven-simulation.md`

Update its progress, decisions, validation evidence, and known limitations as work proceeds.
