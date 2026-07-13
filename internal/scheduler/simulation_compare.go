package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/Rionlyu/topoqueue/internal/model"
)

type simulationComparisonOutcome struct {
	policy Policy
	result SimulationResult
	err    error
}

// CompareSimulations runs strict FIFO and backfill simulations concurrently on
// independent inputs. Results are ordered by policy name, not completion order.
func CompareSimulations(ctx context.Context, cluster model.Cluster, jobs model.TimedJobSet) ([]SimulationResult, error) {
	if ctx == nil {
		return nil, fmt.Errorf("compare simulations: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("compare simulations: %w", err)
	}
	if err := model.ValidateCluster(cluster); err != nil {
		return nil, fmt.Errorf("compare simulations: validate cluster: %w", err)
	}
	if err := model.ValidateTimedJobs(jobs); err != nil {
		return nil, fmt.Errorf("compare simulations: validate timed jobs: %w", err)
	}
	if err := model.ValidateTopologyRequirements(cluster, jobs.StaticJobs()); err != nil {
		return nil, fmt.Errorf("compare simulations: validate topology requirements: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	policies := []Policy{PolicyStrictFIFO, PolicyBackfill}
	outcomes := make(chan simulationComparisonOutcome, len(policies))
	for _, policy := range policies {
		policy := policy
		clusterCopy := cluster.Clone()
		jobsCopy := jobs.Clone()
		go func() {
			result, err := Simulate(ctx, clusterCopy, jobsCopy, policy)
			outcomes <- simulationComparisonOutcome{policy: policy, result: result, err: err}
		}()
	}

	completed := make([]simulationComparisonOutcome, 0, len(policies))
	for range policies {
		outcome := <-outcomes
		completed = append(completed, outcome)
		// Backfill sorts first and therefore has deterministic error precedence.
		// Once it fails intrinsically, the sibling cannot change the selected
		// error and can be canceled safely. A strict-FIFO failure must not cancel
		// a still-running backfill simulation whose error would take precedence.
		if outcome.policy == PolicyBackfill && outcome.err != nil && !errors.Is(outcome.err, context.Canceled) {
			cancel()
		}
	}
	sort.Slice(completed, func(i, j int) bool {
		return completed[i].policy < completed[j].policy
	})

	for _, outcome := range completed {
		if outcome.err != nil && !errors.Is(outcome.err, context.Canceled) {
			return nil, fmt.Errorf("compare simulation policy %q: %w", outcome.policy, outcome.err)
		}
	}

	results := make([]SimulationResult, 0, len(completed))
	for _, outcome := range completed {
		if outcome.err != nil {
			return nil, fmt.Errorf("compare simulation policy %q: %w", outcome.policy, outcome.err)
		}
		results = append(results, outcome.result)
	}
	return results, nil
}
