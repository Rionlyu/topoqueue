package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/example/topoqueue/internal/model"
)

type comparisonOutcome struct {
	policy Policy
	result PolicyResult
	err    error
}

// Compare evaluates strict FIFO and backfill concurrently on independent snapshots.
// Results are sorted by policy name, independent of goroutine completion order.
func Compare(ctx context.Context, cluster model.Cluster, jobs model.JobSet) ([]PolicyResult, error) {
	if ctx == nil {
		return nil, fmt.Errorf("compare policies: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("compare policies: %w", err)
	}
	if err := model.ValidateCluster(cluster); err != nil {
		return nil, fmt.Errorf("compare policies: validate cluster: %w", err)
	}
	if err := model.ValidateJobs(jobs); err != nil {
		return nil, fmt.Errorf("compare policies: validate jobs: %w", err)
	}
	if err := model.ValidateTopologyRequirements(cluster, jobs); err != nil {
		return nil, fmt.Errorf("compare policies: validate topology requirements: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	outcomes := make(chan comparisonOutcome, 2)
	policies := []Policy{PolicyStrictFIFO, PolicyBackfill}
	for _, policy := range policies {
		policy := policy
		clusterCopy := cluster.Clone()
		jobsCopy := jobs.Clone()
		go func() {
			result, err := Schedule(ctx, clusterCopy, jobsCopy, policy)
			outcomes <- comparisonOutcome{policy: policy, result: result, err: err}
		}()
	}

	completed := make([]comparisonOutcome, 0, len(policies))
	for range policies {
		outcome := <-outcomes
		completed = append(completed, outcome)
		if outcome.err != nil {
			cancel()
		}
	}
	sort.Slice(completed, func(i, j int) bool {
		return completed[i].policy < completed[j].policy
	})

	results := make([]PolicyResult, 0, len(completed))
	for _, outcome := range completed {
		if outcome.err != nil && !errors.Is(outcome.err, context.Canceled) {
			return nil, fmt.Errorf("compare policy %q: %w", outcome.policy, outcome.err)
		}
	}
	for _, outcome := range completed {
		if outcome.err != nil {
			return nil, fmt.Errorf("compare policy %q: %w", outcome.policy, outcome.err)
		}
		results = append(results, outcome.result)
	}
	return results, nil
}
