package output

import (
	"io"
	"sort"

	"github.com/Rionlyu/topoqueue/internal/scheduler"
)

// SimulationComparisonJSON is the stable top-level JSON representation of a
// timed policy comparison.
type SimulationComparisonJSON struct {
	Policies []scheduler.SimulationResult `json:"policies"`
}

// WriteSimulationJSON writes one timed policy simulation as indented JSON.
func WriteSimulationJSON(w io.Writer, result scheduler.SimulationResult) error {
	return writeJSON(w, result)
}

// WriteSimulationComparisonJSON writes policy-sorted timed simulations as
// indented JSON.
func WriteSimulationComparisonJSON(w io.Writer, results []scheduler.SimulationResult) error {
	return writeJSON(w, SimulationComparisonJSON{Policies: sortedSimulationResults(results)})
}

func sortedSimulationResults(results []scheduler.SimulationResult) []scheduler.SimulationResult {
	ordered := append([]scheduler.SimulationResult(nil), results...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Policy < ordered[j].Policy
	})
	return ordered
}
