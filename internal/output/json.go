package output

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/Rionlyu/topoqueue/internal/scheduler"
)

// ComparisonJSON is the stable top-level JSON representation of a comparison.
type ComparisonJSON struct {
	Policies []scheduler.PolicyResult `json:"policies"`
}

// WritePolicyJSON writes one policy result as indented JSON.
func WritePolicyJSON(w io.Writer, result scheduler.PolicyResult) error {
	return writeJSON(w, result)
}

// WriteComparisonJSON writes sorted policy results as indented JSON.
func WriteComparisonJSON(w io.Writer, results []scheduler.PolicyResult) error {
	return writeJSON(w, ComparisonJSON{Policies: sortedPolicyResults(results)})
}

func writeJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("encode JSON output: %w", err)
	}
	return nil
}

func sortedPolicyResults(results []scheduler.PolicyResult) []scheduler.PolicyResult {
	ordered := append([]scheduler.PolicyResult(nil), results...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Policy < ordered[j].Policy
	})
	return ordered
}
