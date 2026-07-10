// Package output renders deterministic scheduler results.
package output

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/example/topoqueue/internal/scheduler"
)

// WritePolicyText writes one policy result in a compact human-readable form.
func WritePolicyText(w io.Writer, result scheduler.PolicyResult) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if err := writePolicySummary(tw, result); err != nil {
		return err
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flush policy summary: %w", err)
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return fmt.Errorf("write policy output separator: %w", err)
	}
	if err := writeJobDecisions(w, result); err != nil {
		return err
	}
	return nil
}

// WriteComparisonText writes the comparison summary followed by each policy's
// ordered job decisions.
func WriteComparisonText(w io.Writer, results []scheduler.PolicyResult) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	results = sortedPolicyResults(results)
	if _, err := fmt.Fprintln(tw, "POLICY\tADMITTED\tPENDING\tGPU USED\tHEAD-OF-LINE BLOCKED"); err != nil {
		return fmt.Errorf("write comparison header: %w", err)
	}
	for _, result := range results {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%d\t%d\t%s\t%d\n",
			result.Policy,
			result.AdmittedJobCount,
			result.PendingJobCount,
			resourceUsage(result.UsedResources.GPU, result.TotalResources.GPU),
			result.HeadOfLineBlockedJobCount,
		); err != nil {
			return fmt.Errorf("write comparison policy %q: %w", result.Policy, err)
		}
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flush comparison summary: %w", err)
	}

	for _, result := range results {
		if _, err := fmt.Fprintln(w); err != nil {
			return fmt.Errorf("write comparison separator: %w", err)
		}
		if err := writeJobDecisions(w, result); err != nil {
			return err
		}
	}
	return nil
}

func writePolicySummary(w io.Writer, result scheduler.PolicyResult) error {
	if _, err := fmt.Fprintf(w, "POLICY\tADMITTED\tPENDING\tCPU USED\tGPU USED\tHEAD-OF-LINE BLOCKED\n"); err != nil {
		return fmt.Errorf("write policy summary header: %w", err)
	}
	if _, err := fmt.Fprintf(
		w,
		"%s\t%d\t%d\t%s\t%s\t%d\n",
		result.Policy,
		result.AdmittedJobCount,
		result.PendingJobCount,
		resourceUsage(result.UsedResources.CPU, result.TotalResources.CPU),
		resourceUsage(result.UsedResources.GPU, result.TotalResources.GPU),
		result.HeadOfLineBlockedJobCount,
	); err != nil {
		return fmt.Errorf("write policy summary %q: %w", result.Policy, err)
	}
	return nil
}

func writeJobDecisions(w io.Writer, result scheduler.PolicyResult) error {
	if _, err := fmt.Fprintf(
		w,
		"POLICY %s (CPU %s, GPU %s)\n",
		result.Policy,
		resourceUsage(result.UsedResources.CPU, result.TotalResources.CPU),
		resourceUsage(result.UsedResources.GPU, result.TotalResources.GPU),
	); err != nil {
		return fmt.Errorf("write policy decision heading %q: %w", result.Policy, err)
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "JOB\tSTATUS\tDOMAIN\tALLOCATION\tCPU\tGPU\tREASON"); err != nil {
		return fmt.Errorf("write job header for policy %q: %w", result.Policy, err)
	}
	for _, job := range result.Jobs {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			job.JobName,
			job.Status,
			displayValue(job.SelectedTopologyDomain),
			formatAllocation(job.Allocation),
			resourceUsage(job.AllocatedResources.CPU, job.RequestedResources.CPU),
			resourceUsage(job.AllocatedResources.GPU, job.RequestedResources.GPU),
			formatReason(job.Reason),
		); err != nil {
			return fmt.Errorf("write job %q for policy %q: %w", job.JobName, result.Policy, err)
		}
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flush job decisions for policy %q: %w", result.Policy, err)
	}
	return nil
}

func formatAllocation(allocations []scheduler.Allocation) string {
	if len(allocations) == 0 {
		return "-"
	}
	ordered := append([]scheduler.Allocation(nil), allocations...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].NodeName < ordered[j].NodeName
	})
	parts := make([]string, len(ordered))
	for i, allocation := range ordered {
		parts[i] = fmt.Sprintf("%s=%d", allocation.NodeName, allocation.Replicas)
	}
	return strings.Join(parts, ",")
}

func formatReason(reason *scheduler.Reason) string {
	if reason == nil {
		return "-"
	}
	return fmt.Sprintf("%s: %s", reason.Code, reason.Message)
}

func displayValue(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func resourceUsage(used, total int64) string {
	return fmt.Sprintf("%d/%d", used, total)
}
