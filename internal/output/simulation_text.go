package output

import (
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"

	"github.com/Rionlyu/topoqueue/internal/scheduler"
)

// WriteSimulationText writes one timed policy simulation as a summary, an
// ordered event timeline, and original-order job lifecycles.
func WriteSimulationText(w io.Writer, result scheduler.SimulationResult) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if err := writeSimulationSummary(tw, result); err != nil {
		return err
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flush simulation summary: %w", err)
	}

	if _, err := fmt.Fprintln(w); err != nil {
		return fmt.Errorf("write simulation timeline separator: %w", err)
	}
	if err := writeSimulationEvents(w, result); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w); err != nil {
		return fmt.Errorf("write simulation lifecycle separator: %w", err)
	}
	if err := writeSimulationLifecycles(w, result); err != nil {
		return err
	}
	return nil
}

// WriteSimulationComparisonText writes a sorted policy comparison followed by
// the complete deterministic output for each simulation.
func WriteSimulationComparisonText(w io.Writer, results []scheduler.SimulationResult) error {
	results = sortedSimulationResults(results)
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "POLICY\tCOMPLETED\tUNSCHEDULED\tMAKESPAN\tAVERAGE WAIT\tMAX WAIT"); err != nil {
		return fmt.Errorf("write simulation comparison header: %w", err)
	}
	for _, result := range results {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%d\t%d\t%d\t%.2f\t%d\n",
			result.Policy,
			result.CompletedJobCount,
			result.UnscheduledJobCount,
			result.MakespanTicks,
			result.AverageQueueDelayTicks,
			result.MaximumQueueDelayTicks,
		); err != nil {
			return fmt.Errorf("write simulation comparison policy %q: %w", result.Policy, err)
		}
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flush simulation comparison summary: %w", err)
	}

	for _, result := range results {
		if _, err := fmt.Fprintln(w); err != nil {
			return fmt.Errorf("write simulation comparison separator: %w", err)
		}
		if err := WriteSimulationText(w, result); err != nil {
			return fmt.Errorf("write simulation comparison policy %q details: %w", result.Policy, err)
		}
	}
	return nil
}

func writeSimulationSummary(w io.Writer, result scheduler.SimulationResult) error {
	if _, err := fmt.Fprintln(w, "POLICY\tCOMPLETED\tUNSCHEDULED\tSTART\tEND\tMAKESPAN\tTOTAL WAIT\tAVERAGE WAIT\tMAX WAIT"); err != nil {
		return fmt.Errorf("write simulation summary header: %w", err)
	}
	if _, err := fmt.Fprintf(
		w,
		"%s\t%d\t%d\t%d\t%d\t%d\t%d\t%.2f\t%d\n",
		result.Policy,
		result.CompletedJobCount,
		result.UnscheduledJobCount,
		result.StartTick,
		result.EndTick,
		result.MakespanTicks,
		result.TotalQueueDelayTicks,
		result.AverageQueueDelayTicks,
		result.MaximumQueueDelayTicks,
	); err != nil {
		return fmt.Errorf("write simulation summary for policy %q: %w", result.Policy, err)
	}
	return nil
}

func writeSimulationEvents(w io.Writer, result scheduler.SimulationResult) error {
	if _, err := fmt.Fprintln(w, "EVENT TIMELINE"); err != nil {
		return fmt.Errorf("write simulation event heading for policy %q: %w", result.Policy, err)
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "TICK\tTYPE\tJOB\tDOMAIN\tALLOCATION"); err != nil {
		return fmt.Errorf("write simulation event header for policy %q: %w", result.Policy, err)
	}
	for _, event := range result.Events {
		if _, err := fmt.Fprintf(
			tw,
			"%d\t%s\t%s\t%s\t%s\n",
			event.Tick,
			event.Type,
			event.JobName,
			displayValue(event.SelectedTopologyDomain),
			formatAllocation(event.Allocation),
		); err != nil {
			return fmt.Errorf("write simulation event for job %q at tick %d: %w", event.JobName, event.Tick, err)
		}
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flush simulation events for policy %q: %w", result.Policy, err)
	}
	return nil
}

func writeSimulationLifecycles(w io.Writer, result scheduler.SimulationResult) error {
	if _, err := fmt.Fprintln(w, "JOB LIFECYCLE"); err != nil {
		return fmt.Errorf("write simulation lifecycle heading for policy %q: %w", result.Policy, err)
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(
		tw,
		"JOB\tSTATUS\tARRIVAL\tDURATION\tSTART\tCOMPLETION\tWAIT\tDOMAIN\tALLOCATION\tREQUESTED CPU\tREQUESTED GPU\tREASON",
	); err != nil {
		return fmt.Errorf("write simulation lifecycle header for policy %q: %w", result.Policy, err)
	}
	for _, job := range result.Jobs {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s\n",
			job.JobName,
			job.Status,
			job.ArrivalTick,
			job.DurationTicks,
			formatOptionalTick(job.StartTick),
			formatOptionalTick(job.CompletionTick),
			formatOptionalTick(job.QueueDelayTicks),
			displayValue(job.SelectedTopologyDomain),
			formatAllocation(job.Allocation),
			job.RequestedResources.CPU,
			job.RequestedResources.GPU,
			formatReason(job.Reason),
		); err != nil {
			return fmt.Errorf("write simulation lifecycle for job %q under policy %q: %w", job.JobName, result.Policy, err)
		}
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("flush simulation lifecycles for policy %q: %w", result.Policy, err)
	}
	return nil
}

func formatOptionalTick(tick *int64) string {
	if tick == nil {
		return "-"
	}
	return strconv.FormatInt(*tick, 10)
}
