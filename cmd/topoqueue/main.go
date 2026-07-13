package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/Rionlyu/topoqueue/internal/model"
	"github.com/Rionlyu/topoqueue/internal/output"
	"github.com/Rionlyu/topoqueue/internal/scheduler"
)

const (
	outputText = "text"
	outputJSON = "json"
	policyAll  = "all"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if _, writeErr := fmt.Fprintf(os.Stderr, "topoqueue: %v\n", err); writeErr != nil {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	if len(args) == 0 {
		if err := writeRootUsage(stderr); err != nil {
			return fmt.Errorf("write usage: %w", err)
		}
		return errors.New("a command is required")
	}

	switch args[0] {
	case "schedule":
		return runSchedule(ctx, args[1:], stdout, stderr)
	case "compare":
		return runCompare(ctx, args[1:], stdout, stderr)
	case "simulate":
		return runSimulate(ctx, args[1:], stdout, stderr)
	case "help", "-h", "--help":
		return writeRootUsage(stdout)
	default:
		if err := writeRootUsage(stderr); err != nil {
			return fmt.Errorf("unknown command %q; write usage: %w", args[0], err)
		}
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runSimulate(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("simulate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	clusterPath := flags.String("cluster", "", "path to the cluster YAML file")
	jobsPath := flags.String("jobs", "", "path to the timed jobs YAML file")
	policyName := flags.String("policy", string(scheduler.PolicyStrictFIFO), "queue policy: strict-fifo, backfill, or all")
	outputName := flags.String("output", outputText, "output format: text or json")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse simulate flags: %w", err)
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("simulate: unexpected positional arguments: %v", flags.Args())
	}
	if err := requireInputPaths(*clusterPath, *jobsPath); err != nil {
		return fmt.Errorf("simulate: %w", err)
	}
	if err := validateSimulationPolicy(*policyName); err != nil {
		return fmt.Errorf("simulate: %w", err)
	}
	if err := validateOutputFormat(*outputName); err != nil {
		return fmt.Errorf("simulate: %w", err)
	}

	cluster, jobs, err := loadSimulationInputs(*clusterPath, *jobsPath)
	if err != nil {
		return err
	}
	if *policyName == policyAll {
		results, err := scheduler.CompareSimulations(ctx, cluster, jobs)
		if err != nil {
			return fmt.Errorf("simulate all policies: %w", err)
		}
		switch *outputName {
		case outputText:
			if err := output.WriteSimulationComparisonText(stdout, results); err != nil {
				return fmt.Errorf("render simulation comparison text output: %w", err)
			}
		case outputJSON:
			if err := output.WriteSimulationComparisonJSON(stdout, results); err != nil {
				return fmt.Errorf("render simulation comparison JSON output: %w", err)
			}
		}
		return nil
	}

	policy, err := scheduler.ParsePolicy(*policyName)
	if err != nil {
		return fmt.Errorf("simulate: %w", err)
	}
	result, err := scheduler.Simulate(ctx, cluster, jobs, policy)
	if err != nil {
		return fmt.Errorf("simulate policy %q: %w", policy, err)
	}
	switch *outputName {
	case outputText:
		if err := output.WriteSimulationText(stdout, result); err != nil {
			return fmt.Errorf("render simulation text output: %w", err)
		}
	case outputJSON:
		if err := output.WriteSimulationJSON(stdout, result); err != nil {
			return fmt.Errorf("render simulation JSON output: %w", err)
		}
	}
	return nil
}

func runSchedule(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("schedule", flag.ContinueOnError)
	flags.SetOutput(stderr)
	clusterPath := flags.String("cluster", "", "path to the cluster YAML file")
	jobsPath := flags.String("jobs", "", "path to the jobs YAML file")
	policyName := flags.String("policy", string(scheduler.PolicyStrictFIFO), "queue policy: strict-fifo or backfill")
	outputName := flags.String("output", outputText, "output format: text or json")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse schedule flags: %w", err)
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("schedule: unexpected positional arguments: %v", flags.Args())
	}
	if err := requireInputPaths(*clusterPath, *jobsPath); err != nil {
		return fmt.Errorf("schedule: %w", err)
	}
	policy, err := scheduler.ParsePolicy(*policyName)
	if err != nil {
		return fmt.Errorf("schedule: %w", err)
	}
	if err := validateOutputFormat(*outputName); err != nil {
		return fmt.Errorf("schedule: %w", err)
	}

	cluster, jobs, err := loadInputs(*clusterPath, *jobsPath)
	if err != nil {
		return err
	}
	result, err := scheduler.Schedule(ctx, cluster, jobs, policy)
	if err != nil {
		return fmt.Errorf("schedule policy %q: %w", policy, err)
	}

	switch *outputName {
	case outputText:
		if err := output.WritePolicyText(stdout, result); err != nil {
			return fmt.Errorf("render text output: %w", err)
		}
	case outputJSON:
		if err := output.WritePolicyJSON(stdout, result); err != nil {
			return fmt.Errorf("render JSON output: %w", err)
		}
	}
	return nil
}

func runCompare(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	flags.SetOutput(stderr)
	clusterPath := flags.String("cluster", "", "path to the cluster YAML file")
	jobsPath := flags.String("jobs", "", "path to the jobs YAML file")
	outputName := flags.String("output", outputText, "output format: text or json")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse compare flags: %w", err)
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("compare: unexpected positional arguments: %v", flags.Args())
	}
	if err := requireInputPaths(*clusterPath, *jobsPath); err != nil {
		return fmt.Errorf("compare: %w", err)
	}
	if err := validateOutputFormat(*outputName); err != nil {
		return fmt.Errorf("compare: %w", err)
	}

	cluster, jobs, err := loadInputs(*clusterPath, *jobsPath)
	if err != nil {
		return err
	}
	results, err := scheduler.Compare(ctx, cluster, jobs)
	if err != nil {
		return fmt.Errorf("compare policies: %w", err)
	}

	switch *outputName {
	case outputText:
		if err := output.WriteComparisonText(stdout, results); err != nil {
			return fmt.Errorf("render text output: %w", err)
		}
	case outputJSON:
		if err := output.WriteComparisonJSON(stdout, results); err != nil {
			return fmt.Errorf("render JSON output: %w", err)
		}
	}
	return nil
}

func loadInputs(clusterPath, jobsPath string) (model.Cluster, model.JobSet, error) {
	cluster, err := model.LoadCluster(clusterPath)
	if err != nil {
		return model.Cluster{}, model.JobSet{}, err
	}
	jobs, err := model.LoadJobs(jobsPath)
	if err != nil {
		return model.Cluster{}, model.JobSet{}, err
	}
	if err := model.ValidateTopologyRequirements(cluster, jobs); err != nil {
		return model.Cluster{}, model.JobSet{}, fmt.Errorf(
			"validate cluster file %q against jobs file %q: %w",
			clusterPath,
			jobsPath,
			err,
		)
	}
	return cluster, jobs, nil
}

func loadSimulationInputs(clusterPath, jobsPath string) (model.Cluster, model.TimedJobSet, error) {
	cluster, err := model.LoadCluster(clusterPath)
	if err != nil {
		return model.Cluster{}, model.TimedJobSet{}, err
	}
	jobs, err := model.LoadTimedJobs(jobsPath)
	if err != nil {
		return model.Cluster{}, model.TimedJobSet{}, err
	}
	if err := model.ValidateTopologyRequirements(cluster, jobs.StaticJobs()); err != nil {
		return model.Cluster{}, model.TimedJobSet{}, fmt.Errorf(
			"validate cluster file %q against timed jobs file %q: %w",
			clusterPath,
			jobsPath,
			err,
		)
	}
	return cluster, jobs, nil
}

func requireInputPaths(clusterPath, jobsPath string) error {
	if clusterPath == "" {
		return errors.New("--cluster is required")
	}
	if jobsPath == "" {
		return errors.New("--jobs is required")
	}
	return nil
}

func validateOutputFormat(value string) error {
	switch value {
	case outputText, outputJSON:
		return nil
	default:
		return fmt.Errorf("unsupported output %q (supported: %s, %s)", value, outputText, outputJSON)
	}
}

func validateSimulationPolicy(value string) error {
	if value == policyAll {
		return nil
	}
	if _, err := scheduler.ParsePolicy(value); err != nil {
		return fmt.Errorf("unsupported policy %q (supported: %s, %s, %s)", value, scheduler.PolicyStrictFIFO, scheduler.PolicyBackfill, policyAll)
	}
	return nil
}

func writeRootUsage(w io.Writer) error {
	if _, err := fmt.Fprintln(w, "Usage: topoqueue <schedule|compare|simulate> [flags]"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "Run 'topoqueue <command> -h' for command flags."); err != nil {
		return err
	}
	return nil
}
