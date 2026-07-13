package model

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"go.yaml.in/yaml/v3"
)

// LoadCluster reads, strictly decodes, and validates a cluster YAML file.
func LoadCluster(path string) (Cluster, error) {
	var cluster Cluster
	if err := loadYAML(path, "cluster", &cluster); err != nil {
		return Cluster{}, err
	}
	if cluster.Nodes == nil {
		return Cluster{}, fmt.Errorf("validate cluster file %q: top-level field %q is required and must be a sequence", path, "nodes")
	}
	if err := ValidateCluster(cluster); err != nil {
		return Cluster{}, fmt.Errorf("validate cluster file %q: %w", path, err)
	}
	return cluster, nil
}

// LoadJobs reads, strictly decodes, and validates a jobs YAML file.
func LoadJobs(path string) (JobSet, error) {
	var jobs JobSet
	if err := loadYAML(path, "jobs", &jobs); err != nil {
		return JobSet{}, err
	}
	if jobs.Jobs == nil {
		return JobSet{}, fmt.Errorf("validate jobs file %q: top-level field %q is required and must be a sequence", path, "jobs")
	}
	if err := ValidateJobs(jobs); err != nil {
		return JobSet{}, fmt.Errorf("validate jobs file %q: %w", path, err)
	}
	return jobs, nil
}

// LoadTimedJobs reads, strictly decodes, and validates a timed jobs YAML file.
func LoadTimedJobs(path string) (TimedJobSet, error) {
	var document timedJobDocument
	if err := loadYAML(path, "timed jobs", &document); err != nil {
		return TimedJobSet{}, err
	}
	if document.Jobs == nil {
		return TimedJobSet{}, fmt.Errorf("validate timed jobs file %q: top-level field %q is required and must be a sequence", path, "jobs")
	}

	jobs := TimedJobSet{Jobs: make([]TimedJob, len(document.Jobs))}
	for index, input := range document.Jobs {
		if input.ArrivalTick == nil {
			return TimedJobSet{}, fmt.Errorf("validate timed jobs file %q: %s: arrivalTick is required", path, jobDescription(input.Job, index))
		}
		if input.DurationTicks == nil {
			return TimedJobSet{}, fmt.Errorf("validate timed jobs file %q: %s: durationTicks is required", path, jobDescription(input.Job, index))
		}
		jobs.Jobs[index] = TimedJob{
			Job:           input.Job,
			ArrivalTick:   *input.ArrivalTick,
			DurationTicks: *input.DurationTicks,
		}
	}

	if err := ValidateTimedJobs(jobs); err != nil {
		return TimedJobSet{}, fmt.Errorf("validate timed jobs file %q: %w", path, err)
	}
	return jobs, nil
}

type timedJobDocument struct {
	Jobs []timedJobInput `yaml:"jobs"`
}

type timedJobInput struct {
	Job           `yaml:",inline"`
	ArrivalTick   *int64 `yaml:"arrivalTick"`
	DurationTicks *int64 `yaml:"durationTicks"`
}

func jobDescription(job Job, index int) string {
	if job.Name == "" {
		return fmt.Sprintf("job at index %d", index)
	}
	return fmt.Sprintf("job %q", job.Name)
}

func loadYAML(path, kind string, destination any) error {
	contents, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s file %q: %w", kind, path, err)
	}

	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)
	if err := decoder.Decode(destination); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("decode %s file %q: file is empty", kind, path)
		}
		return fmt.Errorf("decode %s file %q: %w", kind, path, err)
	}

	var extra any
	if err := decoder.Decode(&extra); err == nil {
		return fmt.Errorf("decode %s file %q: multiple YAML documents are not supported", kind, path)
	} else if !errors.Is(err, io.EOF) {
		return fmt.Errorf("decode %s file %q after first document: %w", kind, path, err)
	}

	return nil
}
