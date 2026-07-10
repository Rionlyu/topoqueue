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
	if err := ValidateJobs(jobs); err != nil {
		return JobSet{}, fmt.Errorf("validate jobs file %q: %w", path, err)
	}
	return jobs, nil
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
