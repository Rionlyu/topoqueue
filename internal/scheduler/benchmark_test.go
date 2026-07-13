package scheduler_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/Rionlyu/topoqueue/internal/model"
	"github.com/Rionlyu/topoqueue/internal/scheduler"
)

func BenchmarkScheduleThousandJobs(b *testing.B) {
	cluster := benchmarkCluster(100)
	jobs := benchmarkJobs(1_000)
	b.ResetTimer()

	for iteration := 0; iteration < b.N; iteration++ {
		result, err := scheduler.Schedule(context.Background(), cluster, jobs, scheduler.PolicyBackfill)
		if err != nil {
			b.Fatal(err)
		}
		runtime.KeepAlive(result)
	}
}

func BenchmarkSimulateThousandTimedJobs(b *testing.B) {
	cluster := benchmarkCluster(100)
	jobs := benchmarkTimedJobs(1_000)
	b.ResetTimer()

	for iteration := 0; iteration < b.N; iteration++ {
		result, err := scheduler.Simulate(context.Background(), cluster, jobs, scheduler.PolicyBackfill)
		if err != nil {
			b.Fatal(err)
		}
		runtime.KeepAlive(result)
	}
}

func benchmarkCluster(nodeCount int) model.Cluster {
	cluster := model.Cluster{Nodes: make([]model.Node, 0, nodeCount)}
	for index := 0; index < nodeCount; index++ {
		cluster.Nodes = append(cluster.Nodes, model.Node{
			Name: fmt.Sprintf("node-%03d", index),
			Topology: map[string]string{
				"zone": "zone-a",
				"rack": fmt.Sprintf("rack-%02d", index/10),
			},
			Capacity: model.Resources{CPU: 64, GPU: 8},
		})
	}
	return cluster
}

func benchmarkJobs(jobCount int) model.JobSet {
	jobs := model.JobSet{Jobs: make([]model.Job, 0, jobCount)}
	for index := 0; index < jobCount; index++ {
		topology := "rack"
		if index%3 == 0 {
			topology = ""
		}
		jobs.Jobs = append(jobs.Jobs, model.Job{
			Name:                fmt.Sprintf("job-%04d", index),
			Replicas:            1 + index%12,
			ResourcesPerReplica: model.Resources{CPU: int64(1 + index%4), GPU: 1},
			RequiredTopology:    topology,
		})
	}
	return jobs
}

func benchmarkTimedJobs(jobCount int) model.TimedJobSet {
	jobs := model.TimedJobSet{Jobs: make([]model.TimedJob, 0, jobCount)}
	for index := 0; index < jobCount; index++ {
		topology := "rack"
		if index%3 == 0 {
			topology = ""
		}
		jobs.Jobs = append(jobs.Jobs, model.TimedJob{
			Job: model.Job{
				Name:                fmt.Sprintf("timed-job-%04d", index),
				Replicas:            1 + index%12,
				ResourcesPerReplica: model.Resources{CPU: int64(1 + index%4), GPU: 1},
				RequiredTopology:    topology,
			},
			ArrivalTick:   int64(index / 10),
			DurationTicks: int64(1 + index%10),
		})
	}
	return jobs
}
