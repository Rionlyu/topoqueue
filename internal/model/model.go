// Package model defines TopoQueue's input data model.
package model

// Resources holds CPU and GPU quantities.
type Resources struct {
	CPU int64 `yaml:"cpu" json:"cpu"`
	GPU int64 `yaml:"gpu" json:"gpu"`
}

// Cluster is the cluster snapshot used by a simulation.
type Cluster struct {
	Nodes []Node `yaml:"nodes" json:"nodes"`
}

// Node is a schedulable node and its fixed capacity.
type Node struct {
	Name     string            `yaml:"name" json:"name"`
	Topology map[string]string `yaml:"topology" json:"topology"`
	Capacity Resources         `yaml:"capacity" json:"capacity"`
}

// JobSet preserves the queue order from a jobs file.
type JobSet struct {
	Jobs []Job `yaml:"jobs" json:"jobs"`
}

// Job describes a set of identical replicas that must share a topology
// domain when RequiredTopology is set.
type Job struct {
	Name                string    `yaml:"name" json:"name"`
	Replicas            int       `yaml:"replicas" json:"replicas"`
	ResourcesPerReplica Resources `yaml:"resourcesPerReplica" json:"resourcesPerReplica"`
	RequiredTopology    string    `yaml:"requiredTopology,omitempty" json:"requiredTopology,omitempty"`
}

// Clone returns a deep copy of the cluster, including topology maps.
func (c Cluster) Clone() Cluster {
	clone := Cluster{Nodes: make([]Node, len(c.Nodes))}
	for i, node := range c.Nodes {
		clone.Nodes[i] = node
		if node.Topology != nil {
			clone.Nodes[i].Topology = make(map[string]string, len(node.Topology))
			for key, value := range node.Topology {
				clone.Nodes[i].Topology[key] = value
			}
		}
	}
	return clone
}

// Clone returns a deep copy of the job set.
func (j JobSet) Clone() JobSet {
	clone := JobSet{Jobs: make([]Job, len(j.Jobs))}
	copy(clone.Jobs, j.Jobs)
	return clone
}
