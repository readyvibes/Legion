package cluster

type Cluster struct {
	Nodes []WorkerNode// List of node addresses in the cluster
	Name string   // Name of the cluster
	Status string // Status of the cluster (e.g., "active", "inactive")
	Master MasterNode // The master node in the cluster
}