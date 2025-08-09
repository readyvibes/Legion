package cluster

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
)

type MasterNode struct {
	Node
}

// ClusterMasterState holds the state of the cluster as seen by the master node.
var (
	clusterState = struct {
		sync.Mutex
		Nodes map[string]WorkerNode
	}{Nodes: make(map[string]WorkerNode)}
)

// RegisterHandler handles registration of worker nodes.
func RegisterHandler(w http.ResponseWriter, r *http.Request) {
	var node WorkerNode
	if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	clusterState.Lock()
	clusterState.Nodes[node.address] = node
	clusterState.Unlock()
	w.WriteHeader(http.StatusOK)
}

// HeartbeatHandler handles heartbeat messages from worker nodes.
func HeartbeatHandler(w http.ResponseWriter, r *http.Request) {
	var node WorkerNode
	if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	clusterState.Lock()
	n, ok := clusterState.Nodes[node.address]
	if ok {
		n.Status = "active"
		clusterState.Nodes[node.address] = n
	}
	clusterState.Unlock()
	w.WriteHeader(http.StatusOK)
}

	// StartMasterServer starts the HTTP server for the master node.
func (m *MasterNode) Run() {
	http.HandleFunc("/register", RegisterHandler)
	http.HandleFunc("/heartbeat", HeartbeatHandler)
	log.Println("Master listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
