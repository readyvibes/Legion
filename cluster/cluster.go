package cluster

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	. "heapscheduler/jobs"
	"github.com/gorilla/mux"
	"strconv"
)

type Cluster struct {
	masterNode *MasterNode
	workers    map[string]*WorkerNode
	server     *http.Server
	mu         sync.RWMutex
}

func NewCluster(dbURL string) *Cluster {
	master := NewMasterNode(dbURL)
	
	return &Cluster{
		masterNode: master,
		workers:    make(map[string]*WorkerNode),
	}
}

func (c *Cluster) Start() error {
	// Start master node
	if err := c.masterNode.Start(); err != nil {
		return fmt.Errorf("failed to start master node: %v", err)
	}

	// Setup HTTP server
	router := mux.NewRouter()
	router.HandleFunc("/jobs", c.handleSubmitJob).Methods("POST")
	router.HandleFunc("/jobs/{id}", c.handleGetJob).Methods("GET")
	router.HandleFunc("/jobs", c.handleListJobs).Methods("GET")
	router.HandleFunc("/cluster/status", c.handleClusterStatus).Methods("GET")
	router.HandleFunc("/health", c.handleHealth).Methods("GET")

	c.server = &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	log.Println("Cluster HTTP server starting on :8080")
	return c.server.ListenAndServe()
}

func (c *Cluster) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var job Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Forward job to master node
	addedJob := c.masterNode.AddJob(&job)
	if !addedJob {
		http.Error(w, "Failed to add job", http.StatusInternalServerError)
		return
	}
}

func (c *Cluster) handleGetJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["id"]

	id, err := strconv.ParseUint(jobID, 10, 64)

	job, err := c.masterNode.GetJob(id)
	if err != nil {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

func (c *Cluster) handleListJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := c.masterNode.ListJobs()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}

func (c *Cluster) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	c.mu.RLock()
	workerCount := len(c.workers)
	c.mu.RUnlock()

	status := map[string]interface{}{
		"workers": workerCount,
		"master":  "running",
		"queue_size": c.masterNode.GetQueueLength(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (c *Cluster) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "OK")
}

func (c *Cluster) AddWorker(workerID string) *WorkerNode {
	c.mu.Lock()
	defer c.mu.Unlock()

	worker := NewWorkerNode(workerID, c.masterNode)
	c.workers[workerID] = worker
	
	// Register worker with master
	c.masterNode.RegisterWorker(worker)
	
	return worker
}