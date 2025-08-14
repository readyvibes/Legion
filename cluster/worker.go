package cluster

import (
	"time"
	"sync"
	"context"
	"net/http"
	"log"
	"fmt"
	"encoding/json"

	. "heapscheduler/jobs"
)

type WorkerStatus struct {
	ID         string    
	Available  bool      
	LastSeen   time.Time 
	CurrentJob uint64 // ID of the current job being executed
}

type WorkerNode struct {
	ID           string
	master       *MasterNode
	Address      string 
	Port 	     int 
	available    bool
	currentJobID uint64
	currentJob   *Job
	lastSeen     time.Time
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewWorkerNode(id string, master *MasterNode) *WorkerNode {
	ctx, cancel := context.WithCancel(context.Background())
	
	// Get local IP address
	localAddr := getLocalIP()

	worker := &WorkerNode{
		ID:        id,
		Address:   localAddr,
		Port:      8091 + len(id), // Port can be set later if needed
		master:    master,
		available: true,
		lastSeen:  time.Now(),
		ctx:       ctx,
		cancel:    cancel,
	}

	go worker.startWorkerServer()

	// Start heartbeat
	go worker.heartbeatLoop()

	return worker
}

func (w *WorkerNode) startWorkerServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/job/assign", w.handleJobAssign)
	mux.HandleFunc("/job/cancel", w.handleJobCancel)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", w.Port),
		Handler: mux,
	}

	log.Printf("Worker %s server starting on :%d", w.ID, w.Port)
	if err := server.ListenAndServe(); err != nil {
		log.Printf("Worker server error: %v", err)
	}
}

func (w *WorkerNode) handleJobAssign(writer http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(writer, "Invalid JSON", http.StatusBadRequest)
		return
	}

	payload, ok := msg.Payload.(map[string]interface{})
	if !ok {
		http.Error(writer, "Invalid payload", http.StatusBadRequest)
		return
	}

	jobData, _ := json.Marshal(payload["job"])
	var job Job
	json.Unmarshal(jobData, &job)
	go w.ExecuteJob(&job)
	
	writer.WriteHeader(http.StatusOK)
	json.NewEncoder(writer).Encode(map[string]string{"status": "accepted"})
}

func (w *WorkerNode) handleJobCancel(writer http.ResponseWriter, r *http.Request) {
	w.mu.Lock()
	if w.currentJob != nil {
		// Stop Job Function Below
		// {Insert Here}

		w.currentJob = nil 
		w.currentJobID = 0
		w.available = true
	}

	w.mu.Unlock()

	writer.WriteHeader(http.StatusOK)
	json.NewEncoder(writer).Encode(map[string]string{"status":"cancelled"})
}

func (w *WorkerNode) ExecuteJob(job *Job) {
	w.mu.Lock()
	w.available = false
	w.currentJob = job
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		w.available = true
		w.currentJob = nil
		w.mu.Unlock()
	}()

	log.Printf("Worker %s executing job: %s", w.ID, job.Command)

	// Simulate job execution
	result, err := w.executeCommand(job.Command)

	// Report back to master
	w.master.OnJobCompleted(job.ID, result, err)
}

func (w *WorkerNode) executeCommand(command string) (string, error) {
	// Simulate work with sleep
	time.Sleep(time.Duration(2+len(command)%5) * time.Second)
	
	// Simulate occasional failures
	if len(command)%7 == 0 {
		return "", fmt.Errorf("simulated job failure")
	}

	return fmt.Sprintf("Command '%s' executed successfully on worker %s", command, w.ID), nil
}

func (w *WorkerNode) IsAvailable() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.available
}

func (w *WorkerNode) GetStatus() WorkerStatus {
	w.mu.RLock()
	defer w.mu.RUnlock()

	status := WorkerStatus{
		ID:        w.ID,
		Available: w.available,
		LastSeen:  w.lastSeen,
	}

	if w.currentJob != nil {
		status.CurrentJob = w.currentJob.ID
	}

	return status
}

func (w *WorkerNode) heartbeatLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.mu.Lock()
			w.lastSeen = time.Now()
			w.mu.Unlock()
		}
	}
}

func (w *WorkerNode) Stop() {
	w.cancel()
}
