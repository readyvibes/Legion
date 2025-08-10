package cluster

import (
	"time"
	"sync"
	"context"
	. "heapscheduler/jobs"
	"log"
	"fmt"
)

type WorkerStatus struct {
	ID         string    
	Available  bool      
	LastSeen   time.Time 
	CurrentJob uint64 // ID of the current job being executed
}

type WorkerNode struct {
	ID         string
	master     *MasterNode
	available  bool
	currentJob *Job
	lastSeen   time.Time
	mu         sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewWorkerNode(id string, master *MasterNode) *WorkerNode {
	ctx, cancel := context.WithCancel(context.Background())
	
	worker := &WorkerNode{
		ID:        id,
		master:    master,
		available: true,
		lastSeen:  time.Now(),
		ctx:       ctx,
		cancel:    cancel,
	}

	// Start heartbeat
	go worker.heartbeatLoop()

	return worker
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

	log.Printf("Worker %s executing job %s: %s", w.ID, job.ID, job.Command)

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
