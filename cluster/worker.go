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

type WorkerNode struct {
	ID            string
	Address       string 
	Port 	      int 
	available     bool
	currentJobID  uint64
	currentJob    *Job
	lastSeen      time.Time
	mu            sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	masterAddress string
	masterPort 	  int

	// updates for cli
	cpuUtilization    float64
    memoryUtilization float64 
	jobsRunning       int // amount of jobs in the queue
}

type WorkerStatus struct {
	ID         	      string    
	Available         bool      
	LastSeen          time.Time
	CPUUtilization    float64
    MemoryUtilization float64 
	CurrentJob        uint64 // ID of the current job being executed
	JobsRunning       int // amount of jobs in the queue
}

func NewWorkerNode(id string, masterAddr string, masterPort int) *WorkerNode {
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
		masterAddress: masterAddr,
		masterPort: masterPort,
	}

	return worker
}

func (w *Worker) Start() error {

	m.ctx, m.cancel = context.WithCancel(context.Background()) 

	go w.startWorkerServer()

	go w.register.registerWithMaster()

	go w.heartbeatLoop()

	return nil
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

func (w *WorkerNode) registerWithMaster() {
	time.Sleep(2 * time.Second) // Wait for server to start

	url := fmt.Sprintf("http://%s:%d/worker/register", w.masterAddress, w.masterPort)

	msg := Message{
		Type: MsgTypeRegister,
		WorkerID: w.ID,
		TimeStamp: time.Now(),
		Payload: RegisterPayload{
			WorkerID: w.ID,
			Address: w.Address,
			Port: w.Port,
		},
	}

	for {
		if err := w.sendMessageToMaster(url, msg); err != nil {
			log.Printf("Failed to register with master: %v, retrying in 5s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Printf("Worker %s registered with master", w.ID)
		break
	}
}

func (w *WorkerNode) sendMessageToMaster(url string, msg Message) error {
	client := &http.Client{Timeout: 5 * time.Second}
	
	jsonData, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	resp, err := client.Post(url, "application/json", 
		strings.NewReader(string(jsonData)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (w *WorkerNode) sendHeartbeat() {
	url := fmt.Sprintf("http://%s:%d/worker/heartbeat", w.masterAddress, w.masterPort)

	w.mu.RLock()
	msg := Message{
		Type: MessageTypeHeartbeat,
		WorkerID: w.ID,
		TimeStamp: time.Now(),
		Payload: HeartbeatPayload{
			Available: w.available,
			CurrentJobID: w.currentJobID,
			CPUUsage: getCPUUsage(),
			MemoryUsage: getMemoryUsage()
		},
	}

	w.mu.RUnlock()

	if err := w.sendMessageToMaster(url, msg); err != nil {
		log.Printf("Failed to send heartbeat: %v", err)
	}
}

func (w *WorkerNode) IsAvailable() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.available
}

// get individual worker status
func (w *WorkerNode) GetWorkerStatus() WorkerStatus {
	w.mu.RLock()
	defer w.mu.RUnlock()

	status := WorkerStatus{
		ID:        w.ID,
		Available: w.available,
		LastSeen:  w.lastSeen,
		CPUUtilization: w.cpuUtilization,    
    	MemoryUtilization: w.memoryUtilization, 
		CurrentJob: w.currentJobID,        
		JobsRunning: w.jobsRunning,      
	}

	if w.currentJob != nil {
		status.CurrentJob = w.currentJob.ID
	}

	return status
}

// getAllWorkerStatuses: TODO

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

func getCPUUsage() float64 {
	return 0.0
}

func getMemoryUsage() float64 {
	return 0.0
}