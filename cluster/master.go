package cluster

import (
	"container/heap"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	. "heapscheduler/jobs"
	. "heapscheduler/priorityqueue"

	"github.com/jackc/pgx/v5/pgxpool"
)

type MasterNode struct {
	address  string
	port     int 
	db      *pgxpool.Pool
	jobQueue *JobQueue
	jobMap   map[uint64]*Job // Add a map for fast lookup	
	workers  map[string]*WorkerNode // Workers managed by this master
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewMasterNode(dbURL string, address string) *MasterNode {
	if dbURL == "" {
		log.Println("Connection string is empty, using default settings")
		return nil
	}	

	poolConfig, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return nil
	}

	pool, err1 := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err1 != nil {
		return nil
	}

	h := JobQueue{}
	heap.Init(&h)

	return &MasterNode{
		jobQueue: &h,
		jobMap:   make(map[uint64]*Job),
		db:       pool,
		address: address,
		port: 9090,
	}
}

func (m *MasterNode) Start() error {
	
	m.ctx, m.cancel = context.WithCancel(context.Background())
	
	// Start communication server for workers
	go m.StartCommunicationServer() // Lines 73 - 187

	// Start scheduler loop
	go m.schedulerLoop() // Lines 189 - 259
	
	// Start worker health monitor
	go m.monitorWorkers() // Lines 261 - 286

	log.Println("Master node started")
	return nil
}


func (m *MasterNode) StartCommunicationServer() {

	mux := http.NewServeMux()
	mux.HandleFunc("/worker/register", m.handleWorkerRegister)
	mux.HandleFunc("/worker/heartbeat", m.handleHeartBeat)
	mux.HandleFunc("/worker/job-complete", m.handleJobComplete)

	server := &http.Server{ // HTTPS server
		Addr:    ":9090", // Worker communication port
		Handler: mux,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12, // Force TLS 1.2 or higher
		},
	}

	log.Println("Master communication server starting on :9090")
	if err := server.ListenAndServe(); err != nil {
		log.Printf("Communication server error: %v", err)
	}
}

func (m *MasterNode) handleWorkerRegister(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	payload, ok := msg.Payload.(map[string]interface{})
	if !ok {
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}

	workerID := payload["worker_id"].(string)
	address := payload["address"].(string)
	port := int(payload["port"].(float64))

	m.mu.Lock()

	// Create or update worker
	if _, exists := m.workers[workerID]; !exists { // If workerID does not exist in Workers
		worker := NewWorkerNode(workerID, m.address) // Remember, not part of WorkerNode implementation, only used for MasterNode
		m.workers[workerID] = worker
		log.Printf("Worker %s registered from %s:%d", workerID, address, port)
	} else {
		// Update existing worker
		m.workers[workerID].lastSeen = time.Now()
		m.workers[workerID].available = true
	}

	m.mu.Unlock()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "registered"})

}

func (m *MasterNode) handleHeartBeat(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	m.mu.Lock()

	if worker, exists := m.workers[msg.WorkerID]; exists {
		worker.lastSeen = time.Now()
		
		if payload, ok := msg.Payload.(map[string]interface{}); ok {
			worker.available = payload["available"].(bool)
			if jobID, exists := payload["current_job_id"]; exists && jobID != nil {
				worker.currentJobID = uint64(jobID.(float64))
			} else {
				worker.currentJobID = 0
			}
		}
	}

	m.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (m *MasterNode) handleJobComplete(w http.ResponseWriter, r *http.Request) {
	var msg Message 
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
	}

	payload, ok := msg.Payload.(map[string]interface{})
	if !ok {
		http.Error(w, "Invalid Payload", http.StatusBadRequest)
	}

	jobID := uint64(payload["job_id"].(float64))
	result := ""
	errorMsg := ""

	if r, exists := payload["result"]; exists && r != nil {
		result = r.(string)
	}

	if e, exists := payload["error"]; exists && e != nil {
		errorMsg = e.(string)
	}

	var err error
	if errorMsg != "" {
		err = fmt.Errorf("%s", errorMsg)
	}

	m.OnJobCompleted(jobID, result, err)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "received"})
}

func (m *MasterNode) schedulerLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.scheduleJobs()
		}
	}
}

func (m *MasterNode) scheduleJobs() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find available workers
	availableWorkers := m.getAvailableWorkers()
	if len(availableWorkers) == 0 {
		return
	}

	// Assign jobs to available workers
	for len(availableWorkers) > 0 && m.jobQueue.Len() > 0 {
		job := heap.Pop(m.jobQueue).(*Job)
		worker := availableWorkers[0]
		availableWorkers = availableWorkers[1:]

		// Assign job to worker
		job.Status = StatusRunning
		job.WorkerID = worker.ID
		now := time.Now()
		job.StartTime = now

		// Update database
		m.updateJobStatusInDB(job.ID, StatusRunning)

		m.assignJobToWorker(job, worker)

		log.Printf("Assigned job to worker %s", worker.ID)
	}
}

func (m *MasterNode) getAvailableWorkers() []*WorkerNode {
	var available []*WorkerNode
	for _, worker := range m.workers {
		if worker.IsAvailable() {
			available = append(available, worker)
		}
	}
	return available
}

func (m *MasterNode) updateJobStatusInDB(id uint64, status Status) error {
	query := `
		UPDATE jobs
		SET status = $1, updated_at = $2
		WHERE id = $3;
	`
	_, err := m.db.Exec(
		context.Background(),
		query,
		string(status),
		time.Now(),
		id,
	)
	return err
}

func (m *MasterNode) monitorWorkers() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.checkWorkerHealth()
		}
	}
}

func (m *MasterNode) checkWorkerHealth() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for id, worker := range m.workers {
		if now.Sub(worker.lastSeen) > 60*time.Second {
			log.Printf("Worker %s appears to be down", id)
			// Handle worker failure - could reschedule its jobs
		}
	}
}

// Adding Jobs to queue
func (m *MasterNode) AddJob(job *Job) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// First persist to DB to get the ID
	if dbErr := m.persistJobToDB(job); dbErr != nil {
		return false // Could log error too
	}

	// Then push into heap and map
	heap.Push(m.jobQueue, job)
	m.jobMap[job.ID] = job

	return true
}

func (m *MasterNode) persistJobToDB(job *Job) error {
	query := `
        INSERT INTO jobs (name, description, status, command, user, priority, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id;
    `
	now := time.Now()
	return m.db.QueryRow(
		context.Background(),
		query,
		job.Name,
		job.Description,
		job.Status,
		job.Command,
		job.User,
		job.Priority,
		now,
		now,
	).Scan(&job.ID) // This sets the ID from the DB
	// The QueryRow method returns a row object, which is immediately followed by a call to .Scan(&job.ID). The Scan method attempts to read the first column of the result row into the job.ID field.
}

// Canceling Jobs
func (m *MasterNode) CancelJob(id uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Stop Job if it's running (optional, depends on your use case)
	// For now, we just remove it from the queue and update status

	job, ok := m.jobMap[id]
	if !ok || job.Index < 0 || job.Index >= len(*m.jobQueue) {
		return false // Job not found or invalid index
	}
	heap.Remove(m.jobQueue, job.Index) // Remove from Priority Queue
	delete(m.jobMap, id)                // Remove from jobMap

	// Update status in database
	if err := m.updateJobStatusInDB(id, StatusCancelled); err != nil {
		// Optional: log error, but job is already removed in memory
		return false
	}

	// Find all the workers that are running specified job
	for _, job := range m.jobMap {
		for _, worker := range m.workers {
			if worker.currentJobID == job.ID {
				err := m.cancelJobOnWorker(job, worker)
				if err != nil {
					return false
				}
			}
		}
	}

	return true
}

// Utility


// Update Job Priority
func (m *MasterNode) UpdateJobPriority(id uint64, newPriority int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, ok := m.jobMap[id]
	if !ok {
		return false // Job not found
	}

	// Update in DB
	if err := m.updateJobPriorityInDB(id, newPriority); err != nil {
		// Optional: log error
		return false
	}

	// Update in memory
	job.Priority = newPriority
	heap.Fix(m.jobQueue, job.Index) // Reorder the heap

	return true
}

func (m *MasterNode) updateJobPriorityInDB(id uint64, newPriority int) error {
	query := `
		UPDATE jobs
		SET priority = $1, updated_at = $2
		WHERE id = $3;
	`
	_, err := m.db.Exec(
		context.Background(),
		query,
		newPriority,
		time.Now(),
		id,
	)
	return err
}

func (m *MasterNode) ListJobs() ([]*Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	jobs := make([]*Job, 0, len(m.jobMap))
	for _, job := range m.jobMap {
		jobs = append(jobs, job)
	}
	return jobs, nil
}

// Get Job by ID
// First check in memory, then fall back to DB
func (m *MasterNode) GetJob(id uint64) (*Job, error) {
	m.mu.Lock()
	job, ok := m.jobMap[id]
	m.mu.Unlock()

	if ok {
		return job, nil
	}

	// Fall back to DB
	return m.GetJobFromDB(id)
}

func (m *MasterNode) GetJobFromDB(id uint64) (*Job, error) {
	query := `
		SELECT id, name, description, status, command, user, priority, created_at, updated_at, start_time, end_time
		FROM jobs
		WHERE id = $1;
	`

	row := m.db.QueryRow(context.Background(), query, id)

	var job Job
	err := row.Scan(
		&job.ID,
		&job.Name,
		&job.Description,
		&job.Status,
		&job.Command,
		&job.User,
		&job.Priority,
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.StartTime,
		&job.EndTime,
	)

	if err != nil {
		return nil, err
	}

	return &job, nil
}

func (m *MasterNode) GetQueueLength() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.jobQueue.Len()
}

func (m *MasterNode) OnJobCompleted(jobID uint64, result string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, exists := m.jobMap[jobID]
	if !exists {
		return
	}

	now := time.Now()
	job.EndTime = now

	if err != nil {
		job.Status = StatusFailed
		job.Error = err.Error()
	} else {
		job.Status = StatusCompleted
		job.Result = result
	}

	// Update database
	m.updateJobStatusInDB(job.ID, job.Status)

	log.Printf("Job completed with status %s", job.Status)
}

func (m *MasterNode) RegisterWorker(worker *WorkerNode) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.workers[worker.ID] = worker
	log.Printf("Worker %s registered", worker.ID)
}

func (m *MasterNode) assignJobToWorker(job *Job, worker *WorkerNode) error {
	url := fmt.Sprintf("http://%s:%d/job/assign", worker.Address, worker.Port)

	msg := Message{
		Type: MessageTypeJobAssign,
		WorkerID: worker.ID,
		TimeStamp: time.Now(),
		Payload: JobAssignPayload{
			Job: job,
		},
	}

	return m.sendMessageToWorker(url, msg)

}

func (m *MasterNode) cancelJobOnWorker(job *Job, worker *WorkerNode) error {
	url := fmt.Sprintf("http://%s:%d/job/cancel", worker.Address, worker.Port)

	msg := Message{
		Type: MessageTypeJobCancel,
		WorkerID: worker.ID,
		TimeStamp: time.Now(),
		Payload: JobCancelPayload{
			Job: job,
		},
	}

	return m.sendMessageToWorker(url, msg)
}

func (m *MasterNode) sendMessageToWorker(url string, msg Message) error {
	client := &http.Client{Timeout: 5 * time.Second}

	jsonData, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	resp, err := client.Post(url, "application/json", strings.NewReader(string(jsonData)))
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Worker returned status %d", resp.StatusCode)
	}

	return nil
}