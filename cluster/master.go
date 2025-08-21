package cluster

import (
	"container/heap"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	. "legion/jobs"
	. "legion/priorityqueue"

	"github.com/jackc/pgx/v5/pgxpool"
)

type MasterNode struct {
	db              *pgxpool.Pool
	jobQueue        *JobQueue
	jobMap          map[uint64]*Job        // Add a map for fast lookup
	workers         map[string]*WorkerNode // Workers managed by this master
	mu              sync.Mutex
	ctx             context.Context
	cancel          context.CancelFunc
	httpsClient     *http.Client
	logger          *slog.Logger
	serverLogger    *slog.Logger
	schedulerLogger *slog.Logger
	monitorLogger   *slog.Logger
}

func NewMasterNode(pool *pgxpool.Pool) *MasterNode {

	h := JobQueue{}
	heap.Init(&h)

	masterLog, err := os.OpenFile("/var/log/legion/master.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644) 
	if err != nil {
		panic(err)
	}

	serverLogFile, err := os.OpenFile("/var/log/legion/master-server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	defer serverLogFile.Close()

	schedulerLogFile, err := os.OpenFile("/var/log/legion/master-scheduler.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	defer schedulerLogFile.Close()

	monitorLogFile, err := os.OpenFile("/var/log/legion/master-monitor.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	defer monitorLogFile.Close()

	return &MasterNode{
		jobQueue: &h,
		jobMap:   make(map[uint64]*Job),
		db:       pool,
		logger:   slog.New(slog.NewTextHandler(masterLog, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
		serverLogger: slog.New(slog.NewTextHandler(serverLogFile, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
		schedulerLogger: slog.New(slog.NewTextHandler(schedulerLogFile, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
		monitorLogger: slog.New(slog.NewTextHandler(monitorLogFile, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	}
}

func (m *MasterNode) Start() error {
	m.logger.Info("Setting Up HTTPS Client on Master Host")
	if err := m.initHTTPSClient(); err != nil {
		m.logger.Error("Failed to Initialize HTTPS Client on Master Host")
		return err
	}
	m.logger.Info("Completed Setup For HTTPS Client on Master Host")

	m.ctx, m.cancel = context.WithCancel(context.Background())

	// Start communication server for workers
	m.logger.Info("Starting Master Server")
	m.serverLogger.Info("Starting Master Server")
	go m.StartCommunicationServer() // Lines 73 - 187

	// Start scheduler loop
	m.logger.Info("Starting Master Server")
	m.schedulerLogger.Info("Starting Master Scheduler Loop")
	go m.schedulerLoop()

	// Start worker health monitor
	m.logger.Info("Starting Master Health Monitoring")
	m.monitorLogger.Info("Starting Master Health Monitoring")
	go m.monitorWorkers()

	m.logger.Info("Master Node started")
	return nil
}

func (m *MasterNode) initHTTPSClient() error {
	certFile, err := os.Open("/etc/ssl/ca.cert")
	if err != nil {
		// Handle error
		m.logger.Error("Failed to open CA (Certificate Authority) Certificate")
	}
	defer certFile.Close()

	caCert, err := io.ReadAll(certFile)
	if err != nil {
		// Handle error
		m.logger.Error("Failed to read CA (Certificate Authority) Certificate")
	}

	m.logger.Info("Loading Certificates")
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	m.logger.Info("Completed Certificate Setup")

	m.logger.Info("Initializing HTTPS Client")
	m.httpsClient = &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    caCertPool,
				MinVersion: tls.VersionTLS12,
			},
		},
	}
	m.logger.Info("Completed HTTPS Client Setup")
	return nil
}

func (m *MasterNode) StartCommunicationServer() {
	m.serverLogger.Info("Setting Up HTTPS Server")
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
	m.serverLogger.Info("HTTPS Server Setup Completed")

	if err := server.ListenAndServeTLS("/etc/ssl/certs/master.crt", "/etc/ssl/private/master.key"); err != nil {
		log.Printf("Communication server error: %v", err)
	}
	m.serverLogger.Info("Master HTTPS Server Starting on :9090")
}

func (m *MasterNode) handleWorkerRegister(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		m.serverLogger.Error("Invalid JSON")
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	payload, ok := msg.Payload.(map[string]interface{})
	if !ok {
		m.serverLogger.Error("Invalid Payload")
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}

	workerID := payload["worker_id"].(string)
	address := payload["address"].(string)

	m.serverLogger.Info("Received Register From WorkerNode")
	m.serverLogger.Info("Worker started", slog.String("workerID", workerID), slog.String("address", address))
	m.mu.Lock()

	// Create or update worker
	if _, exists := m.workers[workerID]; !exists { // If workerID does not exist in Workers
		worker := NewWorkerNode(&address) // Remember, not part of WorkerNode implementation, only used for MasterNode
		m.workers[workerID] = worker
		m.serverLogger.Info("Added Worker to Workers", slog.String("workerID", workerID), slog.String("address", address))
	} else {
		// Update existing worker
		m.serverLogger.Info("Worker has already been added to Workers")
		m.workers[workerID].lastSeen = time.Now()
		m.workers[workerID].available = true
		m.serverLogger.Info("Updated worker status instead")
	}

	m.mu.Unlock()

	response := map[string]interface{}{
		"status":      "registered",
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (m *MasterNode) handleHeartBeat(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		m.serverLogger.Error("Invalid JSON")
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	m.mu.Lock()

	if worker, exists := m.workers[msg.WorkerID]; exists {
		worker.lastSeen = time.Now()
		m.serverLogger.Info("Received HeartBeat from", slog.String("Worker", worker.ID))

		if payload, ok := msg.Payload.(map[string]interface{}); ok {
			worker.available = payload["available"].(bool)
			m.serverLogger.Info("Updated Available Status for", slog.String("Worker", worker.ID))
			
			if jobID, exists := payload["current_job_id"]; exists && jobID != nil {
				worker.currentJobID = uint64(jobID.(float64))
				m.serverLogger.Info("Current JobID From Worker", slog.String("Worker", worker.ID))
			} else {
				worker.currentJobID = 0
			}
		}
		m.serverLogger.Info("Updated Status of Worker:", slog.String("Worker", worker.ID) )
	}
	
	m.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (m *MasterNode) handleJobComplete(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		m.serverLogger.Error("Invalid JSON")
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
	}

	payload, ok := msg.Payload.(map[string]interface{})
	if !ok {
		m.serverLogger.Error("Invalid Payload")
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

	m.serverLogger.Info("Job Completed")
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

		m.schedulerLogger.Info("Running job", 
			slog.String("jobID", strconv.FormatUint(job.ID, 10)),
			slog.String("workerID", worker.ID))
		
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
	m.monitorLogger.Info("Monitoring Status of Workers")
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
			m.monitorLogger.Error("Worker appears to be down:", slog.String("workerID", id))
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
		m.schedulerLogger.Error("Add Job Failed: Failed to add job to LegionDB", slog.String("jobID", strconv.FormatUint(job.ID, 10)),)
		return false // Could log error too
	}

	// Then push into heap and map
	heap.Push(m.jobQueue, job)
	m.jobMap[job.ID] = job
	m.schedulerLogger.Info("Add Job Successful:", slog.String("jobID", strconv.FormatUint(job.ID, 10)))
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
		m.schedulerLogger.Error("Cancel Job Failed: Job Not Found", slog.String("jobID", strconv.FormatUint(id, 10)))
		return false // Job not found or invalid index
	}
	heap.Remove(m.jobQueue, job.Index) // Remove from Priority Queue
	delete(m.jobMap, id)               // Remove from jobMap

	// Update status in database
	if err := m.updateJobStatusInDB(id, StatusCancelled); err != nil {
		// Optional: log error, but job is already removed in memory
		m.schedulerLogger.Error("Cancel Job Failed: Failed to Update Job Status in Legion DB", slog.String("jobID", strconv.FormatUint(id, 10)))
		return false
	}

	// Find all the workers that are running specified job
	
	for _, worker := range m.workers {
		if worker.currentJobID == id {
			err := m.cancelJobOnWorker(job, worker)
			if err != nil {
				m.schedulerLogger.Error("Cancel Job Failed: Failed to cancel job", slog.String("jobID", strconv.FormatUint(id, 10)))
				return false
			}
		}
	}
	m.schedulerLogger.Info("Cancel Job Successful:", slog.String("jobID", strconv.FormatUint(id, 10)))
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
	url := fmt.Sprintf("http://%s:9090/job/assign", worker.Address)

	msg := Message{
		Type:      MessageTypeJobAssign,
		WorkerID:  worker.ID,
		TimeStamp: time.Now(),
		Payload: JobAssignPayload{
			Job: job,
		},
	}

	return m.sendMessageToWorker(url, msg)

}

func (m *MasterNode) cancelJobOnWorker(job *Job, worker *WorkerNode) error {
	url := fmt.Sprintf("http://%s:9090/job/cancel", worker.Address)

	msg := Message{
		Type:      MessageTypeJobCancel,
		WorkerID:  worker.ID,
		TimeStamp: time.Now(),
		Payload: JobCancelPayload{
			Job: job,
		},
	}

	return m.sendMessageToWorker(url, msg)
}

func (m *MasterNode) sendMessageToWorker(url string, msg Message) error {
	jsonData, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	resp, err := m.httpsClient.Post(url, "application/json", strings.NewReader(string(jsonData)))
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Worker returned status %d", resp.StatusCode)
	}

	return nil
}
