package cluster

import (
	"container/heap"
	"context"
	. "heapscheduler/jobs"
	. "heapscheduler/priorityqueue"
	"log"
	"os/exec"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type MasterNode struct {
	db      *pgxpool.Pool
	jobQueue *JobQueue
	jobMap   map[uint64]*Job // Add a map for fast lookup	
	workers  map[string]*WorkerNode // Workers managed by this master
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewMasterNode(dbURL string) *MasterNode {
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
		jobQueue: h,
		jobMap:   make(map[uint64]*Job),
		db:       pool,
	}
}

// Adding Jobs
func (m *MasterNode) AddJob(job *Job) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// First persist to DB to get the ID
	if dbErr := m.persistJobToDB(job); dbErr != nil {
		return false // Could log error too
	}

	// Then push into heap and map
	heap.Push(&m.jobQueue, job)
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
	if !ok || job.Index < 0 || job.Index >= len(m.jobQueue) {
		return false // Job not found or invalid index
	}
	heap.Remove(&m.jobQueue, job.Index) // Remove from Priority Queue
	delete(m.jobMap, id)                // Remove from jobMap

	// Update status in database
	if err := m.updateJobStatusInDB(id, StatusCancelled); err != nil {
		// Optional: log error, but job is already removed in memory
		return false
	}

	return true
}

// Utility
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
	heap.Fix(&m.jobQueue, job.Index) // Reorder the heap

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
		&job.EndTTime,
	)

	if err != nil {
		return nil, err
	}

	return &job, nil
}

func (m *MasterNode) RunJob(job Job) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if the job is in the queue
	if _, exists := m.jobMap[job.ID]; !exists {
		return false // Job not found
	}

	go func(job *Job) {
		job.StartTime = time.Now()
		job.Status = "Running"
		cmd := exec.Command(job.Command)
		cmd.Run()
		job.EndTTime = time.Now()
		job.Status = "Completed"
		m.updateJobStatusInDB(job.ID, StatusCompleted) // Update status in DB
	}(m.jobMap[job.ID]) // Pass pointer for updates

	return true
}

// func (m *MasterNode) RestoreAndRunJobs(pool *pgxpool.Pool) error {
// 	// If host running MasterNode was rebooted, restore pending and prev running jobs from DB back to priority queue
// 	rows, err := pool.Query(context.Background(),
// 		`SELECT id, name, description, status, start_time, end_ttime, command, user, priority, created_at, updated_at, index
//          FROM jobs WHERE status = $1 OR status = $2`, StatusRunning, StatusPending)
// 	if err != nil {
// 		log.Fatalf("Failed to query running jobs: %v", err)
// 	}
// 	defer rows.Close()

// 	for rows.Next() {
// 		var job Job
// 		err := rows.Scan(
// 			&job.ID, &job.Name, &job.Description, &job.Status,
// 			&job.StartTime, &job.EndTTime, &job.Command, &job.User,
// 			&job.Priority, &job.CreatedAt, &job.UpdatedAt, &job.Index,
// 		)
// 		if err != nil {
// 			log.Printf("Failed to scan job: %v", err)
// 			continue
// 		}
// 		m.AddJob(&job)
// 	}

// 	runningJobs := m.FindRunningJobs()

// 	for i := 0; i < len(runningJobs); i++ {
// 		job := &runningJobs[i]
// 		m.RunJob(*job) // Start the job
// 	}

// 	return nil
// }

func (m *MasterNode) getAvailableWorkers() []*WorkerNode {
	var available []*WorkerNode
	for _, worker := range m.workers {
		if worker.IsAvailable() {
			available = append(available, worker)
		}
	}
	return available
}

func (m *MasterNode) GetQueueLength() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.jobQueue.Len()
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

		// Send job to worker
		go worker.ExecuteJob(job)

		log.Printf("Assigned job %s to worker %s", job.ID, worker.ID)
	}
}