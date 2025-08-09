package scheduler

import (
	"container/heap"
	"context"
	. "heapscheduler/jobs"
	"log"
	"os/exec"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Scheduler struct {
	JobQueue JobQueue
	mu       sync.Mutex
	jobMap   map[uint64]*Job // Add a map for fast lookup
	db       *pgxpool.Pool
}

func NewScheduler(db *pgxpool.Pool) *Scheduler {
	h := JobQueue{}
	heap.Init(&h)
	return &Scheduler{
		JobQueue: h,
		jobMap:   make(map[uint64]*Job),
		db:       db,
	}
}

// Adding Jobs
func (s *Scheduler) AddJob(job *Job) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// First persist to DB to get the ID
	if dbErr := s.persistJobToDB(job); dbErr != nil {
		return false // Could log error too
	}

	// Then push into heap and map
	heap.Push(&s.JobQueue, job)
	s.jobMap[job.ID] = job

	return true
}

func (s *Scheduler) persistJobToDB(job *Job) error {
	query := `
        INSERT INTO jobs (name, description, status, command, user, priority, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id;
    `
	now := time.Now()
	return s.db.QueryRow(
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
func (s *Scheduler) CancelJob(id uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Stop Job if it's running (optional, depends on your use case)
	// For now, we just remove it from the queue and update status

	job, ok := s.jobMap[id]
	if !ok || job.Index < 0 || job.Index >= len(s.JobQueue) {
		return false // Job not found or invalid index
	}
	heap.Remove(&s.JobQueue, job.Index) // Remove from Priority Queue
	delete(s.jobMap, id)                // Remove from jobMap

	// Update status in database
	if err := s.updateJobStatusInDB(id, StatusCancelled); err != nil {
		// Optional: log error, but job is already removed in memory
		return false
	}

	return true
}

// Utility
func (s *Scheduler) updateJobStatusInDB(id uint64, status Status) error {
	query := `
		UPDATE jobs
		SET status = $1, updated_at = $2
		WHERE id = $3;
	`
	_, err := s.db.Exec(
		context.Background(),
		query,
		string(status),
		time.Now(),
		id,
	)
	return err
}

// Update Job Priority
func (s *Scheduler) UpdateJobPriority(id uint64, newPriority int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobMap[id]
	if !ok {
		return false // Job not found
	}

	// Update in DB
	if err := s.updateJobPriorityInDB(id, newPriority); err != nil {
		// Optional: log error
		return false
	}

	// Update in memory
	job.Priority = newPriority
	heap.Fix(&s.JobQueue, job.Index) // Reorder the heap

	return true
}

func (s *Scheduler) updateJobPriorityInDB(id uint64, newPriority int) error {
	query := `
		UPDATE jobs
		SET priority = $1, updated_at = $2
		WHERE id = $3;
	`
	_, err := s.db.Exec(
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
func (s *Scheduler) GetJob(id uint64) (*Job, error) {
	s.mu.Lock()
	job, ok := s.jobMap[id]
	s.mu.Unlock()

	if ok {
		return job, nil
	}

	// Fall back to DB
	return s.GetJobFromDB(id)
}

func (s *Scheduler) GetJobFromDB(id uint64) (*Job, error) {
	query := `
		SELECT id, name, description, status, command, user, priority, created_at, updated_at, start_time, end_time
		FROM jobs
		WHERE id = $1;
	`

	row := s.db.QueryRow(context.Background(), query, id)

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

func (s *Scheduler) RunJob(job Job) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if the job is in the queue
	if _, exists := s.jobMap[job.ID]; !exists {
		return false // Job not found
	}

	go func(job *Job) {
		job.StartTime = time.Now()
		job.Status = "Running"
		cmd := exec.Command(job.Command)
		cmd.Run()
		job.EndTTime = time.Now()
		job.Status = "Completed"
	}(s.jobMap[job.ID]) // Pass pointer for updates

	return true
}

func (s *Scheduler) RestoreJobs(pool *pgxpool.Pool) ([]Job, error) {
	// If host running scheduler was rebooted, restore pending and prev running jobs from DB back to priority queue
	rows, err := pool.Query(context.Background(),
		`SELECT id, name, description, status, start_time, end_ttime, command, user, priority, created_at, updated_at, index
         FROM jobs WHERE status = $1 OR status = $2`, StatusRunning, StatusPending)
	if err != nil {
		log.Fatalf("Failed to query running jobs: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var job Job
		err := rows.Scan(
			&job.ID, &job.Name, &job.Description, &job.Status,
			&job.StartTime, &job.EndTTime, &job.Command, &job.User,
			&job.Priority, &job.CreatedAt, &job.UpdatedAt, &job.Index,
		)
		if err != nil {
			log.Printf("Failed to scan job: %v", err)
			continue
		}
		s.AddJob(&job)
	}
}
