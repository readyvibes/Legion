package jobs

import (
	"time"
)

type Status string

const (
	StatusPending   Status = "Pending"
	StatusRunning   Status = "Running"
	StatusCompleted Status = "Completed"
	StatusCancelled Status = "Cancelled"
)

type Job struct {
	ID          uint64
	Name        string
	Description string
	Status      Status
	StartTime   time.Time
	EndTTime    time.Time
	Command     string
	User        string
	Priority    int
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Index       int // Index in the priority queue
	WorkerID    string // ID of the worker executing the job
}

func (j *Job) UpdateStatus(status Status) {
	j.Status = status
	j.UpdatedAt = time.Now()
}

func (j *Job) SetStartTime(startTime time.Time) {
	j.StartTime = startTime
	j.UpdatedAt = time.Now()
}

func (j *Job) SetEndTime(endTime time.Time) {
	j.EndTTime = endTime
	j.UpdatedAt = time.Now()
}

type NewJobRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Command     string `json:"command"`
	User        string `json:"user"`
	Priority    *int    `json:"priority"`
}

func NewJob(req NewJobRequest) *Job {
    now := time.Now()
    priority := 10 // default
    if req.Priority != nil {
        priority = *req.Priority
    }
    return &Job{
        Name:        req.Name,
        Description: req.Description,
        Command:     req.Command,
        User:        req.User,
        Priority:    priority,
        Status:      StatusPending,
        CreatedAt:   now,
        UpdatedAt:   now,
        Index:       -1,
    }
}
