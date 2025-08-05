package jobs

import (
	"time"
)

type Status string

const (
	StatusPending  Status = "Pending"
	StatusRunning  Status = "Running"
	StatusCompleted Status = "Completed"	
	StatusCancelled Status = "Cancelled"
)

type Job struct {
	ID          uint64    `json:"id"`
	Name		string    `json:"name"`
	Description string    `json:"description"`
	Status      Status    `json:"status"`
	StartTime   time.Time `json:"start_time,omitempty"`
	EndTTime    time.Time `json:"end_time,omitempty"`
	Command     string    `json:"command"`
	User		string    `json:"user"`
	Priority    int 	  `json:"priority"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Index		int       `json:"-"` // Index in the priority queue 
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
