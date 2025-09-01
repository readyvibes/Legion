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
	StatusFailed    Status = "Failed"
)

type Job struct {
	ID          uint64
	Name        string
	Description string
	Status      Status
	StartTime   time.Time
	EndTime    time.Time
	Command     string
	Username    string
	Priority    int
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Index       int // Index in the priority queue
	WorkerID    string // ID of the worker executing the job
	Result 		string // Result of the job execution
	Error       string
	GitRepoLink string    // Repository URL for git-based jobs
    WorkingDir  string    // Working directory for job execution
}


type NewJobRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Command     string `json:"command"`
	Username    string `json:"username"`
	Priority    *int    `json:"priority"`

	// optional
	GitRepoLink string `json:"git_repo_link,omitempty"` // https link, ex: https://github.com/readyvibes/Legion.git
	WorkingDir  string `json:"working_dir,omitempty"` // relative to cloud host
}

func NewJob(req NewJobRequest) *Job {
    now := time.Now()
    priority := 10 // default
    if req.Priority != nil {
        priority = *req.Priority
    }

	gitRepoLink := ""
	if req.GitRepoLink != "" {
		gitRepoLink = req.GitRepoLink
	}

	workDir := "home/" + req.Username // default working dir in google cloud is /home/username
	if req.WorkingDir != "" {
		workDir = req.WorkingDir
	}

    return &Job{
        Name:        req.Name,
        Description: req.Description,
        Command:     req.Command,
        Username:    req.Username,
        Priority:    priority,
        Status:      StatusPending,
        CreatedAt:   now,
        UpdatedAt:   now,
        Index:       -1,

		// optional fields
		GitRepoLink: gitRepoLink,
		WorkingDir:  workDir,
    }
}
