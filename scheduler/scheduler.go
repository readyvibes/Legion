package scheduler

import (
	"container/heap"
	. "heapscheduler/jobs"
	"sync"
	"time"
	"os/exec"
)

type Scheduler struct {
	jobQueue JobQueue
	mu	     sync.Mutex
	jobMap   map[uint64]*Job // Add a map for fast lookup
}

func NewScheduler() *Scheduler {
	h := JobQueue{}
	heap.Init(&h)
	return &Scheduler{
		jobQueue: h,
		jobMap:  make(map[uint64]*Job),
	}
}

func (s *Scheduler) AddJob(job *Job) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Push job into the heap
	heap.Push(&s.jobQueue, job)
	s.jobMap[job.ID] = job 

	return true // Always returns true since heap.Push does not fail
}

func (s *Scheduler) CancelJob(id uint64) bool {
    s.mu.Lock()
    defer s.mu.Unlock()

    job, ok := s.jobMap[id]
	if !ok || job.Index < 0 || job.Index >= len(s.jobQueue) {
		return false // Job not found or invalid index
	}
	heap.Remove(&s.jobQueue, job.Index)
	delete(s.jobMap, id) // Remove from jobMap
	return true
}

func (s *Scheduler) UpdateJobPriority(id uint64, newPriority int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobMap[id]
	if !ok {
		return false // Job not found
	}

	job.Priority = newPriority

	//heap.Fix() is a generic heap re-balancing algorithm written by the Go team that uses your methods (like Less() and Swap()) to do the actual work.
	heap.Fix(&s.jobQueue, job.Index) // Reorder the heap 
	// You're passing job.Index to heap.Fix() because that value represents the job’s current position in the heap array (s.jobQueue), and heap.Fix() needs to know where in the heap the changed element is so it can reheapify properly.
	
	return true
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