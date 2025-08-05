package scheduler

import (
    "heapscheduler/jobs"
)

// JobQueue is a min-heap of *Job based on Priority (lower value = higher priority)
type JobQueue []*jobs.Job

// Len returns the number of jobs in the heap.
// Required by sort.Interface and heap.Interface
func (h JobQueue) Len() int {
    return len(h)
}

// Less determines the ordering of jobs in the heap.
// This creates a min-heap: jobs with lower Priority are "smaller" (i.e., higher priority)
func (h JobQueue) Less(i, j int) bool {
    return h[i].Priority < h[j].Priority
}

// Swap exchanges two jobs in the heap and updates their index fields.
// Required by sort.Interface
func (h JobQueue) Swap(i, j int) {
    h[i], h[j] = h[j], h[i]
    h[i].Index = i
    h[j].Index = j
}

// Push adds a new job to the heap.
// Required by heap.Interface
func (h *JobQueue) Push(x interface{}) {
    job := x.(*jobs.Job)        // Type assertion: x must be a *jobs.Job
    job.Index = len(*h)         // Record the job's index in the heap
    *h = append(*h, job)        // Add job to the underlying slice
}

// Pop removes and returns the job with the highest priority (lowest Priority value).
// Required by heap.Interface
func (h *JobQueue) Pop() interface{} {
    old := *h
    n := len(old)
    job := old[n-1]             // Get the last job (root after reordering)
    job.Index = -1              // Invalidate its index (useful for safety/cancel logic)
    *h = old[0 : n-1]           // Shrink the slice
    return job
}
