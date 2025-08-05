package main

import (
	"encoding/json"
	. "heapscheduler/jobs"
	. "heapscheduler/scheduler"
	"net/http"
	"time"
	"strconv"
)


var sched *Scheduler // “I’m declaring a package-level variable named sched that will hold a pointer to a Scheduler.”
					 // This doesn't give it a value yet — it just reserves the name sched at the package level, so it’s accessible in all functions in that file, like your createJobHandler.

func addJobHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var job Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		http.Error(w, "Failed to decode JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	job.Status = StatusPending
	job.CreatedAt = time.Now()
	job.UpdatedAt = time.Now()

}

func cancelJobHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Only DELETE method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var job Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		http.Error(w, "Failed to decode JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Read ID from query param, e.g., /jobs/cancel?id=42
	query := r.URL.Query()
	idStr := query.Get("id")
	if idStr == "" {
		http.Error(w, "Missing job ID", http.StatusBadRequest)
		return
	}

	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid job ID", http.StatusBadRequest)
		return
	}

	if ok := sched.CancelJob(id); !ok {
		http.Error(w, "Job not found or already removed", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Job cancelled successfully"))

}

func main() {
	sched = NewScheduler() // Assign actual scheduler instance to it in main()

	http.HandleFunc("/jobs/add", addJobHandler)
	http.HandleFunc("/jobs/cancel", cancelJobHandler)
	http.ListenAndServe(":8080", nil)
}
