package main

import (
	"context"
	"encoding/json"
	"flag"
	"heapscheduler/db"
	. "heapscheduler/jobs"
	. "heapscheduler/scheduler"
	"log"
	"net/http"
	"strconv"
)

var sched *Scheduler // “I’m declaring a package-level variable named sched that will hold a pointer to a Scheduler.”
// This doesn't give it a value yet — it just reserves the name sched at the package level, so it’s accessible in all functions in that file, like your createJobHandler.

func addJobHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// In addJobHandler (main.go)
	var req NewJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Failed to decode JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	job := NewJob(req)
	success := sched.AddJob(job)
	if !success {
		http.Error(w, "Failed to add job", http.StatusInternalServerError)
		return
	}
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

func updateJobPriorityHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "Only PUT method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var job Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		http.Error(w, "Failed to decode JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Read ID from query param, e.g., /jobs/priority?id=42&priority=10
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

	newPriorityStr := query.Get("priority")
	if newPriorityStr == "" {
		http.Error(w, "Missing new priority", http.StatusBadRequest)
		return
	}

	newPriority, err := strconv.Atoi(newPriorityStr)
	if err != nil {
		http.Error(w, "Invalid new priority", http.StatusBadRequest)
		return
	}

	if ok := sched.UpdateJobPriority(id, newPriority); !ok {
		http.Error(w, "Job not found or already removed", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Job priority updated successfully"))
}

func runMaster() {
	// Initialize DB, scheduler, API, etc.
	// Accept jobs, schedule them, assign to workers
	// 1. Connect to DB
	pool, err := db.ConnectDB()
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer pool.Close()

	// 2. Run DB migration (creates jobs table)
	if err := db.Migrate(pool); err != nil {
		log.Fatalf("Failed to run migration: %v", err)
	}

	// 3. Initialize Scheduler with DB connection
	sched = NewScheduler(pool) // Assign actual scheduler instance to it in main()

	var jobCountDB int
	jobErr := pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM jobs").Scan(&jobCountDB)

	if jobErr != nil {
		log.Fatalf("Failed to count jobs in DB: %v", jobErr)
	}

	if jobCountDB > 0 && len(sched.JobQueue) == 0  {
		// 4. Restore jobs from DB back to into priority queue
		log.Printf("Restoring %d jobs from DB into scheduler...", jobCountDB)
		sched.RestoreJobs(pool)
	}

	http.HandleFunc("/jobs/add", addJobHandler)
	http.HandleFunc("/jobs/cancel", cancelJobHandler)
	http.HandleFunc("/jobs/priority", updateJobPriorityHandler)
	http.ListenAndServe(":8080", nil)
}

func runWorker() {
	// Connect to master, poll for jobs, execute jobs, report status
	// This would typically involve connecting to the master node's API
	// and fetching jobs to execute.
	// For simplicity, this is left as a placeholder.
	log.Println("Worker started, waiting for jobs...")
}

func main() {
	// Option 1: Use a command-line flag
	role := flag.String("role", "worker", "Node role: master or worker")
	flag.Parse()

	// Option 2: Or use an environment variable
	// role := os.Getenv("NODE_ROLE")

	switch *role {
	case "master":
		runMaster()
	case "worker":
		runWorker()
	default:
		log.Fatalf("Invalid role: %s. Use 'master' or 'worker'.", *role)
	}
}
