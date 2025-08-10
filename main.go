package main

import (
	"log"
	. "heapscheduler/cluster"
)

func main() {
	// Create cluster
	cluster := NewCluster("postgres://user:password@localhost/scheduler?sslmode=disable")

	// Add some workers
	worker1 := cluster.AddWorker("worker-1")
	worker2 := cluster.AddWorker("worker-2")
	worker3 := cluster.AddWorker("worker-3")

	// Start workers (in real implementation, these might be separate processes)
	go func() {
		log.Printf("Worker %s started", worker1.ID)
		select {} // Keep alive
	}()

	go func() {
		log.Printf("Worker %s started", worker2.ID)
		select {} // Keep alive
	}()

	go func() {
		log.Printf("Worker %s started", worker3.ID)
		select {} // Keep alive
	}()

	// Start cluster (this will block)
	log.Fatal(cluster.Start())
}