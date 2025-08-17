package main

import (
	"log"
	. "heapscheduler/cluster"
)

func main() {
	// Create cluster
	cluster := NewCluster("postgres://user:password@localhost/scheduler?sslmode=disable")

	log.Fatal(cluster.Start())
}