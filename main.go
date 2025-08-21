package main

import (
	. "legion/cluster"
)

func main() {
	// Create cluster
	cluster := NewCluster()
	cluster.Start()
}
