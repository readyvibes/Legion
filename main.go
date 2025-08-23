package main

import (
	"flag"
	"fmt"
	"os"

	. "legion/cluster"
)

func main() {

	master := flag.Bool("master", false, "Run as master node")
	worker := flag.Bool("worker", false, "Run as worker node")

	flag.Parse()

	if *master && *worker {
		fmt.Println("Error: Cannot run as both master and worker")
		os.Exit(1)
	}

	if !*master && !*worker {
		fmt.Println("Error: Must specify either -master or -worker")
		fmt.Println("Usage:")
		fmt.Println("     ./legion -master      # Run as master node")
		fmt.Println("     ./legion -worker      # Run as worker node")
		os.Exit(1)
	}

	if *master {
		fmt.Println("Starting cluster in master mode")
		// Create cluster
		cluster := NewCluster()
		cluster.Start()
	} else if *worker {
		fmt.Println("Starting cluster in worker mode")
		worker := NewWorkerNode(nil)
		worker.Start()
	}
}
