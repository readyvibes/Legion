package main

import (
	"log"
	"fmt"
	"github.com/spf13/cobra"
	"os"

	. "heapscheduler/cluster"
)

var (
	// Global flags
	verbose bool
	output  string
	mode    string  // master or worker
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "legion",
	Short: "A distributed service that can run as master or worker",
	Long: `A distributed service application that can operate in two modes:
- Master: Coordinates work and manages workers
- Worker: Processes tasks assigned by the master

Use the --mode flag to specify which service type to run.`,
	Run: func(cmd *cobra.Command, args []string) {
		if verbose {
			fmt.Printf("Starting in verbose mode, service type: %s\n", mode)
		}
		
		switch mode {
		case "master":
			master := NewMasterNode("testSQLServer")
			master.Start()
		case "worker":
			worker := NewWorkerNode()
			worker.Start()
		default:
			fmt.Printf("Invalid mode: %s. Must be 'master' or 'worker'\n", mode)
			os.Exit(1)
		}
	},
}

func main() {
	// Create cluster
	cluster := NewCluster("postgres://user:password@localhost/scheduler?sslmode=disable")

	log.Fatal(cluster.Start())
}