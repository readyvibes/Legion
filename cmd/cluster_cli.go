package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"legion/cluster/master"
	"legion/cluster/cluster"
)

// command does nothing, shows help text
var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Cluster management and status commands",
	Long:  `View and manage your HPC cluster status, including master and worker nodes.`,
}

// cluster status
var clusterStatusCmd = &cobra.Command{
	Use: "cluster-status",
	Short: "",
	Long: "",
	Run: func(cmd *cobra.Command, args []string) {
		ShowClusterStatus() // includes both master and worker status
	},
}

// master status
var masterStatusCmd = &cobra.Command{
	Use: "master-status",
	Short: "",
	Long: "",
	Run: func(cmd *cobra.Command, args []string) {
		ShowMasterStatus() 
	},
}

// worker status
var workerStatusesCmd = &cobra.Command{
	Use: "worker-status",
	Short: "",
	Long: "",
	Run: func(cmd *cobra.Command, args []string) {
		ShowWorkerStatuses() 
	},
}

func ShowClusterStatus() {
	// todo once master and worker are complete
}

func ShowMasterStatus() {
	if verbose {
		fmt.Println("Fetching master node status...")
	}

	masterStatus, err := master.GetMasterStatus()
	if err != nil {
		fmt.Printf("Error getting master status: %v\n", err)
		return
	}

	fmt.Printf("===== Master Node Status =====")
	fmt.Printf("Node ID: %s\n", masterStatus.ID)
	fmt.Printf("CPU Utilization: %.1f%%\n", masterStatus.CPUUtilization)
	fmt.Printf("Memory Utilization: %.1f%%\n", masterStatus.MemoryUtilization)
	fmt.Printf("Jobs Running: %d\n", masterStatus.JobsRunning)
	fmt.Printf("Jobs Pending: %d\n", masterStatus.JobsPending)
	fmt.Printf("==============================")
}

func ShowWorkerStatuses() {
	if verbose {
		fmt.Println("Fetching master node status...")
	}
	
	workerStatuses, err := cluster.getAllWorkerStatuses()
	if err != nil {
		fmt.Printf("Error getting master status: %v\n", err)
		return
	}
	// todo
	// create a method somewhere that retrieves statuses of all active workers (calls getWorkerStatus() on all the workers)
	// output all the statuses
}


func init() {
	rootCLI.AddCommand(clusterCmd)
	rootCLI.AddCommand(clusterStatusCmd)
	rootCLI.AddCommand(masterStatusCmd)
	rootCLI.AddCommand(workerStatusesCmd)
}