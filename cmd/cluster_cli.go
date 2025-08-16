package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	. "heapscheduler/cluster"
)

var master *MasterNode

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
	fmt.Printf("WIP")
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
	fmt.Printf("CPU Utilization: %.2f%%\n", masterStatus.CPUUtilization)
	fmt.Printf("Memory Utilization: %.2f%%\n", masterStatus.MemoryUtilization)
	fmt.Printf("Jobs Running: %d\n", masterStatus.JobsRunning)
	fmt.Printf("Jobs Pending: %d\n", masterStatus.JobsPending)
	fmt.Printf("==============================")
}

func ShowWorkerStatuses() {
	if verbose {
		fmt.Println("Fetching master node status...")
	}
	
	workerStatuses, err := master.GetAllWorkerStatuses()
	if err != nil {
		fmt.Printf("Error getting master status: %v\n", err)
		return
	}

	fmt.Printf("===== Worker Node Status =====\n")
	for _, status := range workerStatuses {
		fmt.Printf("Worker ID %d: CPU: %.2f, Memory Utilization: %.2f, Available: %t, Last Seen: %s, Jobs Running: %d\n",
			status.ID, status.CPUUtilization, status.MemoryUtilization, status.Available, status.LastSeen, status.JobsRunning)
	}
	fmt.Printf("==============================")

}

func init() {
	rootCLI.AddCommand(clusterCmd)
	rootCLI.AddCommand(clusterStatusCmd)
	rootCLI.AddCommand(masterStatusCmd)
	rootCLI.AddCommand(workerStatusesCmd)
}