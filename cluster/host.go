package cluster


type Host struct {	
	Address string // Host address (IP or hostname)
	Port    int    // Port number for the host	
	Role    string // Role of the host (e.g., "worker", "manager")
	Status  string // Status of the host (e.g., "active", "inactive")	
}