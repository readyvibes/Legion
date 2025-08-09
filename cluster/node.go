package cluster


type Node struct {
	address string
	port    int
	Role    string // "master" or "worker"
	Status  string // "active", "inactive", etc.
}