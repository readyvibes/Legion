package cluster

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"
)

type WorkerNode struct {
	Node
}

// RegisterWithMaster registers this node with the master node.
func (w *WorkerNode) RegisterWithMaster(masterAddr string, node Node) error {
	// Marshal the 'node' object into JSON format and store the result in 'data'.
	// Any error returned by json.Marshal is ignored.
	data, _ := json.Marshal(node)

	// Send a POST request to the master's /register endpoint with the node data as JSON.
	resp, err := http.Post("http://"+masterAddr+"/register", "application/json", bytes.NewReader(data))
	// If there was an error sending the request, return the error.
	if err != nil {
		return err
	}

	// Ensure the response body is closed after we're done.
	defer resp.Body.Close()
	// Return nil to indicate success.
	return nil
}

// SendHeartbeat sends periodic heartbeats to the master node.
func (w *WorkerNode) SendHeartbeat(masterAddr string, node Node) {
	for {
		data, _ := json.Marshal(node)
		http.Post("http://"+masterAddr+"/heartbeat", "application/json", bytes.NewReader(data))
		time.Sleep(10 * time.Second)
	}
}

// ExampleWorkerMain demonstrates how a worker node could use the above functions.
// Remove or adapt this for your actual worker entrypoint.
func WorkerMain() {
	worker := WorkerNode{
		Node: Node{
			address: "worker1.local",
			port:    8081,
			Role:    "worker",
			Status:  "active",
		},
	}
	masterAddr := "master.local:8080"

	if err := worker.RegisterWithMaster(masterAddr, worker.Node); err != nil {
		panic(err)
	}
	go worker.SendHeartbeat(masterAddr, worker.Node)

	// ...worker logic...
	select {}
}
