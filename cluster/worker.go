package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	. "legion/jobs"
)

type WorkerStatus struct {
	ID         string
	Available  bool
	LastSeen   time.Time
	CurrentJob uint64 // ID of the current job being executed
}

type WorkerNode struct {
	ID           string
	Address      string
	Port         int
	available    bool
	currentJobID uint64
	currentJob   *Job
	lastSeen     time.Time
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	httpsClient  *http.Client
	masterPort   int
}

type WorkerNodeOptions struct {
	Address string
	Port    int
}

func NewWorkerNode(option *WorkerNodeOptions) *WorkerNode {
	addr := "localhost"
	port := 9090

	if option != nil {
		if option.Address != "" {
			addr = option.Address
		}
		if option.Port != 0 {
			port = option.Port
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	id := uuid.New()

	worker := &WorkerNode{
		ID:        id.String(),
		Address:   addr,
		Port:      port, // Port can be set later if needed
		available: true,
		lastSeen:  time.Now(),
		ctx:       ctx,
		cancel:    cancel,
	}

	return worker
}

func (w *WorkerNode) initHTTPSClient() error {
	certFile, err := os.Open("/etc/ssl/ca.cert")
	if err != nil {
		// Handle error
	}
	defer certFile.Close()

	caCert, err := io.ReadAll(certFile)
	if err != nil {
		// Handle error
		log.Fatalf("Error with loading Certificate Authority Cert: %s", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	w.httpsClient = &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    caCertPool,
				MinVersion: tls.VersionTLS12,
			},
		},
	}
	return nil
}

func (w *WorkerNode) Start() error {

	if err := w.initHTTPSClient(); err != nil {
		log.Printf("Warning: Failed to initiate HTTPS client: %v", err)
		return err
	}

	w.ctx, w.cancel = context.WithCancel(context.Background())

	go w.startWorkerServer()
	go w.registerWithMaster()
	go w.heartbeatLoop()

	return nil
}

func (w *WorkerNode) startWorkerServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/job/assign", w.handleJobAssign)
	mux.HandleFunc("/job/cancel", w.handleJobCancel)

	server := &http.Server{ // HTTPS server
		Addr:    ":9090",
		Handler: mux,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	log.Printf("Worker %s server starting on :%d", w.ID, w.Port)
	if err := server.ListenAndServeTLS("/etc/ssl/certs/worker.crt", "/etc/ssl/private/worker.key"); err != nil {
		log.Printf("Worker server error: %v", err)
	}
}

func (w *WorkerNode) handleJobAssign(writer http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(writer, "Invalid JSON", http.StatusBadRequest)
		return
	}

	payload, ok := msg.Payload.(map[string]interface{})
	if !ok {
		http.Error(writer, "Invalid payload", http.StatusBadRequest)
		return
	}

	jobData, _ := json.Marshal(payload["job"])
	var job Job
	json.Unmarshal(jobData, &job)
	go w.ExecuteJob(&job)

	writer.WriteHeader(http.StatusOK)
	json.NewEncoder(writer).Encode(map[string]string{"status": "accepted"})
}

func (w *WorkerNode) ExecuteJob(job *Job) {
	w.mu.Lock()
	w.available = false
	w.currentJob = job
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		w.available = true
		w.currentJob = nil
		w.mu.Unlock()
	}()

	log.Printf("Worker %s executing job: %s", w.ID, job.Command)

	// Simulate job execution
	result, err := w.executeCommand(job.Command)
	if err != nil {
		log.Printf("Encountered the following error: %s", err)
	}

	log.Printf("Result: %s", result)

	// Report back to master
	// w.master.OnJobCompleted(job.ID, result, err)
}

func (w *WorkerNode) executeCommand(command string) (string, error) {
	// Simulate work with sleep
	time.Sleep(time.Duration(2+len(command)%5) * time.Second)

	// Simulate occasional failures
	if len(command)%7 == 0 {
		return "", fmt.Errorf("simulated job failure")
	}

	return fmt.Sprintf("Command '%s' executed successfully on worker %s", command, w.ID), nil
}

func (w *WorkerNode) handleJobCancel(writer http.ResponseWriter, r *http.Request) {
	w.mu.Lock()
	if w.currentJob != nil {
		// Stop Job Function Below
		// {Insert Here}

		w.currentJob = nil
		w.currentJobID = 0
		w.available = true
	}

	w.mu.Unlock()

	writer.WriteHeader(http.StatusOK)
	json.NewEncoder(writer).Encode(map[string]string{"status": "cancelled"})
}

func (w *WorkerNode) registerWithMaster() {
	time.Sleep(2 * time.Second) // Wait for server to start

	url := fmt.Sprintf("http://master.cluster.local:%d/worker/register", w.masterPort)

	msg := Message{
		Type:      MessageTypeRegister,
		WorkerID:  w.ID,
		TimeStamp: time.Now(),
		Payload: RegisterPayload{
			WorkerID: w.ID,
			Address:  w.Address,
			Port:     w.Port,
		},
	}

	for {
		if err := w.sendMessageToMaster(url, msg); err != nil {
			log.Printf("Failed to register with master: %v, retrying in 5s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Printf("Worker %s registered with master", w.ID)
		break
	}
}

func (w *WorkerNode) sendMessageToMaster(url string, msg Message) error {
	w.httpsClient = &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	jsonData, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	resp, err := w.httpsClient.Post(url, "application/json",
		strings.NewReader(string(jsonData)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read and parse response
	var response map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	// Extract master port if present
	if masterPort, exists := response["master_port"]; exists {
		if port, ok := masterPort.(float64); ok { // JSON numbers are float64
			w.masterPort = int(port)
			log.Printf("Master port received: %d", w.masterPort)
		}
	}

	return nil
}

func (w *WorkerNode) sendHeartbeat() {
	url := fmt.Sprintf("http://master.cluster.local:%d/worker/register", w.masterPort)

	w.mu.RLock()
	msg := Message{
		Type:      MessageTypeHeartbeat,
		WorkerID:  w.ID,
		TimeStamp: time.Now(),
		Payload: HeartbeatPayload{
			Available:   w.available,
			CurrentJob:  w.currentJobID,
			CPUUsage:    getCPUUsage(),
			MemoryUsage: getMemoryUsage(),
		},
	}

	w.mu.RUnlock()

	if err := w.sendMessageToMaster(url, msg); err != nil {
		log.Printf("Failed to send heartbeat: %v", err)
	}
}

func (w *WorkerNode) IsAvailable() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.available
}

func (w *WorkerNode) GetStatus() WorkerStatus {
	w.mu.RLock()
	defer w.mu.RUnlock()

	status := WorkerStatus{
		ID:        w.ID,
		Available: w.available,
		LastSeen:  w.lastSeen,
	}

	if w.currentJob != nil {
		status.CurrentJob = w.currentJob.ID
	}

	return status
}

func (w *WorkerNode) heartbeatLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.sendHeartbeat()
		}
	}
}

func (w *WorkerNode) Stop() {
	w.cancel()
}

func getCPUUsage() float64 {
	return 0.0
}

func getMemoryUsage() float64 {
	return 0.0
}
