package cluster

import (
	"net"
	"time"

	. "legion/jobs"
)

type MessageType string

const (
	MessageTypeRegister    MessageType = "register"
	MessageTypeHeartbeat   MessageType = "heartbeat"
	MessageTypeJobAssign   MessageType = "job_assign"
	MessageTypeJobComplete MessageType = "job_complete"
	MessageTypeJobCancel   MessageType = "job_cancel"
)

type Message struct {
	Type      MessageType `json:"type"`
	WorkerID  string      `json:"worker_id,omitempty"`
	TimeStamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload,omitempty"`
}

type RegisterPayload struct {
	WorkerID string `json:"worker_id"`
	Address  string `json:"address"`
}

type HeartbeatPayload struct {
	Available   bool    `json:"available"`
	CurrentJob  uint64  `json:"current_job,omitempty"`
	CPUUsage    float64 `json:"cpu_usage,omitempty"`
	MemoryUsage float64 `json:"memory_usage,omitempty"`
}

type JobAssignPayload struct {
	Job *Job `json:"job"`
}

type JobCancelPayload struct {
	Job *Job `json:"job"`
}

type JobCompletePayload struct {
	JobID  uint64 `json:"job_id"`
	Result string `json:"result"`
	Error  string `json:"error,omitempty"`
}

func getLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "localhost"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}
