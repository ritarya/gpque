package model

import "time"

// TelemetryRecord is the canonical message flowing through the pipeline.
// ProcessedAt is the wall-clock time injected by the streamer at ingestion;
// SourceTimestamp is the original DCGM CSV value kept for auditability only.
type TelemetryRecord struct {
	ID              string    `json:"id"`
	ProcessedAt     time.Time `json:"processed_at"`
	SourceTimestamp string    `json:"source_timestamp"`
	MetricName      string    `json:"metric_name"`
	GpuID           int       `json:"gpu_id"`
	Device          string    `json:"device"`
	UUID            string    `json:"uuid"`
	ModelName       string    `json:"model_name"`
	Hostname        string    `json:"hostname"`
	Container       *string   `json:"container,omitempty"`
	Pod             *string   `json:"pod,omitempty"`
	Namespace       *string   `json:"namespace,omitempty"`
	Value           float64   `json:"value"`
	LabelsRaw       string    `json:"labels_raw"`
	StreamerID      string    `json:"streamer_id"`
}

// GPU is a distinct physical GPU tracked in the gpus table.
type GPU struct {
	UUID      string    `json:"uuid"`
	GpuID     int       `json:"gpu_id"`
	Device    string    `json:"device"`
	ModelName string    `json:"model_name"`
	Hostname  string    `json:"hostname"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
}

// Message is a raw envelope returned by the queue service.
type Message struct {
	Offset      int64
	Payload     []byte
	PublishedAt time.Time
	ProducerID  string
}
