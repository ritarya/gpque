// Package source defines the ingestion interface for the Streamer.
// Each data source (CSV file, HTTP scrape, gRPC stream, Kafka bridge, WebSocket)
// implements Source and produces TelemetryRecords.  Adding a new source type
// requires only a new implementation here — no changes to the queue, collector,
// or API Gateway.
package source

import (
	"context"

	"gpqueue/internal/model"
)

// Source produces TelemetryRecords from an underlying data stream.
// Finite sources (e.g. CSV) loop indefinitely rather than returning io.EOF.
type Source interface {
	// Next blocks until the next record is available, then returns it.
	Next(ctx context.Context) (*model.TelemetryRecord, error)
	Close() error
}
