package source_test

import (
	"context"
	"os"
	"testing"

	"gpqueue/internal/source"
)

const testCSVContent = `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
2025-07-18T20:42:34Z,DCGM_FI_DEV_GPU_UTIL,0,nvidia0,GPU-abc123,NVIDIA H100 80GB HBM3,host-001,,,,97.0,driver_version=535
2025-07-18T20:42:35Z,DCGM_FI_DEV_POWER_USAGE,1,nvidia1,GPU-def456,NVIDIA H100 80GB HBM3,host-001,,,,250.5,driver_version=535
`

func writeTempCSV(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "test-*.csv")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return f.Name()
}

func TestNewCSV_Success(t *testing.T) {
	path := writeTempCSV(t, testCSVContent)
	s, err := source.NewCSV(path, "streamer-0")
	if err != nil {
		t.Fatalf("NewCSV: %v", err)
	}
	s.Close()
}

func TestNewCSV_FileNotFound(t *testing.T) {
	_, err := source.NewCSV("/nonexistent/path/to/file.csv", "streamer-0")
	if err == nil {
		t.Fatal("NewCSV: expected error on missing file, got nil")
	}
}

func TestCSVSource_Next_SetsFields(t *testing.T) {
	path := writeTempCSV(t, testCSVContent)
	s, _ := source.NewCSV(path, "streamer-0")
	defer s.Close()

	rec, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if rec.ID == "" {
		t.Error("Next: record ID should be non-empty")
	}
	if rec.ProcessedAt.IsZero() {
		t.Error("Next: ProcessedAt should be set")
	}
	if rec.StreamerID != "streamer-0" {
		t.Errorf("StreamerID: want streamer-0, got %q", rec.StreamerID)
	}
	if rec.MetricName != "DCGM_FI_DEV_GPU_UTIL" {
		t.Errorf("MetricName: want DCGM_FI_DEV_GPU_UTIL, got %q", rec.MetricName)
	}
}

func TestCSVSource_LoopsOnEOF(t *testing.T) {
	path := writeTempCSV(t, testCSVContent)
	s, _ := source.NewCSV(path, "streamer-0")
	defer s.Close()

	s.Next(context.Background()) // row 1
	s.Next(context.Background()) // row 2

	// 3rd call should loop back to row 1
	rec, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("Next after loop: %v", err)
	}
	if rec == nil {
		t.Fatal("Next after loop: expected record, got nil")
	}
	if rec.MetricName != "DCGM_FI_DEV_GPU_UTIL" {
		t.Errorf("loop restart: want first row back, got MetricName=%q", rec.MetricName)
	}
}

func TestCSVSource_ContextCancelled(t *testing.T) {
	path := writeTempCSV(t, testCSVContent)
	s, _ := source.NewCSV(path, "streamer-0")
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.Next(ctx)
	if err == nil {
		t.Fatal("Next with cancelled context: expected error, got nil")
	}
}

func TestCSVSource_Close(t *testing.T) {
	path := writeTempCSV(t, testCSVContent)
	s, _ := source.NewCSV(path, "streamer-0")
	if err := s.Close(); err != nil {
		t.Errorf("Close: unexpected error: %v", err)
	}
}
