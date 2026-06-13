package csv_test

import (
	"io"
	"strings"
	"testing"

	csvparser "gpqueue/internal/csv"
)

const validCSV = `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
2025-07-18T20:42:34Z,DCGM_FI_DEV_GPU_UTIL,0,nvidia0,GPU-abc123,NVIDIA H100 80GB HBM3,host-001,,,,97.0,driver_version=535
2025-07-18T20:42:35Z,DCGM_FI_DEV_POWER_USAGE,1,nvidia1,GPU-def456,NVIDIA H100 80GB HBM3,host-001,mycontainer,mypod,mynamespace,250.5,driver_version=535
`

func TestNew_ValidHeader(t *testing.T) {
	p, err := csvparser.New(strings.NewReader(validCSV))
	if err != nil {
		t.Fatalf("New: unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("New: returned nil parser")
	}
}

func TestNew_EmptyInput(t *testing.T) {
	_, err := csvparser.New(strings.NewReader(""))
	if err == nil {
		t.Fatal("New: expected error on empty input, got nil")
	}
}

func TestNext_BasicRow(t *testing.T) {
	p, _ := csvparser.New(strings.NewReader(validCSV))
	rec, err := p.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if rec.MetricName != "DCGM_FI_DEV_GPU_UTIL" {
		t.Errorf("MetricName: want DCGM_FI_DEV_GPU_UTIL, got %q", rec.MetricName)
	}
	if rec.GpuID != 0 {
		t.Errorf("GpuID: want 0, got %d", rec.GpuID)
	}
	if rec.Value != 97.0 {
		t.Errorf("Value: want 97.0, got %v", rec.Value)
	}
	if rec.UUID != "GPU-abc123" {
		t.Errorf("UUID: want GPU-abc123, got %q", rec.UUID)
	}
	if rec.Container != nil {
		t.Errorf("Container: want nil, got %q", *rec.Container)
	}
}

func TestNext_NullableFields(t *testing.T) {
	p, _ := csvparser.New(strings.NewReader(validCSV))
	p.Next() // skip first row
	rec, err := p.Next()
	if err != nil {
		t.Fatalf("Next row 2: %v", err)
	}
	if rec.Container == nil || *rec.Container != "mycontainer" {
		t.Errorf("Container: want mycontainer, got %v", rec.Container)
	}
	if rec.Pod == nil || *rec.Pod != "mypod" {
		t.Errorf("Pod: want mypod, got %v", rec.Pod)
	}
	if rec.Namespace == nil || *rec.Namespace != "mynamespace" {
		t.Errorf("Namespace: want mynamespace, got %v", rec.Namespace)
	}
}

func TestNext_EOF(t *testing.T) {
	p, _ := csvparser.New(strings.NewReader(validCSV))
	p.Next()
	p.Next()
	_, err := p.Next()
	if err != io.EOF {
		t.Errorf("Next at EOF: want io.EOF, got %v", err)
	}
}

func TestNext_InvalidGpuID(t *testing.T) {
	data := "timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw\n" +
		"2025-07-18T20:42:34Z,DCGM_FI_DEV_GPU_UTIL,notanint,nvidia0,GPU-abc,NVIDIA H100,host,,,, 97.0,\n"
	p, _ := csvparser.New(strings.NewReader(data))
	_, err := p.Next()
	if err == nil {
		t.Fatal("Next: expected error on invalid gpu_id")
	}
}

func TestNext_InvalidValue(t *testing.T) {
	data := "timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw\n" +
		"2025-07-18T20:42:34Z,DCGM_FI_DEV_GPU_UTIL,0,nvidia0,GPU-abc,NVIDIA H100,host,,,,notafloat,\n"
	p, _ := csvparser.New(strings.NewReader(data))
	_, err := p.Next()
	if err == nil {
		t.Fatal("Next: expected error on invalid value")
	}
}
