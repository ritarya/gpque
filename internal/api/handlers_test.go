package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"

	"gpqueue/internal/api"
	"gpqueue/internal/model"
	"gpqueue/internal/storage"
)

// mockRepo implements storage.Repository for handler tests.
type mockRepo struct {
	gpus      []model.GPU
	gpusErr   error
	getGPU    *model.GPU
	getGPUErr error
	telPage   storage.TelemetryPage
	telErr    error
}

func (m *mockRepo) ListGPUs(_ context.Context) ([]model.GPU, error) {
	return m.gpus, m.gpusErr
}
func (m *mockRepo) GetGPU(_ context.Context, _ string) (*model.GPU, error) {
	return m.getGPU, m.getGPUErr
}
func (m *mockRepo) QueryTelemetry(_ context.Context, _ storage.TelemetryQuery) (storage.TelemetryPage, error) {
	return m.telPage, m.telErr
}
func (m *mockRepo) UpsertGPU(_ context.Context, _ *model.TelemetryRecord) error      { return nil }
func (m *mockRepo) InsertTelemetry(_ context.Context, _ *model.TelemetryRecord) error { return nil }
func (m *mockRepo) Ping(_ context.Context) error                                       { return nil }
func (m *mockRepo) Close() error                                                       { return nil }

func newTestServer(t *testing.T, repo storage.Repository) *httptest.Server {
	t.Helper()
	r := chi.NewRouter()
	humaAPI := humachi.New(r, huma.DefaultConfig("Test API", "1.0.0"))
	api.Register(humaAPI, repo)
	ts := httptest.NewServer(r)
	t.Cleanup(ts.Close)
	return ts
}

// ── ListGPUs ─────────────────────────────────────────────────────────────────

func TestListGPUs_Empty(t *testing.T) {
	ts := newTestServer(t, &mockRepo{})
	resp, err := http.Get(ts.URL + "/api/v1/gpus")
	if err != nil {
		t.Fatalf("GET /api/v1/gpus: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
	var body struct {
		GPUs  []model.GPU `json:"gpus"`
		Total int         `json:"total"`
	}
	json.NewDecoder(resp.Body).Decode(&body)
	if body.Total != 0 {
		t.Errorf("total: want 0, got %d", body.Total)
	}
	if body.GPUs == nil {
		t.Error("gpus: want non-nil empty slice, got nil")
	}
}

func TestListGPUs_WithData(t *testing.T) {
	now := time.Now()
	repo := &mockRepo{
		gpus: []model.GPU{
			{UUID: "GPU-abc", GpuID: 0, Device: "nvidia0", ModelName: "H100", Hostname: "host1", FirstSeen: now, LastSeen: now},
			{UUID: "GPU-def", GpuID: 1, Device: "nvidia1", ModelName: "H100", Hostname: "host1", FirstSeen: now, LastSeen: now},
		},
	}
	ts := newTestServer(t, repo)
	resp, _ := http.Get(ts.URL + "/api/v1/gpus")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
	var body struct {
		Total int `json:"total"`
	}
	json.NewDecoder(resp.Body).Decode(&body)
	if body.Total != 2 {
		t.Errorf("total: want 2, got %d", body.Total)
	}
}

func TestListGPUs_RepoError(t *testing.T) {
	repo := &mockRepo{gpusErr: fmt.Errorf("db error")}
	ts := newTestServer(t, repo)
	resp, _ := http.Get(ts.URL + "/api/v1/gpus")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("want 500, got %d", resp.StatusCode)
	}
}

// ── GetTelemetry ──────────────────────────────────────────────────────────────

func TestGetTelemetry_GPUNotFound(t *testing.T) {
	ts := newTestServer(t, &mockRepo{getGPU: nil})
	resp, _ := http.Get(ts.URL + "/api/v1/gpus/GPU-nonexistent/telemetry")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("want 404, got %d", resp.StatusCode)
	}
}

func TestGetTelemetry_GetGPUError(t *testing.T) {
	repo := &mockRepo{getGPUErr: fmt.Errorf("db error")}
	ts := newTestServer(t, repo)
	resp, _ := http.Get(ts.URL + "/api/v1/gpus/GPU-abc/telemetry")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("want 500, got %d", resp.StatusCode)
	}
}

func TestGetTelemetry_Success(t *testing.T) {
	now := time.Now()
	gpu := &model.GPU{UUID: "GPU-abc", GpuID: 0}
	page := storage.TelemetryPage{
		Records: []storage.TelemetryRow{
			{ID: "rec-1", ProcessedAt: now, MetricName: "DCGM_FI_DEV_GPU_UTIL", UUID: "GPU-abc", Value: 97.0},
		},
		Total: 1,
	}
	ts := newTestServer(t, &mockRepo{getGPU: gpu, telPage: page})

	resp, _ := http.Get(ts.URL + "/api/v1/gpus/GPU-abc/telemetry")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
	var body struct {
		GPUUUID string `json:"gpu_uuid"`
		Total   int64  `json:"total"`
		Records []any  `json:"records"`
	}
	json.NewDecoder(resp.Body).Decode(&body)
	if body.GPUUUID != "GPU-abc" {
		t.Errorf("gpu_uuid: want GPU-abc, got %q", body.GPUUUID)
	}
	if body.Total != 1 {
		t.Errorf("total: want 1, got %d", body.Total)
	}
	if len(body.Records) != 1 {
		t.Errorf("records: want 1, got %d", len(body.Records))
	}
}

func TestGetTelemetry_QueryError(t *testing.T) {
	gpu := &model.GPU{UUID: "GPU-abc"}
	repo := &mockRepo{getGPU: gpu, telErr: fmt.Errorf("db error")}
	ts := newTestServer(t, repo)
	resp, _ := http.Get(ts.URL + "/api/v1/gpus/GPU-abc/telemetry")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("want 500, got %d", resp.StatusCode)
	}
}

func TestGetTelemetry_InvalidStartTime(t *testing.T) {
	ts := newTestServer(t, &mockRepo{getGPU: &model.GPU{UUID: "GPU-abc"}})
	resp, _ := http.Get(ts.URL + "/api/v1/gpus/GPU-abc/telemetry?start_time=not-a-date")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("want 400, got %d", resp.StatusCode)
	}
}

func TestGetTelemetry_InvalidEndTime(t *testing.T) {
	ts := newTestServer(t, &mockRepo{getGPU: &model.GPU{UUID: "GPU-abc"}})
	resp, _ := http.Get(ts.URL + "/api/v1/gpus/GPU-abc/telemetry?end_time=bad-date")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("want 400, got %d", resp.StatusCode)
	}
}

func TestGetTelemetry_InvalidCursor(t *testing.T) {
	ts := newTestServer(t, &mockRepo{getGPU: &model.GPU{UUID: "GPU-abc"}})
	resp, _ := http.Get(ts.URL + "/api/v1/gpus/GPU-abc/telemetry?cursor=invalid")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("want 400, got %d", resp.StatusCode)
	}
}

func TestGetTelemetry_WithValidCursor(t *testing.T) {
	now := time.Date(2025, 7, 18, 20, 42, 34, 0, time.UTC)
	cursor := storage.EncodeCursor(now, "rec-0")

	gpu := &model.GPU{UUID: "GPU-abc"}
	page := storage.TelemetryPage{Records: []storage.TelemetryRow{}, Total: 0}
	ts := newTestServer(t, &mockRepo{getGPU: gpu, telPage: page})

	resp, _ := http.Get(ts.URL + "/api/v1/gpus/GPU-abc/telemetry?cursor=" + cursor)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("valid cursor: want 200, got %d", resp.StatusCode)
	}
}
