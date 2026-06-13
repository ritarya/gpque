package storage

import (
	"context"
	"database/sql"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"gpqueue/internal/model"
)

func newMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, mock
}

// ── Ping / Close ──────────────────────────────────────────────────────────────

func TestPing_Success(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectPing()
	repo := &pgRepo{db: db}
	if err := repo.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestClose_Success(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectClose()
	repo := &pgRepo{db: db}
	if err := repo.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// ── UpsertGPU ─────────────────────────────────────────────────────────────────

func TestUpsertGPU_Success(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectExec("INSERT INTO gpus").WillReturnResult(sqlmock.NewResult(1, 1))
	repo := &pgRepo{db: db}

	rec := &model.TelemetryRecord{
		UUID: "GPU-abc", GpuID: 0, Device: "nvidia0",
		ModelName: "NVIDIA H100", Hostname: "host1",
		ProcessedAt: time.Now(),
	}
	if err := repo.UpsertGPU(context.Background(), rec); err != nil {
		t.Fatalf("UpsertGPU: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestUpsertGPU_DBError(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectExec("INSERT INTO gpus").WillReturnError(sql.ErrConnDone)
	repo := &pgRepo{db: db}

	rec := &model.TelemetryRecord{UUID: "GPU-abc", ProcessedAt: time.Now()}
	if err := repo.UpsertGPU(context.Background(), rec); err == nil {
		t.Fatal("UpsertGPU: expected error, got nil")
	}
}

// ── InsertTelemetry ───────────────────────────────────────────────────────────

func TestInsertTelemetry_Success(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectExec("INSERT INTO telemetry").WillReturnResult(sqlmock.NewResult(1, 1))
	repo := &pgRepo{db: db}

	rec := &model.TelemetryRecord{
		ID: "rec-1", UUID: "GPU-abc", MetricName: "DCGM_FI_DEV_GPU_UTIL",
		GpuID: 0, Device: "nvidia0", Hostname: "host1", Value: 97.0,
		ProcessedAt: time.Now(),
	}
	if err := repo.InsertTelemetry(context.Background(), rec); err != nil {
		t.Fatalf("InsertTelemetry: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestInsertTelemetry_DBError(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectExec("INSERT INTO telemetry").WillReturnError(sql.ErrConnDone)
	repo := &pgRepo{db: db}

	rec := &model.TelemetryRecord{ID: "rec-1", UUID: "GPU-abc", ProcessedAt: time.Now()}
	if err := repo.InsertTelemetry(context.Background(), rec); err == nil {
		t.Fatal("InsertTelemetry: expected error, got nil")
	}
}

// ── ListGPUs ──────────────────────────────────────────────────────────────────

func TestListGPUs_ReturnsRows(t *testing.T) {
	db, mock := newMockDB(t)
	now := time.Now()
	rows := sqlmock.NewRows([]string{"uuid", "gpu_id", "device", "model_name", "hostname", "first_seen", "last_seen"}).
		AddRow("GPU-abc", 0, "nvidia0", "NVIDIA H100", "host1", now, now).
		AddRow("GPU-def", 1, "nvidia1", "NVIDIA H100", "host1", now, now)
	mock.ExpectQuery("SELECT uuid").WillReturnRows(rows)
	repo := &pgRepo{db: db}

	gpus, err := repo.ListGPUs(context.Background())
	if err != nil {
		t.Fatalf("ListGPUs: %v", err)
	}
	if len(gpus) != 2 {
		t.Errorf("want 2 GPUs, got %d", len(gpus))
	}
	if gpus[0].UUID != "GPU-abc" {
		t.Errorf("GPU[0].UUID: want GPU-abc, got %q", gpus[0].UUID)
	}
}

func TestListGPUs_Empty(t *testing.T) {
	db, mock := newMockDB(t)
	rows := sqlmock.NewRows([]string{"uuid", "gpu_id", "device", "model_name", "hostname", "first_seen", "last_seen"})
	mock.ExpectQuery("SELECT uuid").WillReturnRows(rows)
	repo := &pgRepo{db: db}

	gpus, err := repo.ListGPUs(context.Background())
	if err != nil {
		t.Fatalf("ListGPUs empty: %v", err)
	}
	if len(gpus) != 0 {
		t.Errorf("want 0 GPUs, got %d", len(gpus))
	}
}

func TestListGPUs_DBError(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectQuery("SELECT uuid").WillReturnError(sql.ErrConnDone)
	repo := &pgRepo{db: db}

	if _, err := repo.ListGPUs(context.Background()); err == nil {
		t.Fatal("ListGPUs: expected error, got nil")
	}
}

// ── GetGPU ────────────────────────────────────────────────────────────────────

func TestGetGPU_Found(t *testing.T) {
	db, mock := newMockDB(t)
	now := time.Now()
	rows := sqlmock.NewRows([]string{"uuid", "gpu_id", "device", "model_name", "hostname", "first_seen", "last_seen"}).
		AddRow("GPU-abc", 0, "nvidia0", "NVIDIA H100", "host1", now, now)
	mock.ExpectQuery("SELECT uuid").WithArgs("GPU-abc").WillReturnRows(rows)
	repo := &pgRepo{db: db}

	gpu, err := repo.GetGPU(context.Background(), "GPU-abc")
	if err != nil {
		t.Fatalf("GetGPU: %v", err)
	}
	if gpu == nil {
		t.Fatal("GetGPU: expected non-nil GPU")
	}
	if gpu.UUID != "GPU-abc" {
		t.Errorf("UUID: want GPU-abc, got %q", gpu.UUID)
	}
}

func TestGetGPU_NotFound(t *testing.T) {
	db, mock := newMockDB(t)
	rows := sqlmock.NewRows([]string{"uuid", "gpu_id", "device", "model_name", "hostname", "first_seen", "last_seen"})
	mock.ExpectQuery("SELECT uuid").WithArgs("GPU-xyz").WillReturnRows(rows)
	repo := &pgRepo{db: db}

	gpu, err := repo.GetGPU(context.Background(), "GPU-xyz")
	if err != nil {
		t.Fatalf("GetGPU not-found: unexpected error: %v", err)
	}
	if gpu != nil {
		t.Errorf("GetGPU not-found: want nil, got %+v", gpu)
	}
}

func TestGetGPU_DBError(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectQuery("SELECT uuid").WithArgs("GPU-abc").WillReturnError(sql.ErrConnDone)
	repo := &pgRepo{db: db}

	if _, err := repo.GetGPU(context.Background(), "GPU-abc"); err == nil {
		t.Fatal("GetGPU error: expected error, got nil")
	}
}

// ── QueryTelemetry ────────────────────────────────────────────────────────────

var telemetryCols = []string{
	"id", "processed_at", "metric_name", "gpu_uuid",
	"gpu_id", "device", "hostname", "value",
	"container", "pod", "namespace", "model_name",
}

func TestQueryTelemetry_Basic(t *testing.T) {
	db, mock := newMockDB(t)
	now := time.Now()

	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(2)))

	mock.ExpectQuery("SELECT t.id").
		WillReturnRows(sqlmock.NewRows(telemetryCols).
			AddRow("rec-1", now, "DCGM_FI_DEV_GPU_UTIL", "GPU-abc", 0, "nvidia0", "host1", 97.0, nil, nil, nil, "NVIDIA H100").
			AddRow("rec-2", now, "DCGM_FI_DEV_POWER_USAGE", "GPU-abc", 0, "nvidia0", "host1", 250.0, nil, nil, nil, "NVIDIA H100"))

	repo := &pgRepo{db: db}
	page, err := repo.QueryTelemetry(context.Background(), TelemetryQuery{
		GPUUUID: "GPU-abc",
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("QueryTelemetry: %v", err)
	}
	if len(page.Records) != 2 {
		t.Errorf("want 2 records, got %d", len(page.Records))
	}
	if page.Total != 2 {
		t.Errorf("total: want 2, got %d", page.Total)
	}
	if page.NextCursor != "" {
		t.Errorf("NextCursor: want empty (no next page), got %q", page.NextCursor)
	}
}

func TestQueryTelemetry_WithFilters(t *testing.T) {
	db, mock := newMockDB(t)
	start := time.Date(2025, 7, 18, 20, 0, 0, 0, time.UTC)
	end := time.Date(2025, 7, 18, 21, 0, 0, 0, time.UTC)

	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectQuery("SELECT t.id").
		WillReturnRows(sqlmock.NewRows(telemetryCols).
			AddRow("rec-1", start, "DCGM_FI_DEV_GPU_UTIL", "GPU-abc", 0, "nvidia0", "host1", 97.0, nil, nil, nil, "NVIDIA H100"))

	repo := &pgRepo{db: db}
	page, err := repo.QueryTelemetry(context.Background(), TelemetryQuery{
		GPUUUID:   "GPU-abc",
		StartTime: &start,
		EndTime:   &end,
		Metric:    "DCGM_FI_DEV_GPU_UTIL",
		Limit:     100,
	})
	if err != nil {
		t.Fatalf("QueryTelemetry with filters: %v", err)
	}
	if len(page.Records) != 1 {
		t.Errorf("want 1 record, got %d", len(page.Records))
	}
}

func TestQueryTelemetry_Pagination(t *testing.T) {
	db, mock := newMockDB(t)
	now := time.Now()

	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(5)))

	// Return limit+1 rows (3 when limit=2) to trigger NextCursor generation.
	mock.ExpectQuery("SELECT t.id").
		WillReturnRows(sqlmock.NewRows(telemetryCols).
			AddRow("rec-1", now, "DCGM_FI_DEV_GPU_UTIL", "GPU-abc", 0, "nvidia0", "host1", 97.0, nil, nil, nil, "NVIDIA H100").
			AddRow("rec-2", now, "DCGM_FI_DEV_GPU_UTIL", "GPU-abc", 0, "nvidia0", "host1", 96.0, nil, nil, nil, "NVIDIA H100").
			AddRow("rec-3", now, "DCGM_FI_DEV_GPU_UTIL", "GPU-abc", 0, "nvidia0", "host1", 95.0, nil, nil, nil, "NVIDIA H100"))

	repo := &pgRepo{db: db}
	page, err := repo.QueryTelemetry(context.Background(), TelemetryQuery{
		GPUUUID: "GPU-abc",
		Limit:   2,
	})
	if err != nil {
		t.Fatalf("QueryTelemetry pagination: %v", err)
	}
	if len(page.Records) != 2 {
		t.Errorf("want 2 records (trimmed), got %d", len(page.Records))
	}
	if page.NextCursor == "" {
		t.Error("NextCursor: want non-empty cursor for next page")
	}
}

func TestQueryTelemetry_WithCursor(t *testing.T) {
	db, mock := newMockDB(t)
	now := time.Now()
	cursor := &CursorPos{ProcessedAt: now, ID: "rec-0"}

	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectQuery("SELECT t.id").
		WillReturnRows(sqlmock.NewRows(telemetryCols).
			AddRow("rec-1", now, "DCGM_FI_DEV_GPU_UTIL", "GPU-abc", 0, "nvidia0", "host1", 97.0, nil, nil, nil, "NVIDIA H100"))

	repo := &pgRepo{db: db}
	page, err := repo.QueryTelemetry(context.Background(), TelemetryQuery{
		GPUUUID: "GPU-abc",
		Limit:   10,
		Cursor:  cursor,
	})
	if err != nil {
		t.Fatalf("QueryTelemetry with cursor: %v", err)
	}
	if len(page.Records) != 1 {
		t.Errorf("want 1 record, got %d", len(page.Records))
	}
}

func TestQueryTelemetry_CountError(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectQuery("SELECT COUNT").WillReturnError(sql.ErrConnDone)
	repo := &pgRepo{db: db}

	if _, err := repo.QueryTelemetry(context.Background(), TelemetryQuery{GPUUUID: "GPU-abc"}); err == nil {
		t.Fatal("QueryTelemetry count error: expected error, got nil")
	}
}

func TestQueryTelemetry_DataError(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectQuery("SELECT t.id").WillReturnError(sql.ErrConnDone)
	repo := &pgRepo{db: db}

	if _, err := repo.QueryTelemetry(context.Background(), TelemetryQuery{GPUUUID: "GPU-abc"}); err == nil {
		t.Fatal("QueryTelemetry data error: expected error, got nil")
	}
}

func TestQueryTelemetry_DefaultLimit(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))
	mock.ExpectQuery("SELECT t.id").
		WillReturnRows(sqlmock.NewRows(telemetryCols))
	repo := &pgRepo{db: db}

	// Limit=0 should default to 100.
	page, err := repo.QueryTelemetry(context.Background(), TelemetryQuery{
		GPUUUID: "GPU-abc",
		Limit:   0,
	})
	if err != nil {
		t.Fatalf("QueryTelemetry default limit: %v", err)
	}
	if len(page.Records) != 0 {
		t.Errorf("want 0 records, got %d", len(page.Records))
	}
}
