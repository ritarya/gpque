package main

import (
	"context"
	"errors"
	"os"
	"testing"

	"gpqueue/internal/model"
	"gpqueue/internal/storage"
)

// mockRepo implements storage.Repository for unit tests.
type mockRepo struct {
	pingErr   error
	upsertErr error
	insertErr error
}

func (m *mockRepo) Ping(_ context.Context) error                                       { return m.pingErr }
func (m *mockRepo) UpsertGPU(_ context.Context, _ *model.TelemetryRecord) error       { return m.upsertErr }
func (m *mockRepo) InsertTelemetry(_ context.Context, _ *model.TelemetryRecord) error { return m.insertErr }
func (m *mockRepo) ListGPUs(_ context.Context) ([]model.GPU, error)                   { return nil, nil }
func (m *mockRepo) GetGPU(_ context.Context, _ string) (*model.GPU, error)            { return nil, nil }
func (m *mockRepo) QueryTelemetry(_ context.Context, _ storage.TelemetryQuery) (storage.TelemetryPage, error) {
	return storage.TelemetryPage{}, nil
}
func (m *mockRepo) Close() error { return nil }

// ── persist ───────────────────────────────────────────────────────────────────

func TestPersist_Success(t *testing.T) {
	rec := &model.TelemetryRecord{ID: "rec-1", UUID: "GPU-abc"}
	if err := persist(context.Background(), &mockRepo{}, rec); err != nil {
		t.Fatalf("persist: %v", err)
	}
}

func TestPersist_UpsertError(t *testing.T) {
	rec := &model.TelemetryRecord{ID: "rec-1", UUID: "GPU-abc"}
	if err := persist(context.Background(), &mockRepo{upsertErr: errors.New("upsert failed")}, rec); err == nil {
		t.Fatal("persist: expected error on upsert failure")
	}
}

func TestPersist_InsertError(t *testing.T) {
	rec := &model.TelemetryRecord{ID: "rec-1", UUID: "GPU-abc"}
	if err := persist(context.Background(), &mockRepo{insertErr: errors.New("insert failed")}, rec); err == nil {
		t.Fatal("persist: expected error on insert failure")
	}
}

// ── waitForDB ─────────────────────────────────────────────────────────────────

func TestWaitForDB_ImmediatelyReady(t *testing.T) {
	waitForDB(context.Background(), &mockRepo{pingErr: nil}, "collector-0")
}

func TestWaitForDB_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	waitForDB(ctx, &mockRepo{pingErr: errors.New("db not ready")}, "collector-0")
}

// ── loadConfig ────────────────────────────────────────────────────────────────

func TestLoadConfig_Defaults(t *testing.T) {
	for _, k := range []string{"COLLECTOR_ID", "CONSUMER_GROUP", "TOPIC_NAME", "QUEUE_ADDR", "DB_DSN", "BATCH_SIZE", "DLQ_TOPIC", "MAX_RETRIES"} {
		os.Unsetenv(k)
	}
	cfg := loadConfig()
	if cfg.collectorID != "collector-0" {
		t.Errorf("collectorID: want collector-0, got %q", cfg.collectorID)
	}
	if cfg.consumerGroup != "telemetry-collectors" {
		t.Errorf("consumerGroup: want telemetry-collectors, got %q", cfg.consumerGroup)
	}
	if cfg.topicName != "telemetry" {
		t.Errorf("topicName: want telemetry, got %q", cfg.topicName)
	}
	if cfg.batchSize != 50 {
		t.Errorf("batchSize: want 50, got %d", cfg.batchSize)
	}
	if cfg.maxRetries != 3 {
		t.Errorf("maxRetries: want 3, got %d", cfg.maxRetries)
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	os.Setenv("COLLECTOR_ID", "my-collector")
	os.Setenv("BATCH_SIZE", "25")
	os.Setenv("CONSUMER_GROUP", "my-group")
	defer func() {
		os.Unsetenv("COLLECTOR_ID")
		os.Unsetenv("BATCH_SIZE")
		os.Unsetenv("CONSUMER_GROUP")
	}()

	cfg := loadConfig()
	if cfg.collectorID != "my-collector" {
		t.Errorf("collectorID: want my-collector, got %q", cfg.collectorID)
	}
	if cfg.batchSize != 25 {
		t.Errorf("batchSize: want 25, got %d", cfg.batchSize)
	}
	if cfg.consumerGroup != "my-group" {
		t.Errorf("consumerGroup: want my-group, got %q", cfg.consumerGroup)
	}
}

// ── getenv ────────────────────────────────────────────────────────────────────

func TestGetenv_Default(t *testing.T) {
	os.Unsetenv("_TEST_GETENV_KEY")
	if got := getenv("_TEST_GETENV_KEY", "fallback"); got != "fallback" {
		t.Errorf("want fallback, got %q", got)
	}
}

func TestGetenv_EnvSet(t *testing.T) {
	os.Setenv("_TEST_GETENV_KEY", "custom")
	defer os.Unsetenv("_TEST_GETENV_KEY")
	if got := getenv("_TEST_GETENV_KEY", "fallback"); got != "custom" {
		t.Errorf("want custom, got %q", got)
	}
}

// ── getenvInt ─────────────────────────────────────────────────────────────────

func TestGetenvInt_Default(t *testing.T) {
	os.Unsetenv("_TEST_INT_KEY")
	if got := getenvInt("_TEST_INT_KEY", 42); got != 42 {
		t.Errorf("want 42, got %d", got)
	}
}

func TestGetenvInt_ValidValue(t *testing.T) {
	os.Setenv("_TEST_INT_KEY", "99")
	defer os.Unsetenv("_TEST_INT_KEY")
	if got := getenvInt("_TEST_INT_KEY", 42); got != 99 {
		t.Errorf("want 99, got %d", got)
	}
}

func TestGetenvInt_InvalidValue(t *testing.T) {
	os.Setenv("_TEST_INT_KEY", "notanint")
	defer os.Unsetenv("_TEST_INT_KEY")
	if got := getenvInt("_TEST_INT_KEY", 42); got != 42 {
		t.Errorf("want default 42 on invalid int, got %d", got)
	}
}
