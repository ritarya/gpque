package main

import (
	"os"
	"testing"
)

// ── loadConfig ────────────────────────────────────────────────────────────────

func TestLoadConfig_Defaults(t *testing.T) {
	for _, k := range []string{"STREAMER_ID", "CSV_PATH", "PUBLISH_INTERVAL_MS", "TOPIC_NAME", "QUEUE_ADDR"} {
		os.Unsetenv(k)
	}
	cfg := loadConfig()
	if cfg.streamerID != "streamer-0" {
		t.Errorf("streamerID: want streamer-0, got %q", cfg.streamerID)
	}
	if cfg.csvPath != "/data/dcgm_metrics.csv" {
		t.Errorf("csvPath: want /data/dcgm_metrics.csv, got %q", cfg.csvPath)
	}
	if cfg.publishIntervalMS != 100 {
		t.Errorf("publishIntervalMS: want 100, got %d", cfg.publishIntervalMS)
	}
	if cfg.topicName != "telemetry" {
		t.Errorf("topicName: want telemetry, got %q", cfg.topicName)
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	os.Setenv("STREAMER_ID", "my-streamer")
	os.Setenv("PUBLISH_INTERVAL_MS", "500")
	os.Setenv("CSV_PATH", "/tmp/test.csv")
	defer func() {
		os.Unsetenv("STREAMER_ID")
		os.Unsetenv("PUBLISH_INTERVAL_MS")
		os.Unsetenv("CSV_PATH")
	}()

	cfg := loadConfig()
	if cfg.streamerID != "my-streamer" {
		t.Errorf("streamerID: want my-streamer, got %q", cfg.streamerID)
	}
	if cfg.publishIntervalMS != 500 {
		t.Errorf("publishIntervalMS: want 500, got %d", cfg.publishIntervalMS)
	}
	if cfg.csvPath != "/tmp/test.csv" {
		t.Errorf("csvPath: want /tmp/test.csv, got %q", cfg.csvPath)
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
	os.Setenv("_TEST_INT_KEY", "200")
	defer os.Unsetenv("_TEST_INT_KEY")
	if got := getenvInt("_TEST_INT_KEY", 42); got != 200 {
		t.Errorf("want 200, got %d", got)
	}
}

func TestGetenvInt_InvalidValue(t *testing.T) {
	os.Setenv("_TEST_INT_KEY", "bad")
	defer os.Unsetenv("_TEST_INT_KEY")
	if got := getenvInt("_TEST_INT_KEY", 42); got != 42 {
		t.Errorf("want default 42 on invalid int, got %d", got)
	}
}
