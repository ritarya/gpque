package main

import (
	"os"
	"testing"
)

// ── loadConfig ────────────────────────────────────────────────────────────────

func TestLoadConfig_Defaults(t *testing.T) {
	for _, k := range []string{"MQ_PORT", "MQ_WAL_PATH", "MQ_HIGH_WATER_MARK", "MQ_MAX_RETRIES", "MQ_POLL_TIMEOUT_MS"} {
		os.Unsetenv(k)
	}
	cfg := loadConfig()
	if cfg.port != 8080 {
		t.Errorf("port: want 8080, got %d", cfg.port)
	}
	if cfg.walPath != "/data/wal.log" {
		t.Errorf("walPath: want /data/wal.log, got %q", cfg.walPath)
	}
	if cfg.highWaterMark != 100000 {
		t.Errorf("highWaterMark: want 100000, got %d", cfg.highWaterMark)
	}
	if cfg.maxRetries != 3 {
		t.Errorf("maxRetries: want 3, got %d", cfg.maxRetries)
	}
	if cfg.pollTimeoutMS != 5000 {
		t.Errorf("pollTimeoutMS: want 5000, got %d", cfg.pollTimeoutMS)
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	os.Setenv("MQ_PORT", "9090")
	os.Setenv("MQ_WAL_PATH", "/tmp/wal.log")
	os.Setenv("MQ_HIGH_WATER_MARK", "50000")
	defer func() {
		os.Unsetenv("MQ_PORT")
		os.Unsetenv("MQ_WAL_PATH")
		os.Unsetenv("MQ_HIGH_WATER_MARK")
	}()

	cfg := loadConfig()
	if cfg.port != 9090 {
		t.Errorf("port: want 9090, got %d", cfg.port)
	}
	if cfg.walPath != "/tmp/wal.log" {
		t.Errorf("walPath: want /tmp/wal.log, got %q", cfg.walPath)
	}
	if cfg.highWaterMark != 50000 {
		t.Errorf("highWaterMark: want 50000, got %d", cfg.highWaterMark)
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
	os.Setenv("_TEST_INT_KEY", "7777")
	defer os.Unsetenv("_TEST_INT_KEY")
	if got := getenvInt("_TEST_INT_KEY", 42); got != 7777 {
		t.Errorf("want 7777, got %d", got)
	}
}

func TestGetenvInt_InvalidValue(t *testing.T) {
	os.Setenv("_TEST_INT_KEY", "bad")
	defer os.Unsetenv("_TEST_INT_KEY")
	if got := getenvInt("_TEST_INT_KEY", 42); got != 42 {
		t.Errorf("want default 42 on bad value, got %d", got)
	}
}
