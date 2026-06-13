package main

import (
	"os"
	"testing"
)

// ── getenv ────────────────────────────────────────────────────────────────────

func TestGetenv_Default(t *testing.T) {
	os.Unsetenv("_TEST_GW_KEY")
	if got := getenv("_TEST_GW_KEY", "fallback"); got != "fallback" {
		t.Errorf("want fallback, got %q", got)
	}
}

func TestGetenv_EnvSet(t *testing.T) {
	os.Setenv("_TEST_GW_KEY", "custom")
	defer os.Unsetenv("_TEST_GW_KEY")
	if got := getenv("_TEST_GW_KEY", "fallback"); got != "custom" {
		t.Errorf("want custom, got %q", got)
	}
}
