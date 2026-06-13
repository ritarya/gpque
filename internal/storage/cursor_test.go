package storage_test

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"gpqueue/internal/storage"
)

// ── storage.New / Close ───────────────────────────────────────────────────────

func TestNew_OpensWithoutConnecting(t *testing.T) {
	// sql.Open is lazy: it does not dial until the first query, so New
	// should succeed even when no postgres server is running.
	repo, err := storage.New("postgres://postgres:postgres@localhost:5432/telemetry?sslmode=disable")
	if err != nil {
		t.Fatalf("New: unexpected error: %v", err)
	}
	if err := repo.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestEncodeCursor_DecodeCursor_RoundTrip(t *testing.T) {
	ts := time.Date(2025, 7, 18, 20, 42, 34, 123000000, time.UTC)
	id := "01930a4e-7b3c-7f2d-a1b2-c3d4e5f60001"

	encoded := storage.EncodeCursor(ts, id)
	if encoded == "" {
		t.Fatal("EncodeCursor: returned empty string")
	}

	pos, err := storage.DecodeCursor(encoded)
	if err != nil {
		t.Fatalf("DecodeCursor: %v", err)
	}
	if !pos.ProcessedAt.Equal(ts) {
		t.Errorf("ProcessedAt: want %v, got %v", ts, pos.ProcessedAt)
	}
	if pos.ID != id {
		t.Errorf("ID: want %q, got %q", id, pos.ID)
	}
}

func TestDecodeCursor_InvalidBase64(t *testing.T) {
	_, err := storage.DecodeCursor("!!!not-valid-base64!!!")
	if err == nil {
		t.Fatal("DecodeCursor: expected error on invalid base64, got nil")
	}
}

func TestDecodeCursor_InvalidJSON(t *testing.T) {
	encoded := base64.URLEncoding.EncodeToString([]byte("not-json"))
	_, err := storage.DecodeCursor(encoded)
	if err == nil {
		t.Fatal("DecodeCursor: expected error on invalid JSON, got nil")
	}
}

func TestDecodeCursor_InvalidTimestamp(t *testing.T) {
	b, _ := json.Marshal(map[string]string{"ts": "not-a-timestamp", "id": "abc"})
	encoded := base64.URLEncoding.EncodeToString(b)
	_, err := storage.DecodeCursor(encoded)
	if err == nil {
		t.Fatal("DecodeCursor: expected error on invalid timestamp, got nil")
	}
}
