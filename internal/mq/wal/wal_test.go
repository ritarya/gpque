package wal_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gpqueue/internal/mq/wal"
)

func TestAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := wal.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	entries := []wal.Entry{
		{Topic: "t1", Offset: 0, Payload: []byte("msg0"), PublishedAt: time.Now(), ProducerID: "p1"},
		{Topic: "t1", Offset: 1, Payload: []byte("msg1"), PublishedAt: time.Now(), ProducerID: "p1"},
	}
	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	w.Close()

	var replayed []wal.Entry
	if err := wal.Replay(path, func(e wal.Entry) { replayed = append(replayed, e) }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("want 2 entries, got %d", len(replayed))
	}
	if string(replayed[0].Payload) != "msg0" {
		t.Errorf("entry 0 payload: want msg0, got %q", replayed[0].Payload)
	}
	if replayed[1].Offset != 1 {
		t.Errorf("entry 1 offset: want 1, got %d", replayed[1].Offset)
	}
	if replayed[1].Topic != "t1" {
		t.Errorf("entry 1 topic: want t1, got %q", replayed[1].Topic)
	}
}

func TestReplayMissingFile(t *testing.T) {
	err := wal.Replay("/nonexistent/path/wal.log", func(wal.Entry) {})
	if err != nil {
		t.Errorf("replay on missing file should return nil, got %v", err)
	}
}

func TestReplaySkipsMalformedLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	// valid JSON, corrupt line, valid JSON
	content := `{"topic":"t1","offset":0,"payload":"bXNnMA==","published_at":"2025-01-01T00:00:00Z","producer_id":"p1"}
not-valid-json
{"topic":"t1","offset":1,"payload":"bXNnMQ==","published_at":"2025-01-01T00:00:00Z","producer_id":"p1"}
`
	os.WriteFile(path, []byte(content), 0o600)

	var count int
	if err := wal.Replay(path, func(wal.Entry) { count++ }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if count != 2 {
		t.Errorf("want 2 valid entries (malformed skipped), got %d", count)
	}
}

func TestAppendPreservesBinaryPayload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	payload := []byte{0x00, 0xff, 0x1f, 0xab, 0xcd}
	w, _ := wal.Open(path)
	w.Append(wal.Entry{Topic: "t", Offset: 0, Payload: payload, PublishedAt: time.Now()})
	w.Close()

	var got []byte
	wal.Replay(path, func(e wal.Entry) { got = e.Payload })
	if len(got) != len(payload) {
		t.Fatalf("payload length: want %d, got %d", len(payload), len(got))
	}
	for i := range payload {
		if got[i] != payload[i] {
			t.Errorf("byte %d: want %02x, got %02x", i, payload[i], got[i])
		}
	}
}

func TestOpenAppendsToExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w1, _ := wal.Open(path)
	w1.Append(wal.Entry{Topic: "t", Offset: 0, Payload: []byte("first"), PublishedAt: time.Now()})
	w1.Close()

	w2, _ := wal.Open(path)
	w2.Append(wal.Entry{Topic: "t", Offset: 1, Payload: []byte("second"), PublishedAt: time.Now()})
	w2.Close()

	var entries []wal.Entry
	wal.Replay(path, func(e wal.Entry) { entries = append(entries, e) })
	if len(entries) != 2 {
		t.Fatalf("want 2 entries after two opens, got %d", len(entries))
	}
}
