package mqclient_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gpqueue/internal/mqclient"
)

func TestPublish_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("Publish: want POST, got %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/messages") {
			t.Errorf("Publish: unexpected path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	c := mqclient.New(srv.URL, "test-producer")
	if err := c.Publish(context.Background(), "telemetry", []byte(`{"hello":"world"}`)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestPublish_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := mqclient.New(srv.URL, "test-producer")
	if err := c.Publish(context.Background(), "telemetry", []byte("data")); err == nil {
		t.Fatal("Publish: expected error on 429, got nil")
	}
}

func TestFetch_Success(t *testing.T) {
	payload := base64.StdEncoding.EncodeToString([]byte(`{"metric":"GPU_UTIL"}`))
	now := time.Now().UTC()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := map[string]any{
			"messages": []map[string]any{
				{
					"offset":       int64(42),
					"payload":      payload,
					"published_at": now.Format(time.RFC3339),
					"producer_id":  "streamer-0",
				},
			},
			"next_offset": int64(43),
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	c := mqclient.New(srv.URL, "consumer-0")
	msgs, err := c.Fetch(context.Background(), "telemetry", "collectors", 10)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("Fetch: want 1 message, got %d", len(msgs))
	}
	if msgs[0].Offset != 42 {
		t.Errorf("Offset: want 42, got %d", msgs[0].Offset)
	}
	if string(msgs[0].Payload) != `{"metric":"GPU_UTIL"}` {
		t.Errorf("Payload: got %q", msgs[0].Payload)
	}
}

func TestFetch_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := mqclient.New(srv.URL, "consumer-0")
	if _, err := c.Fetch(context.Background(), "telemetry", "g1", 10); err == nil {
		t.Fatal("Fetch: expected error on 500")
	}
}

func TestFetch_InvalidBase64Payload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := map[string]any{
			"messages": []map[string]any{
				{
					"offset":       int64(0),
					"payload":      "!!!not-valid-base64!!!",
					"published_at": time.Now().Format(time.RFC3339),
					"producer_id":  "streamer-0",
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	c := mqclient.New(srv.URL, "consumer-0")
	if _, err := c.Fetch(context.Background(), "telemetry", "g1", 10); err == nil {
		t.Fatal("Fetch: expected error on invalid base64 payload")
	}
}

func TestFetch_InvalidJSONBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("not-json"))
	}))
	defer srv.Close()

	c := mqclient.New(srv.URL, "consumer-0")
	if _, err := c.Fetch(context.Background(), "telemetry", "g1", 10); err == nil {
		t.Fatal("Fetch: expected error on invalid JSON body")
	}
}

func TestCommit_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/commit") {
			t.Errorf("Commit: unexpected path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := mqclient.New(srv.URL, "consumer-0")
	if err := c.Commit(context.Background(), "telemetry", "collectors", 42); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestNack_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/nack") {
			t.Errorf("Nack: unexpected path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := mqclient.New(srv.URL, "consumer-0")
	if err := c.Nack(context.Background(), "telemetry", "collectors", 0); err != nil {
		t.Fatalf("Nack: %v", err)
	}
}

func TestNack_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := mqclient.New(srv.URL, "consumer-0")
	if err := c.Nack(context.Background(), "telemetry", "g1", 0); err == nil {
		t.Fatal("Nack: expected error on 500")
	}
}

func TestClose(t *testing.T) {
	c := mqclient.New("http://localhost:9999", "test")
	if err := c.Close(); err != nil {
		t.Errorf("Close: unexpected error: %v", err)
	}
}
