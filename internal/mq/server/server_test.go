package server_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"gpqueue/internal/mq/server"
)

// newTestServer creates a Server backed by a temp directory and wraps it in
// an httptest.Server.  PollTimeoutMS is set to 100 ms so empty-fetch tests
// return quickly.
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	dir := t.TempDir()
	srv, err := server.New(server.Config{
		WALPath:       filepath.Join(dir, "wal.log"),
		OffsetPath:    filepath.Join(dir, "offsets.json"),
		HighWaterMark: 100,
		MaxRetries:    2,
		PollTimeoutMS: 100,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(func() {
		ts.Close()
		srv.Close()
	})
	return ts
}

// publish posts one message to topic and fails the test on any error.
func publish(t *testing.T, base, topic, msg string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{
		"payload":     base64.StdEncoding.EncodeToString([]byte(msg)),
		"producer_id": "test",
	})
	resp, err := http.Post(base+"/topics/"+topic+"/messages", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("publish: want 201, got %d: %s", resp.StatusCode, b)
	}
}

// fetch calls GET /topics/{topic}/messages and returns the decoded messages slice.
func fetch(t *testing.T, base, topic, group string, limit int) []map[string]any {
	t.Helper()
	url := fmt.Sprintf("%s/topics/%s/messages?group=%s&limit=%d", base, topic, group, limit)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	defer resp.Body.Close()
	var result struct {
		Messages []map[string]any `json:"messages"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	return result.Messages
}

// decodePayload base64-decodes the payload field from a fetched message map.
func decodePayload(m map[string]any) string {
	b, _ := base64.StdEncoding.DecodeString(m["payload"].(string))
	return string(b)
}

// ── Publish / Fetch ───────────────────────────────────────────────────────────

func TestPublishAndFetch(t *testing.T) {
	ts := newTestServer(t)
	publish(t, ts.URL, "t1", "hello")
	publish(t, ts.URL, "t1", "world")

	msgs := fetch(t, ts.URL, "t1", "g1", 10)
	if len(msgs) != 2 {
		t.Fatalf("want 2 messages, got %d", len(msgs))
	}
	if decodePayload(msgs[0]) != "hello" {
		t.Errorf("msg 0: want hello, got %q", decodePayload(msgs[0]))
	}
	if decodePayload(msgs[1]) != "world" {
		t.Errorf("msg 1: want world, got %q", decodePayload(msgs[1]))
	}
}

func TestFetchEmptyReturnsEmptySlice(t *testing.T) {
	ts := newTestServer(t)
	msgs := fetch(t, ts.URL, "empty-topic", "g1", 10)
	if len(msgs) != 0 {
		t.Errorf("want 0 messages on empty topic, got %d", len(msgs))
	}
}

func TestFetchRequiresGroup(t *testing.T) {
	ts := newTestServer(t)
	resp, _ := http.Get(ts.URL + "/topics/t1/messages")
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("missing group: want 400, got %d", resp.StatusCode)
	}
}

func TestFetchOffsetsAreMonotonicallyIncreasing(t *testing.T) {
	ts := newTestServer(t)
	for i := 0; i < 5; i++ {
		publish(t, ts.URL, "t1", fmt.Sprintf("m%d", i))
	}
	msgs := fetch(t, ts.URL, "t1", "g1", 10)
	for i := 1; i < len(msgs); i++ {
		prev := msgs[i-1]["offset"].(float64)
		curr := msgs[i]["offset"].(float64)
		if curr <= prev {
			t.Errorf("offsets not increasing: [%d]=%v [%d]=%v", i-1, prev, i, curr)
		}
	}
}

// ── Competing consumers ───────────────────────────────────────────────────────

func TestCompetingConsumers(t *testing.T) {
	ts := newTestServer(t)
	for i := 0; i < 6; i++ {
		publish(t, ts.URL, "t1", fmt.Sprintf("msg%d", i))
	}

	msgs1 := fetch(t, ts.URL, "t1", "g1", 3)
	msgs2 := fetch(t, ts.URL, "t1", "g1", 3)

	if len(msgs1) != 3 {
		t.Fatalf("consumer 1: want 3, got %d", len(msgs1))
	}
	if len(msgs2) != 3 {
		t.Fatalf("consumer 2: want 3, got %d", len(msgs2))
	}
	off1 := msgs1[0]["offset"].(float64)
	off2 := msgs2[0]["offset"].(float64)
	if off1 == off2 {
		t.Errorf("competing consumers got the same starting offset %v", off1)
	}
}

func TestIndependentGroupsGetAllMessages(t *testing.T) {
	ts := newTestServer(t)
	for i := 0; i < 3; i++ {
		publish(t, ts.URL, "t1", fmt.Sprintf("m%d", i))
	}

	ga := fetch(t, ts.URL, "t1", "groupA", 10)
	gb := fetch(t, ts.URL, "t1", "groupB", 10)

	if len(ga) != 3 {
		t.Errorf("groupA: want 3, got %d", len(ga))
	}
	if len(gb) != 3 {
		t.Errorf("groupB: want 3, got %d", len(gb))
	}
	// Both groups should start at offset 0.
	if ga[0]["offset"].(float64) != 0 || gb[0]["offset"].(float64) != 0 {
		t.Error("independent groups should both start at offset 0")
	}
}

// ── Commit ────────────────────────────────────────────────────────────────────

func TestCommitAdvancesGroupPosition(t *testing.T) {
	ts := newTestServer(t)
	for i := 0; i < 4; i++ {
		publish(t, ts.URL, "t1", fmt.Sprintf("m%d", i))
	}
	fetch(t, ts.URL, "t1", "g1", 4) // dispatches offsets 0-3

	// Commit at offset 3 (the last message in the batch).
	body, _ := json.Marshal(map[string]any{"group_id": "g1", "offset": 3})
	resp, _ := http.Post(ts.URL+"/topics/t1/commit", "application/json", bytes.NewReader(body))
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("commit: want 204, got %d", resp.StatusCode)
	}

	// Nothing left to consume for this group.
	msgs := fetch(t, ts.URL, "t1", "g1", 10)
	if len(msgs) != 0 {
		t.Errorf("after full commit: want 0 messages, got %d", len(msgs))
	}
}

func TestCommitIsIdempotent(t *testing.T) {
	ts := newTestServer(t)
	publish(t, ts.URL, "t1", "msg")
	fetch(t, ts.URL, "t1", "g1", 1)

	body, _ := json.Marshal(map[string]any{"group_id": "g1", "offset": 0})
	for i := 0; i < 3; i++ {
		resp, _ := http.Post(ts.URL+"/topics/t1/commit", "application/json", bytes.NewReader(body))
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Errorf("idempotent commit %d: want 204, got %d", i, resp.StatusCode)
		}
	}
}

// ── Nack / retry / DLQ ───────────────────────────────────────────────────────

func TestNackRollsBackForRetry(t *testing.T) {
	ts := newTestServer(t)
	publish(t, ts.URL, "t1", "flaky")

	msgs := fetch(t, ts.URL, "t1", "g1", 1)
	if len(msgs) != 1 {
		t.Fatalf("first fetch: want 1, got %d", len(msgs))
	}
	off := msgs[0]["offset"].(float64)

	// Nack once (retries=1, below MaxRetries=2).
	body, _ := json.Marshal(map[string]any{"group_id": "g1", "offset": off})
	resp, _ := http.Post(ts.URL+"/topics/t1/nack", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	// Same message must be redelivered.
	retried := fetch(t, ts.URL, "t1", "g1", 1)
	if len(retried) != 1 {
		t.Fatalf("after nack: want 1, got %d", len(retried))
	}
	if retried[0]["offset"].(float64) != off {
		t.Errorf("nack retry: want same offset %.0f, got %.0f", off, retried[0]["offset"].(float64))
	}
}

func TestNackExhaustedRoutesToDLQ(t *testing.T) {
	ts := newTestServer(t)
	publish(t, ts.URL, "t1", "poison")

	// Nack MaxRetries (2) times.
	for i := 0; i < 2; i++ {
		msgs := fetch(t, ts.URL, "t1", "g1", 1)
		if len(msgs) == 0 {
			t.Fatalf("iteration %d: expected message before DLQ routing", i)
		}
		off := msgs[0]["offset"].(float64)
		body, _ := json.Marshal(map[string]any{"group_id": "g1", "offset": off})
		resp, _ := http.Post(ts.URL+"/topics/t1/nack", "application/json", bytes.NewReader(body))
		resp.Body.Close()
	}

	// Original topic should now be empty for this group.
	if msgs := fetch(t, ts.URL, "t1", "g1", 1); len(msgs) != 0 {
		t.Errorf("after DLQ routing: want 0 in t1, got %d", len(msgs))
	}

	// DLQ topic should contain the poisoned message.
	dlq := fetch(t, ts.URL, "t1-dlq", "dlq-inspector", 1)
	if len(dlq) != 1 {
		t.Errorf("DLQ: want 1 message, got %d", len(dlq))
	}
	if decodePayload(dlq[0]) != "poison" {
		t.Errorf("DLQ payload: want poison, got %q", decodePayload(dlq[0]))
	}
}

// ── Backpressure ──────────────────────────────────────────────────────────────

func TestBackpressure429WhenFull(t *testing.T) {
	ts := newTestServer(t) // HighWaterMark=100

	for i := 0; i < 100; i++ {
		publish(t, ts.URL, "t1", "x")
	}
	body, _ := json.Marshal(map[string]string{
		"payload":     base64.StdEncoding.EncodeToString([]byte("overflow")),
		"producer_id": "test",
	})
	resp, _ := http.Post(ts.URL+"/topics/t1/messages", "application/json", bytes.NewReader(body))
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("overflow: want 429, got %d", resp.StatusCode)
	}
}

// ── Utility endpoints ─────────────────────────────────────────────────────────

func TestHealthz(t *testing.T) {
	ts := newTestServer(t)
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz: want 200, got %d", resp.StatusCode)
	}
}

func TestListTopics(t *testing.T) {
	ts := newTestServer(t)
	publish(t, ts.URL, "topicA", "x")
	publish(t, ts.URL, "topicB", "y")

	resp, _ := http.Get(ts.URL + "/topics")
	defer resp.Body.Close()

	var result struct {
		Topics []struct {
			Name  string `json:"name"`
			Depth int    `json:"depth"`
		} `json:"topics"`
	}
	json.NewDecoder(resp.Body).Decode(&result)

	if len(result.Topics) != 2 {
		t.Errorf("want 2 topics, got %d", len(result.Topics))
	}
	for _, topic := range result.Topics {
		if topic.Depth != 1 {
			t.Errorf("topic %s: want depth 1, got %d", topic.Name, topic.Depth)
		}
	}
}

func TestMetricsFormat(t *testing.T) {
	ts := newTestServer(t)
	publish(t, ts.URL, "t1", "msg")
	fetch(t, ts.URL, "t1", "g1", 1) // creates a consumer group

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	body := string(b)

	for _, want := range []string{
		"mq_queue_depth",
		"mq_publish_total",
		"mq_consume_total",
		"mq_consumer_lag",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics: missing %q", want)
		}
	}
}

// ── WAL replay on restart ─────────────────────────────────────────────────────

func TestWALReplayRestoresMessages(t *testing.T) {
	dir := t.TempDir()
	cfg := server.Config{
		WALPath:       filepath.Join(dir, "wal.log"),
		OffsetPath:    filepath.Join(dir, "offsets.json"),
		HighWaterMark: 100,
		MaxRetries:    2,
		PollTimeoutMS: 100,
	}

	// First server: publish 3 messages.
	srv1, _ := server.New(cfg)
	ts1 := httptest.NewServer(srv1)
	publish(t, ts1.URL, "t1", "a")
	publish(t, ts1.URL, "t1", "b")
	publish(t, ts1.URL, "t1", "c")
	ts1.Close()
	srv1.Close()

	// Second server: same WAL — should see all 3 messages.
	srv2, err := server.New(cfg)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	ts2 := httptest.NewServer(srv2)
	defer ts2.Close()
	defer srv2.Close()

	msgs := fetch(t, ts2.URL, "t1", "g1", 10)
	if len(msgs) != 3 {
		t.Fatalf("after restart: want 3 messages, got %d", len(msgs))
	}
	payloads := []string{decodePayload(msgs[0]), decodePayload(msgs[1]), decodePayload(msgs[2])}
	for i, want := range []string{"a", "b", "c"} {
		if payloads[i] != want {
			t.Errorf("message %d: want %q, got %q", i, want, payloads[i])
		}
	}
}
