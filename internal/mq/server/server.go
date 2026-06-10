package server

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gpqueue/internal/mq/offset"
	"gpqueue/internal/mq/ringbuffer"
	"gpqueue/internal/mq/wal"
)

// Config holds the runtime configuration for the queue service.
type Config struct {
	WALPath       string
	OffsetPath    string
	HighWaterMark int
	MaxRetries    int
	PollTimeoutMS int
}

// Server is the HTTP message queue service.
type Server struct {
	cfg      Config
	wal      *wal.WAL
	offsets  *offset.Store
	topicsMu sync.RWMutex
	topics   map[string]*mqTopic
	mux      *http.ServeMux
}

type mqTopic struct {
	name     string
	rb       *ringbuffer.RingBuffer
	groupsMu sync.Mutex
	groups   map[string]*groupState
	publishN atomic.Int64
	consumeN atomic.Int64
}

// groupState tracks per-consumer-group delivery position.
// committed is durably persisted; dispatched is volatile and resets to
// committed on restart, ensuring at-least-once redelivery after a crash.
type groupState struct {
	mu         sync.Mutex
	committed  int64
	dispatched int64 // always >= committed; next offset to dispatch
	retries    map[int64]int
}

func (t *mqTopic) getOrCreateGroup(groupID string, initOff int64) *groupState {
	t.groupsMu.Lock()
	defer t.groupsMu.Unlock()
	if g, ok := t.groups[groupID]; ok {
		return g
	}
	g := &groupState{
		committed:  initOff,
		dispatched: initOff,
		retries:    make(map[int64]int),
	}
	t.groups[groupID] = g
	return g
}

// New creates a Server, replays the WAL to restore ring buffers, and wires routes.
func New(cfg Config) (*Server, error) {
	w, err := wal.Open(cfg.WALPath)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	offs, err := offset.Load(cfg.OffsetPath)
	if err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("load offsets: %w", err)
	}

	s := &Server{
		cfg:     cfg,
		wal:     w,
		offsets: offs,
		topics:  make(map[string]*mqTopic),
		mux:     http.NewServeMux(),
	}

	if err := wal.Replay(cfg.WALPath, func(e wal.Entry) {
		t := s.getOrCreateTopic(e.Topic)
		t.rb.AppendEntry(ringbuffer.Entry{
			Offset:      e.Offset,
			Payload:     e.Payload,
			PublishedAt: e.PublishedAt,
			ProducerID:  e.ProducerID,
		})
	}); err != nil {
		return nil, fmt.Errorf("wal replay: %w", err)
	}

	s.mux.HandleFunc("POST /topics/{topic}/messages", s.handlePublish)
	s.mux.HandleFunc("GET /topics/{topic}/messages", s.handleFetch)
	s.mux.HandleFunc("POST /topics/{topic}/commit", s.handleCommit)
	s.mux.HandleFunc("POST /topics/{topic}/nack", s.handleNack)
	s.mux.HandleFunc("GET /topics", s.handleListTopics)
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)

	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Close shuts down the WAL.
func (s *Server) Close() { _ = s.wal.Close() }

func (s *Server) getOrCreateTopic(name string) *mqTopic {
	s.topicsMu.RLock()
	t, ok := s.topics[name]
	s.topicsMu.RUnlock()
	if ok {
		return t
	}
	s.topicsMu.Lock()
	defer s.topicsMu.Unlock()
	if t, ok = s.topics[name]; ok {
		return t
	}
	t = &mqTopic{
		name:   name,
		rb:     ringbuffer.New(s.cfg.HighWaterMark),
		groups: make(map[string]*groupState),
	}
	s.topics[name] = t
	return t
}

// handlePublish handles POST /topics/{topic}/messages
func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	var req struct {
		Payload    string `json:"payload"`
		ProducerID string `json:"producer_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	payload, err := base64.StdEncoding.DecodeString(req.Payload)
	if err != nil {
		http.Error(w, "invalid base64 payload", http.StatusBadRequest)
		return
	}

	t := s.getOrCreateTopic(topicName)
	off, ok := t.rb.Append(payload, req.ProducerID)
	if !ok {
		http.Error(w, "queue full", http.StatusTooManyRequests)
		return
	}

	// WAL is written after ring buffer so we have the assigned offset.
	// Messages in the ring buffer but not yet WAL-written can be lost on a
	// hard crash; producers should retry on error (at-least-once delivery).
	if err := s.wal.Append(wal.Entry{
		Topic:       topicName,
		Offset:      off,
		Payload:     payload,
		PublishedAt: time.Now(),
		ProducerID:  req.ProducerID,
	}); err != nil {
		log.Printf("wal append failed for offset %d: %v", off, err)
	}

	t.publishN.Add(1)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]int64{"offset": off})
}

// handleFetch handles GET /topics/{topic}/messages?group=&limit=
// Long-polls up to PollTimeoutMS when no messages are available.
func (s *Server) handleFetch(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	groupID := r.URL.Query().Get("group")
	if groupID == "" {
		http.Error(w, "group query parameter required", http.StatusBadRequest)
		return
	}
	limit := 10
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 1000 {
		limit = 1000
	}

	t := s.getOrCreateTopic(topicName)
	g := t.getOrCreateGroup(groupID, s.offsets.Get(topicName, groupID))

	// tryRead atomically reads up to limit messages and advances dispatched.
	// Holding g.mu across rb.Read prevents two concurrent consumers getting
	// the same messages (competing-consumer delivery guarantee).
	tryRead := func() []ringbuffer.Entry {
		g.mu.Lock()
		defer g.mu.Unlock()
		msgs := t.rb.Read(g.dispatched, limit)
		if len(msgs) > 0 {
			g.dispatched = msgs[len(msgs)-1].Offset + 1
		}
		return msgs
	}

	type msgJSON struct {
		Offset      int64     `json:"offset"`
		Payload     string    `json:"payload"`
		PublishedAt time.Time `json:"published_at"`
		ProducerID  string    `json:"producer_id"`
	}
	respond := func(msgs []ringbuffer.Entry) {
		out := make([]msgJSON, len(msgs))
		for i, m := range msgs {
			out[i] = msgJSON{
				Offset:      m.Offset,
				Payload:     base64.StdEncoding.EncodeToString(m.Payload),
				PublishedAt: m.PublishedAt,
				ProducerID:  m.ProducerID,
			}
		}
		var nextOffset int64
		if len(msgs) > 0 {
			nextOffset = msgs[len(msgs)-1].Offset + 1
			t.consumeN.Add(int64(len(msgs)))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"messages":    out,
			"next_offset": nextOffset,
		})
	}

	if msgs := tryRead(); len(msgs) > 0 {
		respond(msgs)
		return
	}

	// Long-poll: wait for new messages or timeout.
	deadline := time.Now().Add(time.Duration(s.cfg.PollTimeoutMS) * time.Millisecond)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			respond(nil)
			return
		}

		notify, unsub := t.rb.Subscribe()
		// Re-check after subscribing to close the window between the failed
		// tryRead above and the subscription registration.
		if msgs := tryRead(); len(msgs) > 0 {
			unsub()
			respond(msgs)
			return
		}

		select {
		case <-notify:
		case <-time.After(remaining):
			unsub()
			respond(nil)
			return
		case <-r.Context().Done():
			unsub()
			return
		}
		unsub()

		if msgs := tryRead(); len(msgs) > 0 {
			respond(msgs)
			return
		}
	}
}

// handleCommit handles POST /topics/{topic}/commit
func (s *Server) handleCommit(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	var req struct {
		GroupID string `json:"group_id"`
		Offset  int64  `json:"offset"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	t := s.getOrCreateTopic(topicName)
	g := t.getOrCreateGroup(req.GroupID, s.offsets.Get(topicName, req.GroupID))

	newCommitted := req.Offset + 1
	g.mu.Lock()
	if newCommitted > g.committed {
		g.committed = newCommitted
		if newCommitted > g.dispatched {
			g.dispatched = newCommitted
		}
		for off := range g.retries {
			if off < newCommitted {
				delete(g.retries, off)
			}
		}
	}
	g.mu.Unlock()

	if err := s.offsets.Set(topicName, req.GroupID, newCommitted); err != nil {
		log.Printf("persist offset failed: %v", err)
		http.Error(w, "failed to persist offset", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleNack handles POST /topics/{topic}/nack
func (s *Server) handleNack(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	var req struct {
		GroupID string `json:"group_id"`
		Offset  int64  `json:"offset"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	t := s.getOrCreateTopic(topicName)
	g := t.getOrCreateGroup(req.GroupID, s.offsets.Get(topicName, req.GroupID))

	g.mu.Lock()
	g.retries[req.Offset]++
	retries := g.retries[req.Offset]

	if retries >= s.cfg.MaxRetries {
		// Exceeded retry limit — route to DLQ and advance past the bad message.
		delete(g.retries, req.Offset)
		next := req.Offset + 1
		if next > g.committed {
			g.committed = next
		}
		if next > g.dispatched {
			g.dispatched = next
		}
		g.mu.Unlock()

		s.routeToDLQ(topicName, req.Offset)
		if err := s.offsets.Set(topicName, req.GroupID, next); err != nil {
			log.Printf("persist dlq offset failed: %v", err)
		}
	} else {
		// Roll back dispatched so the message is redelivered on next fetch.
		if req.Offset < g.dispatched {
			g.dispatched = req.Offset
		}
		g.mu.Unlock()
	}

	w.WriteHeader(http.StatusNoContent)
}

// routeToDLQ publishes the payload at offset from topicName into the DLQ topic.
func (s *Server) routeToDLQ(topicName string, offset int64) {
	s.topicsMu.RLock()
	src, ok := s.topics[topicName]
	s.topicsMu.RUnlock()
	if !ok {
		return
	}
	entries := src.rb.Read(offset, 1)
	if len(entries) == 0 || entries[0].Offset != offset {
		log.Printf("dlq: payload at offset %d not in ring buffer, skipping", offset)
		return
	}
	payload := entries[0].Payload

	dlqName := topicName + "-dlq"
	dt := s.getOrCreateTopic(dlqName)
	off, ok := dt.rb.Append(payload, "mq-server")
	if !ok {
		log.Printf("dlq topic %s full, dropping offset %d", dlqName, offset)
		return
	}
	if err := s.wal.Append(wal.Entry{
		Topic:       dlqName,
		Offset:      off,
		Payload:     payload,
		PublishedAt: time.Now(),
		ProducerID:  "mq-server",
	}); err != nil {
		log.Printf("wal append dlq failed: %v", err)
	}
}

// handleListTopics handles GET /topics
func (s *Server) handleListTopics(w http.ResponseWriter, r *http.Request) {
	s.topicsMu.RLock()
	defer s.topicsMu.RUnlock()
	type topicInfo struct {
		Name  string `json:"name"`
		Depth int    `json:"depth"`
	}
	list := make([]topicInfo, 0, len(s.topics))
	for name, t := range s.topics {
		list = append(list, topicInfo{Name: name, Depth: t.rb.Depth()})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"topics": list})
}

// handleHealthz handles GET /healthz
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// handleMetrics handles GET /metrics (Prometheus text format)
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	s.topicsMu.RLock()
	topics := make([]*mqTopic, 0, len(s.topics))
	for _, t := range s.topics {
		topics = append(topics, t)
	}
	s.topicsMu.RUnlock()

	var sb strings.Builder

	fmt.Fprintf(&sb, "# HELP mq_queue_depth Number of messages buffered per topic.\n")
	fmt.Fprintf(&sb, "# TYPE mq_queue_depth gauge\n")
	for _, t := range topics {
		fmt.Fprintf(&sb, "mq_queue_depth{topic=%q} %d\n", t.name, t.rb.Depth())
	}

	fmt.Fprintf(&sb, "# HELP mq_publish_total Total messages published per topic.\n")
	fmt.Fprintf(&sb, "# TYPE mq_publish_total counter\n")
	for _, t := range topics {
		fmt.Fprintf(&sb, "mq_publish_total{topic=%q} %d\n", t.name, t.publishN.Load())
	}

	fmt.Fprintf(&sb, "# HELP mq_consume_total Total messages dispatched per topic.\n")
	fmt.Fprintf(&sb, "# TYPE mq_consume_total counter\n")
	for _, t := range topics {
		fmt.Fprintf(&sb, "mq_consume_total{topic=%q} %d\n", t.name, t.consumeN.Load())
	}

	fmt.Fprintf(&sb, "# HELP mq_consumer_lag Messages not yet committed per consumer group.\n")
	fmt.Fprintf(&sb, "# TYPE mq_consumer_lag gauge\n")
	for _, t := range topics {
		nextOff := t.rb.NextOffset()
		t.groupsMu.Lock()
		for gid, g := range t.groups {
			g.mu.Lock()
			lag := nextOff - g.committed
			g.mu.Unlock()
			if lag < 0 {
				lag = 0
			}
			fmt.Fprintf(&sb, "mq_consumer_lag{topic=%q,group=%q} %d\n", t.name, gid, lag)
		}
		t.groupsMu.Unlock()
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(sb.String()))
}
