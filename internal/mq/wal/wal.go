package wal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Entry is one record in the write-ahead log.
// Payload is marshalled as base64 by encoding/json automatically.
type Entry struct {
	Topic       string    `json:"topic"`
	Offset      int64     `json:"offset"`
	Payload     []byte    `json:"payload"`
	PublishedAt time.Time `json:"published_at"`
	ProducerID  string    `json:"producer_id"`
}

// WAL is a mutex-guarded, fsync-on-write, append-only log file.
type WAL struct {
	mu  sync.Mutex
	f   *os.File
	enc *json.Encoder
}

// Open creates or appends to the WAL at path.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open wal %q: %w", path, err)
	}
	return &WAL{f: f, enc: json.NewEncoder(f)}, nil
}

// Append writes e as one JSON line and syncs to disk.
func (w *WAL) Append(e Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.enc.Encode(e); err != nil {
		return fmt.Errorf("wal encode: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal sync: %w", err)
	}
	return nil
}

// Replay reads every valid entry from path and calls fn for each.
// Returns nil if the file does not exist.
func Replay(path string, fn func(Entry)) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("replay open %q: %w", path, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 4<<20), 4<<20) // 4 MiB per line
	for sc.Scan() {
		b := sc.Bytes()
		if len(b) == 0 {
			continue
		}
		var e Entry
		if json.Unmarshal(b, &e) == nil {
			fn(e)
		}
	}
	return sc.Err()
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}
