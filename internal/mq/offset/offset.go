package offset

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Store persists consumer group committed offsets to a JSON file.
// Keys are "topic:groupID" strings; values are committed offsets.
type Store struct {
	mu   sync.Mutex
	path string
	data map[string]int64
}

// Load reads the file at path; returns an empty store if the file is absent.
func Load(path string) (*Store, error) {
	s := &Store{path: path, data: make(map[string]int64)}
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read offset store: %w", err)
	}
	if err := json.Unmarshal(b, &s.data); err != nil {
		return nil, fmt.Errorf("parse offset store: %w", err)
	}
	return s, nil
}

// Get returns the committed offset for (topic, groupID), defaulting to 0.
func (s *Store) Get(topic, groupID string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data[topic+":"+groupID]
}

// Set updates and atomically persists the committed offset for (topic, groupID).
func (s *Store) Set(topic, groupID string, off int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[topic+":"+groupID] = off
	return s.persist()
}

// persist writes the map atomically via a temp-file rename.
func (s *Store) persist() error {
	b, err := json.Marshal(s.data)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return fmt.Errorf("write offset tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename offset file: %w", err)
	}
	return nil
}
