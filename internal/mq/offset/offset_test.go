package offset_test

import (
	"path/filepath"
	"testing"

	"gpqueue/internal/mq/offset"
)

func TestGetDefaultsToZero(t *testing.T) {
	s, err := offset.Load(filepath.Join(t.TempDir(), "offsets.json"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := s.Get("topic", "group"); got != 0 {
		t.Errorf("want 0 for unknown key, got %d", got)
	}
}

func TestSetAndGet(t *testing.T) {
	s, _ := offset.Load(filepath.Join(t.TempDir(), "offsets.json"))

	if err := s.Set("t1", "g1", 42); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got := s.Get("t1", "g1"); got != 42 {
		t.Errorf("want 42, got %d", got)
	}
}

func TestSetDoesNotBleedAcrossKeys(t *testing.T) {
	s, _ := offset.Load(filepath.Join(t.TempDir(), "offsets.json"))
	s.Set("t1", "g1", 10)
	s.Set("t1", "g2", 20)

	if got := s.Get("t1", "g1"); got != 10 {
		t.Errorf("t1/g1: want 10, got %d", got)
	}
	if got := s.Get("t1", "g2"); got != 20 {
		t.Errorf("t1/g2: want 20, got %d", got)
	}
	if got := s.Get("t2", "g1"); got != 0 {
		t.Errorf("t2/g1: want 0, got %d", got)
	}
}

func TestPersistAndReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offsets.json")

	s1, _ := offset.Load(path)
	s1.Set("t1", "g1", 100)
	s1.Set("t2", "g2", 200)

	s2, err := offset.Load(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got := s2.Get("t1", "g1"); got != 100 {
		t.Errorf("t1/g1 after reload: want 100, got %d", got)
	}
	if got := s2.Get("t2", "g2"); got != 200 {
		t.Errorf("t2/g2 after reload: want 200, got %d", got)
	}
}

func TestLoadMissingFile(t *testing.T) {
	s, err := offset.Load(filepath.Join(t.TempDir(), "nonexistent.json"))
	if err != nil {
		t.Fatalf("loading missing file should not error, got %v", err)
	}
	if got := s.Get("any", "group"); got != 0 {
		t.Errorf("empty store should return 0, got %d", got)
	}
}

func TestSetIsIdempotent(t *testing.T) {
	s, _ := offset.Load(filepath.Join(t.TempDir(), "offsets.json"))
	s.Set("t1", "g1", 5)
	s.Set("t1", "g1", 5)
	if got := s.Get("t1", "g1"); got != 5 {
		t.Errorf("idempotent set: want 5, got %d", got)
	}
}
