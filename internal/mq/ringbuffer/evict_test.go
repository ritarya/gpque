package ringbuffer_test

import (
	"testing"

	"gpqueue/internal/mq/ringbuffer"
)

func TestEvict_RemovesEntriesBeforeOffset(t *testing.T) {
	rb := ringbuffer.New(10)
	rb.Append([]byte("a"), "p1") // offset 0
	rb.Append([]byte("b"), "p1") // offset 1
	rb.Append([]byte("c"), "p1") // offset 2

	if rb.Depth() != 3 {
		t.Fatalf("before evict: want depth 3, got %d", rb.Depth())
	}

	rb.Evict(2) // remove offsets < 2 (i.e. 0 and 1)
	if rb.Depth() != 1 {
		t.Errorf("after evict(2): want depth 1, got %d", rb.Depth())
	}
}

func TestEvict_BeyondAllEntries(t *testing.T) {
	rb := ringbuffer.New(10)
	rb.Append([]byte("x"), "p1") // offset 0
	rb.Append([]byte("y"), "p1") // offset 1

	rb.Evict(10) // evict all
	if rb.Depth() != 0 {
		t.Errorf("after evict(all): want depth 0, got %d", rb.Depth())
	}
}

func TestEvict_EmptyBuffer(t *testing.T) {
	rb := ringbuffer.New(5)
	rb.Evict(100) // should not panic on empty buffer
	if rb.Depth() != 0 {
		t.Errorf("want depth 0, got %d", rb.Depth())
	}
}

func TestEvict_NoOpWhenOffsetBeforeStart(t *testing.T) {
	rb := ringbuffer.New(5)
	rb.Append([]byte("m"), "p1") // offset 0

	rb.Evict(0) // offset < 0 does not exist; nothing to evict
	if rb.Depth() != 1 {
		t.Errorf("evict(0): want depth 1, got %d", rb.Depth())
	}
}
