package ringbuffer_test

import (
	"testing"
	"time"

	"gpqueue/internal/mq/ringbuffer"
)

func TestAppendAndRead(t *testing.T) {
	rb := ringbuffer.New(5)

	off, ok := rb.Append([]byte("hello"), "p1")
	if !ok || off != 0 {
		t.Fatalf("first append: ok=%v off=%d", ok, off)
	}
	off, ok = rb.Append([]byte("world"), "p1")
	if !ok || off != 1 {
		t.Fatalf("second append: ok=%v off=%d", ok, off)
	}

	entries := rb.Read(0, 10)
	if len(entries) != 2 {
		t.Fatalf("want 2 entries, got %d", len(entries))
	}
	if string(entries[0].Payload) != "hello" {
		t.Errorf("entry 0: want hello, got %s", entries[0].Payload)
	}
	if string(entries[1].Payload) != "world" {
		t.Errorf("entry 1: want world, got %s", entries[1].Payload)
	}
}

func TestReadFutureOffset(t *testing.T) {
	rb := ringbuffer.New(5)
	rb.Append([]byte("x"), "p1")

	if got := rb.Read(100, 10); len(got) != 0 {
		t.Errorf("expected empty for future offset, got %d entries", len(got))
	}
}

func TestReadLimit(t *testing.T) {
	rb := ringbuffer.New(10)
	for i := 0; i < 8; i++ {
		rb.Append([]byte("msg"), "p1")
	}

	entries := rb.Read(0, 3)
	if len(entries) != 3 {
		t.Fatalf("want 3 entries, got %d", len(entries))
	}
	if entries[0].Offset != 0 || entries[2].Offset != 2 {
		t.Errorf("unexpected offsets %d %d", entries[0].Offset, entries[2].Offset)
	}
}

func TestReadFromMidpoint(t *testing.T) {
	rb := ringbuffer.New(10)
	for i := 0; i < 5; i++ {
		rb.Append([]byte("msg"), "p1")
	}

	entries := rb.Read(2, 10)
	if len(entries) != 3 {
		t.Fatalf("want 3 entries from offset 2, got %d", len(entries))
	}
	if entries[0].Offset != 2 {
		t.Errorf("first entry: want offset 2, got %d", entries[0].Offset)
	}
}

func TestBackpressureWhenFull(t *testing.T) {
	rb := ringbuffer.New(3)
	for i := 0; i < 3; i++ {
		if _, ok := rb.Append([]byte("msg"), "p1"); !ok {
			t.Fatalf("append %d should succeed", i)
		}
	}
	if _, ok := rb.Append([]byte("overflow"), "p1"); ok {
		t.Error("append to full buffer should return false")
	}
}

func TestDepth(t *testing.T) {
	rb := ringbuffer.New(5)
	if rb.Depth() != 0 {
		t.Errorf("new buffer depth: want 0, got %d", rb.Depth())
	}
	rb.Append([]byte("a"), "p1")
	rb.Append([]byte("b"), "p1")
	if rb.Depth() != 2 {
		t.Errorf("after 2 appends: want depth 2, got %d", rb.Depth())
	}
}

func TestNextOffset(t *testing.T) {
	rb := ringbuffer.New(5)
	if rb.NextOffset() != 0 {
		t.Errorf("initial NextOffset: want 0, got %d", rb.NextOffset())
	}
	rb.Append([]byte("a"), "p1")
	rb.Append([]byte("b"), "p1")
	if rb.NextOffset() != 2 {
		t.Errorf("after 2 appends: want NextOffset 2, got %d", rb.NextOffset())
	}
}

func TestAppendEntry(t *testing.T) {
	rb := ringbuffer.New(5)
	rb.AppendEntry(ringbuffer.Entry{
		Offset:      42,
		Payload:     []byte("replayed"),
		PublishedAt: time.Now(),
		ProducerID:  "wal-replay",
	})

	if rb.NextOffset() != 43 {
		t.Errorf("want NextOffset 43, got %d", rb.NextOffset())
	}
	entries := rb.Read(42, 1)
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	if string(entries[0].Payload) != "replayed" {
		t.Errorf("want payload 'replayed', got %q", entries[0].Payload)
	}
}

func TestAppendEntryEvictsOldestWhenFull(t *testing.T) {
	rb := ringbuffer.New(3)
	for i := 0; i < 3; i++ {
		rb.AppendEntry(ringbuffer.Entry{Offset: int64(i), Payload: []byte("x"), PublishedAt: time.Now()})
	}
	// Buffer is full; AppendEntry should evict offset 0 to make room.
	rb.AppendEntry(ringbuffer.Entry{Offset: 3, Payload: []byte("new"), PublishedAt: time.Now()})

	if rb.Depth() != 3 {
		t.Errorf("depth should still be 3 after eviction, got %d", rb.Depth())
	}
	if rb.NextOffset() != 4 {
		t.Errorf("want NextOffset 4, got %d", rb.NextOffset())
	}
	// Offset 0 should be gone; reading from 1 should give 3 entries.
	entries := rb.Read(0, 10)
	if len(entries) != 3 {
		t.Fatalf("want 3 entries (0 evicted), got %d", len(entries))
	}
	if entries[0].Offset != 1 {
		t.Errorf("oldest surviving entry: want offset 1, got %d", entries[0].Offset)
	}
}

func TestSubscribeNotification(t *testing.T) {
	rb := ringbuffer.New(5)
	notify, unsub := rb.Subscribe()
	defer unsub()

	done := make(chan struct{})
	go func() {
		select {
		case <-notify:
			close(done)
		case <-time.After(time.Second):
		}
	}()

	rb.Append([]byte("ping"), "p1")

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("subscriber was not notified within 1s")
	}
}

func TestSubscribeUnsubscribeDoesNotLeak(t *testing.T) {
	rb := ringbuffer.New(5)
	for i := 0; i < 100; i++ {
		_, unsub := rb.Subscribe()
		unsub()
	}
	// If we get here without hanging or panicking, the unsubscribe logic is clean.
}
