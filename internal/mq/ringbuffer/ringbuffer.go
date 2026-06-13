package ringbuffer

import (
	"sync"
	"time"
)

// Entry is a single buffered message.
type Entry struct {
	Offset      int64
	Payload     []byte
	PublishedAt time.Time
	ProducerID  string
}

// RingBuffer is a fixed-capacity, mutex-guarded circular buffer.
// Offsets are monotonically increasing from 0.
type RingBuffer struct {
	mu       sync.Mutex
	buf      []Entry
	capacity int
	start    int   // buf index of the oldest live entry
	count    int   // number of live entries
	nextOff  int64 // offset assigned to the next Append
	subs     []chan struct{}
}

// New allocates a RingBuffer of the given capacity.
func New(capacity int) *RingBuffer {
	return &RingBuffer{buf: make([]Entry, capacity), capacity: capacity}
}

// Append stores a new message and returns (offset, true).
// Returns (0, false) when the buffer is at capacity.
func (rb *RingBuffer) Append(payload []byte, producerID string) (int64, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.count >= rb.capacity {
		return 0, false
	}
	off := rb.nextOff
	idx := (rb.start + rb.count) % rb.capacity
	p := make([]byte, len(payload))
	copy(p, payload)
	rb.buf[idx] = Entry{
		Offset:      off,
		Payload:     p,
		PublishedAt: time.Now(),
		ProducerID:  producerID,
	}
	rb.count++
	rb.nextOff++
	rb.wakeAll()
	return off, true
}

// AppendEntry inserts a pre-built entry (used during WAL replay).
// Silently evicts the oldest entry when the buffer is full.
func (rb *RingBuffer) AppendEntry(e Entry) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.count >= rb.capacity {
		rb.start = (rb.start + 1) % rb.capacity
		rb.count--
	}
	idx := (rb.start + rb.count) % rb.capacity
	rb.buf[idx] = e
	rb.count++
	if e.Offset >= rb.nextOff {
		rb.nextOff = e.Offset + 1
	}
}

// Read returns up to limit entries at offsets [fromOffset, nextOff).
func (rb *RingBuffer) Read(fromOffset int64, limit int) []Entry {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.count == 0 || fromOffset >= rb.nextOff {
		return nil
	}
	oldest := rb.buf[rb.start].Offset
	if fromOffset < oldest {
		fromOffset = oldest
	}
	n := int(rb.nextOff - fromOffset)
	if n > limit {
		n = limit
	}
	skip := int(fromOffset - oldest)
	out := make([]Entry, n)
	for i := range out {
		out[i] = rb.buf[(rb.start+skip+i)%rb.capacity]
	}
	return out
}

// Evict removes all entries with Offset < upToOffset from the front of the buffer,
// freeing capacity for new appends. Call this after consumer groups commit so the
// buffer does not fill permanently with already-consumed messages.
func (rb *RingBuffer) Evict(upToOffset int64) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for rb.count > 0 && rb.buf[rb.start].Offset < upToOffset {
		rb.start = (rb.start + 1) % rb.capacity
		rb.count--
	}
}

// NextOffset returns the offset that will be assigned to the next Append.
func (rb *RingBuffer) NextOffset() int64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.nextOff
}

// Depth returns the number of messages currently buffered.
func (rb *RingBuffer) Depth() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.count
}

// Subscribe returns a notification channel and a cancel function.
// The channel receives a value (non-blocking) whenever new entries are appended.
func (rb *RingBuffer) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	rb.mu.Lock()
	rb.subs = append(rb.subs, ch)
	rb.mu.Unlock()
	return ch, func() {
		rb.mu.Lock()
		for i, s := range rb.subs {
			if s == ch {
				rb.subs[i] = rb.subs[len(rb.subs)-1]
				rb.subs = rb.subs[:len(rb.subs)-1]
				break
			}
		}
		rb.mu.Unlock()
	}
}

// wakeAll signals all subscribers; must be called with rb.mu held.
func (rb *RingBuffer) wakeAll() {
	for _, ch := range rb.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
