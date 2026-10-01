package audio

import (
	"sync"
)

// byteRingBuffer is a circular byte buffer for streaming audio data without
// allocating on the heap during playback.
type byteRingBuffer struct {
	mu    sync.Mutex
	buf   []byte
	head  int // write index
	tail  int // read index
	count int // number of unread bytes
}

// newByteRingBuffer creates a ring buffer with the given capacity in bytes.
func newByteRingBuffer(capacity int) *byteRingBuffer {
	if capacity <= 0 {
		capacity = 256 * 1024 // 256 KB default (~1.3s of 48kHz 16-bit stereo)
	}
	return &byteRingBuffer{
		buf: make([]byte, capacity),
	}
}

// Len returns the number of buffered bytes available to read.
func (r *byteRingBuffer) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

// Cap returns the total buffer capacity in bytes.
func (r *byteRingBuffer) Cap() int {
	return len(r.buf)
}

// Write writes data into the buffer. If the buffer is full, it discards the
// oldest unread bytes to make room. Returns the number of bytes written.
func (r *byteRingBuffer) Write(p []byte) int {
	if len(p) == 0 {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	// If p is larger than total capacity, only take the newest tail portion.
	if len(p) > len(r.buf) {
		p = p[len(p)-len(r.buf):]
	}

	avail := len(r.buf) - r.count
	if len(p) > avail {
		r.discardLocked(len(p) - avail)
	}

	n := len(p)
	first := copy(r.buf[r.head:], p)
	if first < n {
		copy(r.buf, p[first:])
	}
	r.head = (r.head + n) % len(r.buf)
	r.count += n
	return n
}

// Read reads up to len(p) bytes from the buffer into p.
// Returns the number of bytes read.
func (r *byteRingBuffer) Read(p []byte) int {
	if len(p) == 0 {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count == 0 {
		return 0
	}

	n := len(p)
	if n > r.count {
		n = r.count
	}

	first := copy(p, r.buf[r.tail:min(r.tail+n, len(r.buf))])
	if first < n {
		copy(p[first:], r.buf[:n-first])
	}
	r.tail = (r.tail + n) % len(r.buf)
	r.count -= n
	return n
}

// Discard discards up to n unread bytes from the oldest buffered audio.
func (r *byteRingBuffer) Discard(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.discardLocked(n)
}

func (r *byteRingBuffer) discardLocked(n int) int {
	if n <= 0 {
		return 0
	}
	if n > r.count {
		n = r.count
	}
	r.tail = (r.tail + n) % len(r.buf)
	r.count -= n
	return n
}

// Reset clears the buffer.
func (r *byteRingBuffer) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.head = 0
	r.tail = 0
	r.count = 0
}
