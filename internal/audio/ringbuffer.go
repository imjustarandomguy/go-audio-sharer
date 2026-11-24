package audio

import (
	"math/bits"
	"sync/atomic"
)

// byteRingBuffer is a lock-free single-producer, single-consumer (SPSC)
// circular byte buffer for streaming audio without heap allocations or mutex
// contention on the real-time audio thread.
//
// Concurrency contract:
//   - Write must only be called by a single producer goroutine.
//   - Read and Discard must only be called by a single consumer thread (the
//     audio callback), or Reset while the consumer is stopped.
//   - Len and Cap may be called concurrently from any goroutine.
type byteRingBuffer struct {
	buf  []byte
	mask uint64

	// Pad atomic counters onto separate cache lines to prevent false sharing
	// between the network producer goroutine and the real-time audio thread.
	_    [56]byte
	head atomic.Uint64 // monotonic total bytes written (producer-owned)
	_    [56]byte
	tail atomic.Uint64 // monotonic total bytes read/discarded (consumer-owned)
}

// newByteRingBuffer creates a ring buffer with at least capacity bytes,
// rounded up to the next power of two for fast bitmask indexing.
func newByteRingBuffer(capacity int) *byteRingBuffer {
	if capacity <= 0 {
		capacity = 256 * 1024 // 256 KB default (~1.3s of 48kHz 16-bit stereo)
	}
	size := nextPowerOfTwo(capacity)
	return &byteRingBuffer{
		buf:  make([]byte, size),
		mask: uint64(size - 1),
	}
}

func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	const maxPow2 = 1 << (bits.UintSize - 2)
	if n >= maxPow2 {
		return maxPow2
	}
	v := 1
	for v < n {
		v <<= 1
	}
	return v
}

// Len returns the approximate number of unread bytes available to read.
func (r *byteRingBuffer) Len() int {
	h := r.head.Load()
	t := r.tail.Load()
	if h <= t {
		return 0
	}
	return int(h - t)
}

// Head returns the monotonic total number of bytes written by the producer.
func (r *byteRingBuffer) Head() uint64 {
	return r.head.Load()
}

// Cap returns the total buffer capacity in bytes.
func (r *byteRingBuffer) Cap() int {
	return len(r.buf)
}

// Write appends as many bytes of p as fit in the buffer without overwriting
// unread data. Returns the number of bytes written.
func (r *byteRingBuffer) Write(p []byte) int {
	return r.WriteAligned(p, 1)
}

// WriteAligned appends as many complete frameBytes-sized frames from p as fit
// in the buffer without overwriting unread data. Returns the number of bytes
// written (always a multiple of frameBytes).
func (r *byteRingBuffer) WriteAligned(p []byte, frameBytes int) int {
	if len(p) == 0 {
		return 0
	}
	if frameBytes <= 0 {
		frameBytes = 1
	}

	h := r.head.Load()
	t := r.tail.Load()
	used := h - t
	capU := uint64(len(r.buf))
	if used >= capU {
		return 0
	}
	free := int(capU - used)

	n := len(p)
	if n > free {
		n = free
	}
	if frameBytes > 1 {
		n = (n / frameBytes) * frameBytes
	}
	if n == 0 {
		return 0
	}

	idx := int(h & r.mask)
	first := copy(r.buf[idx:], p[:n])
	if first < n {
		copy(r.buf, p[first:n])
	}
	r.head.Store(h + uint64(n))
	return n
}

// Read reads up to len(p) unread bytes from the buffer into p.
// Returns the number of bytes read. Must only be called by the consumer.
func (r *byteRingBuffer) Read(p []byte) int {
	if len(p) == 0 {
		return 0
	}
	t := r.tail.Load()
	h := r.head.Load()
	if h <= t {
		return 0
	}
	avail := int(h - t)
	n := len(p)
	if n > avail {
		n = avail
	}

	idx := int(t & r.mask)
	first := copy(p[:n], r.buf[idx:])
	if first < n {
		copy(p[first:n], r.buf[:n-first])
	}
	r.tail.Store(t + uint64(n))
	return n
}

// Discard advances the read cursor past up to n unread bytes.
// Must only be called by the consumer (or when the consumer is stopped).
func (r *byteRingBuffer) Discard(n int) int {
	if n <= 0 {
		return 0
	}
	t := r.tail.Load()
	h := r.head.Load()
	if h <= t {
		return 0
	}
	avail := int(h - t)
	if n > avail {
		n = avail
	}
	r.tail.Store(t + uint64(n))
	return n
}

// Reset discards all currently buffered unread bytes by advancing the consumer
// tail to the current producer head. Safe to call when the consumer is stopped,
// even if the producer is concurrently writing.
func (r *byteRingBuffer) Reset() {
	r.tail.Store(r.head.Load())
}
