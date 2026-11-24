package audio

import (
	"bytes"
	"sync"
	"testing"
)

func TestByteRingBufferBasic(t *testing.T) {
	rb := newByteRingBuffer(16)
	if rb.Len() != 0 {
		t.Fatalf("expected Len 0, got %d", rb.Len())
	}
	if rb.Cap() != 16 {
		t.Fatalf("expected Cap 16, got %d", rb.Cap())
	}

	data := []byte("hello world")
	n := rb.Write(data)
	if n != len(data) {
		t.Fatalf("wrote %d bytes, want %d", n, len(data))
	}
	if rb.Len() != len(data) {
		t.Fatalf("Len %d, want %d", rb.Len(), len(data))
	}

	out := make([]byte, len(data))
	rn := rb.Read(out)
	if rn != len(data) {
		t.Fatalf("read %d bytes, want %d", rn, len(data))
	}
	if !bytes.Equal(out, data) {
		t.Fatalf("got %q, want %q", out, data)
	}
	if rb.Len() != 0 {
		t.Fatalf("expected Len 0 after full read, got %d", rb.Len())
	}
}

func TestByteRingBufferPowerOfTwoCap(t *testing.T) {
	rb := newByteRingBuffer(10)
	if rb.Cap() != 16 {
		t.Fatalf("expected Cap rounded up to 16, got %d", rb.Cap())
	}
}

func TestByteRingBufferWrapAround(t *testing.T) {
	rb := newByteRingBuffer(8)

	// Write 6 bytes
	rb.Write([]byte("123456"))
	// Read 4 bytes (leaves "56", tail at index 4)
	out := make([]byte, 4)
	rb.Read(out)
	if string(out) != "1234" {
		t.Fatalf("got %q, want '1234'", out)
	}

	// Write 5 bytes "ABCDE" -> should wrap around past index 7 back to 0
	rb.Write([]byte("ABCDE"))
	if rb.Len() != 7 { // "56" + "ABCDE" = 7 bytes
		t.Fatalf("expected Len 7, got %d", rb.Len())
	}

	outAll := make([]byte, 7)
	rb.Read(outAll)
	if string(outAll) != "56ABCDE" {
		t.Fatalf("got %q, want '56ABCDE'", outAll)
	}
}

func TestByteRingBufferBackpressureWhenFull(t *testing.T) {
	rb := newByteRingBuffer(8)
	if n := rb.Write([]byte("ABCDEF")); n != 6 {
		t.Fatalf("wrote %d, want 6", n)
	}
	// Only 2 bytes free; SPSC Write must not touch consumer tail.
	if n := rb.Write([]byte("12345")); n != 2 {
		t.Fatalf("wrote %d, want 2", n)
	}
	if rb.Len() != 8 {
		t.Fatalf("Len = %d, want 8", rb.Len())
	}
	out := make([]byte, 8)
	rb.Read(out)
	if string(out) != "ABCDEF12" {
		t.Fatalf("got %q, want 'ABCDEF12'", out)
	}
}

func TestByteRingBufferWriteAligned(t *testing.T) {
	rb := newByteRingBuffer(8)
	// Write 6 bytes with frameBytes=4 -> should only write 4 bytes.
	if n := rb.WriteAligned([]byte("123456"), 4); n != 4 {
		t.Fatalf("WriteAligned = %d, want 4", n)
	}
	if rb.Len() != 4 {
		t.Fatalf("Len = %d, want 4", rb.Len())
	}
}

func TestByteRingBufferDiscard(t *testing.T) {
	rb := newByteRingBuffer(16)
	rb.Write([]byte("0123456789"))
	discarded := rb.Discard(4)
	if discarded != 4 {
		t.Fatalf("discarded %d, want 4", discarded)
	}
	if rb.Len() != 6 {
		t.Fatalf("Len %d, want 6", rb.Len())
	}
	out := make([]byte, 6)
	rb.Read(out)
	if string(out) != "456789" {
		t.Fatalf("got %q, want '456789'", out)
	}
}

func TestByteRingBufferReset(t *testing.T) {
	rb := newByteRingBuffer(16)
	rb.Write([]byte("abcdef"))
	rb.Reset()
	if rb.Len() != 0 {
		t.Fatalf("Len after reset = %d, want 0", rb.Len())
	}
	out := make([]byte, 5)
	if n := rb.Read(out); n != 0 {
		t.Fatalf("read from empty buffer returned %d", n)
	}
}

func TestByteRingBufferConcurrentSPSC(t *testing.T) {
	rb := newByteRingBuffer(4096)
	const totalBytes = 256 * 1024

	var wg sync.WaitGroup
	wg.Add(2)

	// Producer: writes sequential byte pattern 0,1,2,...,255 repeating
	go func() {
		defer wg.Done()
		chunk := make([]byte, 192)
		written := 0
		for written < totalBytes {
			n := len(chunk)
			if written+n > totalBytes {
				n = totalBytes - written
			}
			for i := 0; i < n; i++ {
				chunk[i] = byte((written + i) & 0xFF)
			}
			off := 0
			for off < n {
				w := rb.WriteAligned(chunk[off:n], 4)
				off += w
			}
			written += n
		}
	}()

	// Consumer: reads and verifies every single byte in order
	go func() {
		defer wg.Done()
		buf := make([]byte, 128)
		readTotal := 0
		for readTotal < totalBytes {
			r := rb.Read(buf)
			for i := 0; i < r; i++ {
				want := byte((readTotal + i) & 0xFF)
				if buf[i] != want {
					t.Errorf("byte mismatch at %d: got %d, want %d", readTotal+i, buf[i], want)
					return
				}
			}
			readTotal += r
		}
	}()

	wg.Wait()
	if rb.Len() != 0 {
		t.Fatalf("expected empty ring after full SPSC transfer, got %d", rb.Len())
	}
}

func TestByteRingBufferWriteAlignedWrapAndUnalignedFree(t *testing.T) {
	// Cap = 16; use 6-byte frames so 16 is not a multiple of frameBytes.
	rb := newByteRingBuffer(16)
	if n := rb.WriteAligned(make([]byte, 18), 6); n != 12 {
		t.Fatalf("WriteAligned = %d, want 12", n)
	}
	// Only 4 bytes free (< 6), so another WriteAligned(6) must write 0.
	if n := rb.WriteAligned(make([]byte, 6), 6); n != 0 {
		t.Fatalf("WriteAligned with < frameBytes free = %d, want 0", n)
	}
	// Read 6 bytes -> 10 bytes free -> WriteAligned(12, 6) writes 6 bytes wrapping around.
	out := make([]byte, 6)
	rb.Read(out)
	if n := rb.WriteAligned([]byte("ABCDEF123456"), 6); n != 6 {
		t.Fatalf("WriteAligned wrap = %d, want 6", n)
	}
	if rb.Len()%6 != 0 {
		t.Fatalf("Len %d is not aligned to 6-byte frame", rb.Len())
	}
}

func TestByteRingBufferConcurrentWriteAlignedAndReset(t *testing.T) {
	// Verifies the SPSC Reset contract relied on by Player.followDefault:
	// consumer is stopped while Reset() advances tail concurrently with WriteAligned().
	rb := newByteRingBuffer(1024)
	done := make(chan struct{})

	go func() {
		defer close(done)
		chunk := make([]byte, 64)
		for i := 0; i < 5000; i++ {
			rb.WriteAligned(chunk, 4)
		}
	}()

	for i := 0; i < 500; i++ {
		rb.Reset()
		if rb.Len()%4 != 0 {
			t.Fatalf("unaligned Len %d after concurrent Reset", rb.Len())
		}
	}
	<-done
	rb.Reset()
	if rb.Len() != 0 {
		t.Fatalf("expected Len 0 after final Reset, got %d", rb.Len())
	}
}
