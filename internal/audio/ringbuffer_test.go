package audio

import (
	"bytes"
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

func TestByteRingBufferOverflowDiscardsOldest(t *testing.T) {
	rb := newByteRingBuffer(6)
	rb.Write([]byte("ABC"))
	rb.Write([]byte("DEFGH")) // Total 8 > 6, should drop oldest 2 bytes ("AB"), leaving "CDEFGH"

	if rb.Len() != 6 {
		t.Fatalf("expected Len 6, got %d", rb.Len())
	}
	out := make([]byte, 6)
	rb.Read(out)
	if string(out) != "CDEFGH" {
		t.Fatalf("got %q, want 'CDEFGH'", out)
	}
}

func TestByteRingBufferDiscard(t *testing.T) {
	rb := newByteRingBuffer(10)
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
	rb := newByteRingBuffer(10)
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
