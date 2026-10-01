package audio

import (
	"encoding/binary"
	"testing"
)

func Sample16(v int16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, uint16(v))
	return b
}

func SampleAsInt16(b []byte) int16 {
	return int16(binary.LittleEndian.Uint16(b))
}

func TestScaleS16HalfVolume(t *testing.T) {
	in := append(Sample16(10000), Sample16(-20000)...)
	scaleS16(in, 0.5)

	got1 := SampleAsInt16(in[:2])
	got2 := SampleAsInt16(in[2:4])
	if got1 != 5000 {
		t.Errorf("first sample = %d, want 5000", got1)
	}
	if got2 != -10000 {
		t.Errorf("second sample = %d, want -10000", got2)
	}
}

func TestScaleS16FullVolume(t *testing.T) {
	in := append(Sample16(12345), Sample16(-4321)...)
	orig := append([]byte(nil), in...)
	scaleS16(in, 1.0)
	for i := range in {
		if in[i] != orig[i] {
			t.Fatalf("volume 1.0 should not change samples, byte %d differs", i)
		}
	}
}

func TestScaleS16Clamp(t *testing.T) {
	// Volume > 1 should clamp at int16 max/min rather than overflow.
	in := append(Sample16(30000), Sample16(-30000)...)
	scaleS16(in, 3.0)
	if got := SampleAsInt16(in[:2]); got != 32767 {
		t.Errorf("clamp high = %d, want 32767", got)
	}
	if got := SampleAsInt16(in[2:4]); got != -32768 {
		t.Errorf("clamp low = %d, want -32768", got)
	}
}

func TestScaleS16Mute(t *testing.T) {
	in := append(Sample16(12345), Sample16(-6789)...)
	scaleS16(in, 0)
	if SampleAsInt16(in[:2]) != 0 || SampleAsInt16(in[2:4]) != 0 {
		t.Error("volume 0 should zero all samples")
	}
}

func TestPlayerWriteFrameAlignment(t *testing.T) {
	// 48kHz, 2 channels (stereo), 16-bit PCM = 4 bytes per frame.
	cfg := Config{
		SampleRate:    48000,
		Channels:      2,
		BitsPerSample: 16,
		BufferMs:      50, // min buffer ms = 50 -> 48000 * 4 * 0.05 = 9600 bytes
	}
	p := &Player{
		config: cfg,
		volume: 1.0,
		ring:   newByteRingBuffer(16384),
	}

	frameBytes := cfg.Channels * (cfg.BitsPerSample / 8)
	// Write distinct identifiable stereo frames: L=1000, R=2000
	makeStereoFrame := func(l, r int16) []byte {
		b := make([]byte, 4)
		binary.LittleEndian.PutUint16(b[0:2], uint16(l))
		binary.LittleEndian.PutUint16(b[2:4], uint16(r))
		return b
	}

	// Write more than maxPendingBytes so it triggers surplus drop
	frame := makeStereoFrame(1000, -2000)
	buf := make([]byte, 0, 12000)
	for i := 0; i < 3000; i++ {
		buf = append(buf, frame...)
	}

	if err := p.Write(buf); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Check that buffered bytes count is an exact multiple of frameBytes
	if p.ring.Len()%frameBytes != 0 {
		t.Errorf("buffered bytes %d is not a multiple of frame size %d", p.ring.Len(), frameBytes)
	}

	// Read out a frame and verify that L is 1000 and R is -2000 (not byte shifted or channel swapped)
	out := make([]byte, 4)
	p.onData(out, nil, 1)

	gotL := SampleAsInt16(out[0:2])
	gotR := SampleAsInt16(out[2:4])
	if gotL != 1000 || gotR != -2000 {
		t.Fatalf("frame corrupted after drop: got L=%d, R=%d, want L=1000, R=-2000", gotL, gotR)
	}
}

