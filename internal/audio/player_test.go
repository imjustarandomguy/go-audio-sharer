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
