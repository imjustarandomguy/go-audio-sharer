package audio

import (
	"encoding/binary"
	"math"
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

func makeStereoChunk(frames int, l, r int16) []byte {
	buf := make([]byte, frames*4)
	for i := 0; i < frames; i++ {
		binary.LittleEndian.PutUint16(buf[i*4:i*4+2], uint16(l))
		binary.LittleEndian.PutUint16(buf[i*4+2:i*4+4], uint16(r))
	}
	return buf
}

func newTestPlayer(cfg Config) *Player {
	p := &Player{
		config: cfg,
		ring:   newByteRingBuffer(512 * 1024),
	}
	p.SetVolume(1.0)
	p.pb.scratch = make([]byte, 16*1024)
	return p
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
		BufferMs:      20,
	}
	p := newTestPlayer(cfg)
	frameBytes := cfg.Channels * (cfg.BitsPerSample / 8)

	// Write well over maxPendingBytes so onData triggers overflow trim.
	overflowFrames := (p.maxPendingBytes() / frameBytes) + 1000
	if err := p.Write(makeStereoChunk(overflowFrames, 1000, -2000)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Read 480 frames (10 ms), which is longer than the 2 ms fade-in ramp.
	out := make([]byte, 480*frameBytes)
	p.onData(out, nil, 480)

	if p.ring.Len()%frameBytes != 0 {
		t.Errorf("buffered bytes %d is not a multiple of frame size %d", p.ring.Len(), frameBytes)
	}
	if p.ring.Len() > p.maxPendingBytes() {
		t.Errorf("buffered bytes %d exceeds maxPendingBytes %d after trim", p.ring.Len(), p.maxPendingBytes())
	}

	// Verify the last frame (past the 2 ms fade-in) has exact unshifted L/R channels.
	last := out[len(out)-frameBytes:]
	gotL := SampleAsInt16(last[0:2])
	gotR := SampleAsInt16(last[2:4])
	if gotL != 1000 || gotR != -2000 {
		t.Fatalf("frame corrupted after drop: got L=%d, R=%d, want L=1000, R=-2000", gotL, gotR)
	}
}

func TestPlayerPrefillAndUnderrunRecovery(t *testing.T) {
	cfg := Config{
		SampleRate:    48000,
		Channels:      2,
		BitsPerSample: 16,
		BufferMs:      20, // target cushion = 960 frames (20 ms)
	}
	p := newTestPlayer(cfg)
	out := make([]byte, 480*4) // 10 ms callback

	// Write only 10 ms (below 20 ms target + 10 ms callback = 30 ms prefill).
	_ = p.Write(makeStereoChunk(480, 5000, -5000))
	p.onData(out, nil, 480)
	for i, b := range out {
		if b != 0 {
			t.Fatalf("expected silence while pre-buffering, non-zero byte at %d", i)
		}
	}
	if p.pb.playing {
		t.Fatal("expected player to remain in pre-buffering state")
	}

	// Write another 20 ms -> total 30 ms meets prefill threshold -> playback starts.
	_ = p.Write(makeStereoChunk(960, 5000, -5000))
	p.onData(out, nil, 480)
	if !p.pb.playing {
		t.Fatal("expected player to transition to playing once prefill target is met")
	}
	// First sample should be ramped (fade-in), last sample at full amplitude.
	firstL := SampleAsInt16(out[0:2])
	lastL := SampleAsInt16(out[len(out)-4 : len(out)-2])
	if firstL <= 0 || firstL >= 5000 {
		t.Errorf("expected gradual fade-in on first sample, got %d", firstL)
	}
	if lastL != 5000 {
		t.Errorf("expected full amplitude after fade-in, got %d", lastL)
	}

	// Drain remaining buffer without writing more to force an underrun.
	for i := 0; i < 4; i++ {
		p.onData(out, nil, 480)
	}
	if p.pb.playing {
		t.Fatal("expected player to re-enter pre-buffering state after underrun")
	}
}

func TestPlayerAdaptiveClockDrift(t *testing.T) {
	for _, tc := range []struct {
		name     string
		driftPPM float64
	}{
		{"server_faster_200ppm", +200},
		{"server_slower_200ppm", -200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				SampleRate:    48000,
				Channels:      2,
				BitsPerSample: 16,
				BufferMs:      20,
			}
			p := newTestPlayer(cfg)
			out := make([]byte, 480*4) // 10 ms playback period

			// Pre-fill initial cushion + 1 period.
			_ = p.Write(makeStereoChunk(480*3, 1000, -1000))
			p.onData(out, nil, 480)
			if !p.pb.playing {
				t.Fatal("expected playing after initial prefill")
			}

			// Simulate 20 seconds (2000 callbacks of 10 ms) with sustained clock skew.
			// Without adaptive resampling, 200 ppm over 20s = 4 ms drift, and over
			// 100s = 20 ms drift -> guaranteed underrun or overflow. Simulate 60s
			// (12 ms raw drift, enough to verify PI lock-in and stability).
			var prodAcc float64
			framesPerTick := 480.0 * (1.0 + tc.driftPPM*1e-6)
			for tick := 0; tick < 6000; tick++ {
				prodAcc += framesPerTick
				n := int(prodAcc)
				prodAcc -= float64(n)
				_ = p.Write(makeStereoChunk(n, 1000, -1000))

				p.onData(out, nil, 480)
				if !p.pb.playing {
					t.Fatalf("underrun occurred at tick %d under %+.0f ppm drift", tick, tc.driftPPM)
				}
			}

			// Latency should stay tightly locked near BufferMs (20 ms +/- 3 ms).
			lat := p.LatencyMs()
			if math.Abs(lat-20.0) > 3.0 {
				t.Fatalf("latency drifted to %.2f ms (want ~20 ms +/- 3 ms)", lat)
			}
		})
	}
}

func TestPlayerOnDataZeroAllocs(t *testing.T) {
	cfg := Config{
		SampleRate:    48000,
		Channels:      2,
		BitsPerSample: 16,
		BufferMs:      20,
	}
	p := newTestPlayer(cfg)
	in := makeStereoChunk(480, 1234, -1234)
	out := make([]byte, 480*4)

	// Warm up into steady playing state.
	_ = p.Write(makeStereoChunk(480*3, 1234, -1234))
	p.onData(out, nil, 480)

	allocs := testing.AllocsPerRun(200, func() {
		_ = p.Write(in)
		p.onData(out, nil, 480)
	})
	if allocs != 0 {
		t.Fatalf("expected 0 allocs/run in steady-state Write+onData, got %.2f", allocs)
	}
}
