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

func makeSineChunk(startSample, frames, sampleRate int, freqHz, amp float64) []byte {
	buf := make([]byte, frames*4)
	for i := 0; i < frames; i++ {
		t := float64(startSample+i) / float64(sampleRate)
		l := int16(math.Round(amp * math.Sin(2*math.Pi*freqHz*t)))
		r := int16(math.Round(amp * math.Cos(2*math.Pi*freqHz*t)))
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
	p.pb.scratch = make([]byte, 64*1024)
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

	// Write steady 10 ms chunks well past maxPendingBytes so onData triggers overflow trim.
	for p.ring.Len() <= p.maxPendingBytes()+4000 {
		if err := p.Write(makeStereoChunk(480, 1000, -2000)); err != nil {
			t.Fatalf("Write: %v", err)
		}
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

	// Write only 20 ms (below 20 ms cushion + 10 ms period + 1 lookahead frame).
	_ = p.Write(makeStereoChunk(480, 5000, -5000))
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

	// Write two more 10 ms chunks -> meets prefill threshold -> playback starts.
	_ = p.Write(makeStereoChunk(480, 5000, -5000))
	_ = p.Write(makeStereoChunk(480, 5000, -5000))
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

	// Drain remaining buffer without writing more to force a mid-callback underrun
	// and verify the tail fades down smoothly into silence.
	var sawUnderrunOut []byte
	for i := 0; i < 6; i++ {
		wasPlaying := p.pb.playing
		p.onData(out, nil, 480)
		if wasPlaying && !p.pb.playing {
			sawUnderrunOut = append([]byte(nil), out...)
			break
		}
	}
	if p.pb.playing {
		t.Fatal("expected player to re-enter pre-buffering state after underrun")
	}
	if len(sawUnderrunOut) > 0 {
		// Tail must end at 0 (silence after fade-out).
		tailL := SampleAsInt16(sawUnderrunOut[len(sawUnderrunOut)-4 : len(sawUnderrunOut)-2])
		if tailL != 0 {
			t.Errorf("expected underrun callback to end at silence, got %d", tailL)
		}
	}
}

func TestPlayer5msPresetWith10msServerNoUnderrunLoop(t *testing.T) {
	// Client configured at 5 ms latency (240-frame callbacks) while server sends
	// default 10 ms (480-frame) chunks.
	cfg := Config{
		SampleRate:    48000,
		Channels:      2,
		BitsPerSample: 16,
		BufferMs:      5,
	}
	p := newTestPlayer(cfg)
	out := make([]byte, 240*4) // 5 ms playback callback

	started := false
	for tick := 0; tick < 1000; tick++ {
		if tick%2 == 0 {
			_ = p.Write(makeStereoChunk(480, 4000, -4000))
		}
		p.onData(out, nil, 240)
		if p.pb.playing {
			started = true
		} else if started {
			t.Fatalf("unexpected underrun at tick %d (5 ms preset with 10 ms server)", tick)
		}
	}
	if !started {
		t.Fatal("player never left pre-buffering state")
	}
	if st := p.TakeStats(); st.Underruns != 0 {
		t.Fatalf("expected 0 underruns, got %d", st.Underruns)
	}
}

func TestPlayerRefillScratchPreservesRemainderOnGrow(t *testing.T) {
	cfg := Config{
		SampleRate:    48000,
		Channels:      2,
		BitsPerSample: 16,
		BufferMs:      10,
	}
	p := newTestPlayer(cfg)
	// Force a tiny scratch buffer with an active unread remainder at scratchOff > 0.
	p.pb.scratch = make([]byte, 16)
	copy(p.pb.scratch[8:12], makeStereoChunk(1, 1234, -5678))
	p.pb.scratchOff = 8
	p.pb.scratchLen = 12

	// Put one more frame in the ring and ask refillScratch to grow beyond 16 bytes.
	_ = p.ring.Write(makeStereoChunk(10, 9000, -9000))
	p.refillScratch(40, 4)

	if len(p.pb.scratch) < 40 {
		t.Fatalf("scratch len = %d, want >= 40", len(p.pb.scratch))
	}
	if p.pb.scratchOff != 0 {
		t.Fatalf("scratchOff = %d, want 0", p.pb.scratchOff)
	}
	// First frame must be the preserved remainder (1234, -5678), NOT zeros.
	gotL := SampleAsInt16(p.pb.scratch[0:2])
	gotR := SampleAsInt16(p.pb.scratch[2:4])
	if gotL != 1234 || gotR != -5678 {
		t.Fatalf("remainder corrupted on scratch grow: got (%d, %d), want (1234, -5678)", gotL, gotR)
	}
	// Second frame must come from the ring (9000, -9000).
	gotL2 := SampleAsInt16(p.pb.scratch[4:6])
	gotR2 := SampleAsInt16(p.pb.scratch[6:8])
	if gotL2 != 9000 || gotR2 != -9000 {
		t.Fatalf("ring frame after grow: got (%d, %d), want (9000, -9000)", gotL2, gotR2)
	}
}

func TestPlayerOverflowTrimCrossfadeNoZeroStep(t *testing.T) {
	cfg := Config{
		SampleRate:    48000,
		Channels:      2,
		BitsPerSample: 16,
		BufferMs:      10,
	}
	p := newTestPlayer(cfg)
	out := make([]byte, 480*4)

	// Start playing constant +20000 amplitude so lastOutFrame is +20000.
	for i := 0; i < 3; i++ {
		_ = p.Write(makeStereoChunk(480, 20000, 20000))
	}
	p.onData(out, nil, 480)
	if !p.pb.playing || !p.pb.hasLastOut {
		t.Fatal("expected player to be playing with lastOutFrame recorded")
	}

	// Trigger overflow trim with same +20000 signal: crossfading from +20000
	// into +20000 must stay near +20000 (not dip to ~0!).
	for p.ring.Len() <= p.maxPendingBytes()+4000 {
		_ = p.Write(makeStereoChunk(480, 20000, 20000))
	}
	p.onData(out, nil, 480)
	firstL := SampleAsInt16(out[0:2])
	if firstL < 19000 {
		t.Fatalf("expected smooth crossfade from lastOutFrame (~20000), got step dip to %d", firstL)
	}
}

func TestPlayerMultiCallbackCrossfadeSmoothness(t *testing.T) {
	// Callback smaller than fadeInLen (48 frames = 1 ms vs 96 frames = 2 ms @ 48 kHz).
	// Verifies fadeStartFrame is preserved across callback boundaries without mid-fade clicks.
	cfg := Config{
		SampleRate:    48000,
		Channels:      2,
		BitsPerSample: 16,
		BufferMs:      10,
	}
	p := newTestPlayer(cfg)
	out := make([]byte, 48*4) // 1 ms callback

	// Pre-fill and run 3 callbacks (> 2 ms) of +20000 so lastOutFrame == +20000.
	for i := 0; i < 4; i++ {
		_ = p.Write(makeStereoChunk(480, 20000, 20000))
	}
	for i := 0; i < 3; i++ {
		p.onData(out, nil, 48)
	}
	if got := SampleAsInt16(out[len(out)-4 : len(out)-2]); got != 20000 {
		t.Fatalf("pre-trim last sample = %d, want 20000", got)
	}

	// Overflow the buffer with -20000 so the next callback triggers a trim crossfade
	// spanning 2 callbacks (96 frames total = 48 + 48).
	for p.ring.Len() <= p.maxPendingBytes()+20000 {
		_ = p.Write(makeStereoChunk(480, -20000, -20000))
	}

	prev := int16(20000)
	for cb := 0; cb < 2; cb++ {
		p.onData(out, nil, 48)
		for i := 0; i < 48; i++ {
			s := SampleAsInt16(out[i*4 : i*4+2])
			// Expected linear step from +20000 to -20000 over 96 frames is ~417/frame.
			diff := math.Abs(float64(s) - float64(prev))
			if diff > 600 {
				t.Fatalf("crossfade discontinuity at callback %d frame %d: prev=%d cur=%d (diff=%.0f)", cb, i, prev, s, diff)
			}
			prev = s
		}
	}
	if prev != -20000 {
		t.Fatalf("crossfade did not reach target -20000 after 2 callbacks, got %d", prev)
	}
}

func TestPlayerLargeServerChunksNoTrimLoop(t *testing.T) {
	for _, chunkMs := range []int{35, 60} {
		cfg := Config{
			SampleRate:    48000,
			Channels:      2,
			BitsPerSample: 16,
			BufferMs:      10,
		}
		p := newTestPlayer(cfg)
		out := make([]byte, 480*4) // 10 ms client callback
		chunkFrames := (48000 * chunkMs) / 1000

		started := false
		for ms := 0; ms < 2000; ms += 10 {
			if ms%chunkMs < 10 {
				_ = p.Write(makeStereoChunk(chunkFrames, 3000, -3000))
			}
			p.onData(out, nil, 480)
			if p.pb.playing {
				started = true
			} else if started {
				t.Fatalf("chunkMs=%d: unexpected underrun at t=%dms", chunkMs, ms)
			}
		}
		if !started {
			t.Fatalf("chunkMs=%d: player never left pre-buffering", chunkMs)
		}
		st := p.TakeStats()
		if st.OverflowTrims != 0 || st.Underruns != 0 {
			t.Fatalf("chunkMs=%d: expected 0 trims and 0 underruns, got trims=%d underruns=%d",
				chunkMs, st.OverflowTrims, st.Underruns)
		}
	}
}

func TestPlayerUDPChunkedPipeWireBurstNoUnderrun(t *testing.T) {
	// Simulates PipeWire's 1024-frame (21.33 ms = 4096 B) capture quantum sliced
	// into single-MTU UDP packets (4x960 B + 1x256 B) arriving back-to-back while
	// the client runs at BufferMs = 10 ms (480-frame callbacks).
	cfg := Config{
		SampleRate:    48000,
		Channels:      2,
		BitsPerSample: 16,
		BufferMs:      10,
	}
	p := newTestPlayer(cfg)
	out := make([]byte, 480*4) // 10 ms client callback

	// Pre-buffer 40 ms so initial playback starts, then run 3 seconds of
	// 21.33 ms PipeWire bursts delivered as 4x960 B + 1x256 B UDP datagrams.
	writePipeWireBurst := func() {
		for i := 0; i < 4; i++ {
			_ = p.Write(makeStereoChunk(240, 2000, -2000)) // 960 B
		}
		_ = p.Write(makeStereoChunk(64, 2000, -2000)) // 256 B trailing fragment
	}

	writePipeWireBurst()
	writePipeWireBurst()
	p.onData(out, nil, 480)
	if !p.pb.playing {
		t.Fatal("expected player to start playing after initial bursts")
	}

	var sampleBudget float64
	for tick := 0; tick < 300; tick++ {
		sampleBudget += 480.0
		for sampleBudget >= 1024.0 {
			sampleBudget -= 1024.0
			writePipeWireBurst()
		}
		p.onData(out, nil, 480)
		if !p.pb.playing {
			t.Fatalf("underrun at tick %d under UDP-chunked 1024-frame PipeWire bursts", tick)
		}
	}
	if st := p.TakeStats(); st.Underruns != 0 || st.OverflowTrims != 0 {
		t.Fatalf("expected 0 underruns and 0 trims, got %+v", st)
	}
}

func TestPlayerLargeLatencyDoesNotDeadlock(t *testing.T) {
	cfg := Config{
		SampleRate:    48000,
		Channels:      2,
		BitsPerSample: 16,
		BufferMs:      5000, // exceeds 512 KB ring if unclamped
	}
	p := newTestPlayer(cfg)
	out := make([]byte, 480*4)

	// Fill ring via steady writes and confirm playback starts instead of deadlocking.
	for i := 0; i < 300; i++ {
		_ = p.Write(makeStereoChunk(480, 3000, -3000))
		p.onData(out, nil, 480)
		if p.pb.playing {
			return
		}
	}
	t.Fatal("player deadlocked in pre-buffering state when BufferMs exceeded ring capacity")
}

func TestPlayerResamplerSineContinuityAndVolume(t *testing.T) {
	cfg := Config{
		SampleRate:    48000,
		Channels:      2,
		BitsPerSample: 16,
		BufferMs:      20,
	}
	p := newTestPlayer(cfg)
	p.SetVolume(0.5)
	out := make([]byte, 480*4)

	const freq = 440.0
	const amp = 20000.0
	sampleIdx := 0

	// Pre-fill with 40 ms of sine wave.
	for i := 0; i < 4; i++ {
		_ = p.Write(makeSineChunk(sampleIdx, 480, 48000, freq, amp))
		sampleIdx += 480
	}
	p.onData(out, nil, 480) // ramp fade-in callback

	// Stream 20 callbacks with +150 ppm drift and verify continuity + volume scaling.
	// Max adjacent sample delta for 440 Hz @ 48 kHz with amp=10000 is
	// 10000 * 2*pi*440/48000 ≈ 576; allow generous margin (1200) to catch clicks/wraps.
	var prevL, prevR int16
	hasPrev := false
	maxPeak := int16(0)

	var prodAcc float64
	for tick := 0; tick < 20; tick++ {
		prodAcc += 480.0 * 1.00015
		n := int(prodAcc)
		prodAcc -= float64(n)
		_ = p.Write(makeSineChunk(sampleIdx, n, 48000, freq, amp))
		sampleIdx += n

		p.onData(out, nil, 480)
		for i := 0; i < 480; i++ {
			l := SampleAsInt16(out[i*4 : i*4+2])
			r := SampleAsInt16(out[i*4+2 : i*4+4])
			if l > maxPeak {
				maxPeak = l
			}
			if hasPrev {
				dl := math.Abs(float64(l) - float64(prevL))
				dr := math.Abs(float64(r) - float64(prevR))
				if dl > 1200 || dr > 1200 {
					t.Fatalf("waveform discontinuity at tick %d frame %d: dL=%.0f dR=%.0f", tick, i, dl, dr)
				}
			}
			prevL, prevR = l, r
			hasPrev = true
		}
	}
	// Volume 0.5 on amp 20000 should peak around 10000 (+/- 200).
	if maxPeak < 9500 || maxPeak > 10500 {
		t.Fatalf("expected scaled peak ~10000 at volume 0.5, got %d", maxPeak)
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

			// Pre-fill initial cushion + 1 period + margin.
			for i := 0; i < 4; i++ {
				_ = p.Write(makeStereoChunk(480, 1000, -1000))
			}
			p.onData(out, nil, 480)
			if !p.pb.playing {
				t.Fatal("expected playing after initial prefill")
			}

			// Simulate 60 seconds (6000 callbacks of 10 ms) with sustained clock skew.
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
	for i := 0; i < 4; i++ {
		_ = p.Write(in)
	}
	p.onData(out, nil, 480)

	allocs := testing.AllocsPerRun(200, func() {
		_ = p.Write(in)
		p.onData(out, nil, 480)
	})
	if allocs != 0 {
		t.Fatalf("expected 0 allocs/run in steady-state Write+onData, got %.2f", allocs)
	}
}

func TestPlayerStatsTracking(t *testing.T) {
	cfg := Config{
		SampleRate:    48000,
		Channels:      2,
		BitsPerSample: 16,
		BufferMs:      10,
	}
	p := newTestPlayer(cfg)
	out := make([]byte, 480*4)

	// Trigger an overflow trim.
	for p.ring.Len() <= p.maxPendingBytes()+4000 {
		_ = p.Write(makeStereoChunk(480, 100, -100))
	}
	p.onData(out, nil, 480)

	// Drain until underrun.
	for i := 0; i < 6; i++ {
		p.onData(out, nil, 480)
	}

	// Overflow the entire ring capacity to exercise RingDrops.
	p.ring.Reset()
	big := makeStereoChunk(p.ring.Cap()/4+100, 100, -100)
	_ = p.Write(big)

	st := p.TakeStats()
	if st.OverflowTrims != 1 {
		t.Errorf("OverflowTrims = %d, want 1", st.OverflowTrims)
	}
	if st.Underruns == 0 {
		t.Errorf("expected Underruns > 0, got 0")
	}
	if st.RingDrops == 0 {
		t.Errorf("expected RingDrops > 0, got 0")
	}

	// Second TakeStats must return zeroed counters.
	st2 := p.TakeStats()
	if st2.OverflowTrims != 0 || st2.Underruns != 0 || st2.RingDrops != 0 {
		t.Errorf("expected stats to reset after TakeStats, got %+v", st2)
	}
}
