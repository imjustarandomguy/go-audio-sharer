package audio

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gen2brain/malgo"
)

// PlayerStats reports glitch telemetry collected since the previous TakeStats call.
type PlayerStats struct {
	Underruns     uint64
	OverflowTrims uint64
	RingDrops     uint64
	DeviceResets  uint64
	MaxWriteGap   time.Duration
}

const (
	maxResampleChannels = 8

	// Adaptive clock-drift PI controller constants.
	// Smoothed over ~0.5s so short network jitter doesn't cause pitch wobble,
	// while hardware crystal drift (typically +/-20..100 ppm) is tracked to zero
	// steady-state error well within the inaudible +/-0.5% cap.
	driftEMAAlpha    = 0.02
	driftKp          = 0.25
	driftKi          = 0.02
	driftMaxIntegral = 0.002 // +/-2000 ppm
	driftMaxRatio    = 0.005 // +/-0.5% (< 9 cents, inaudible)
)

// playbackState holds consumer-only state accessed exclusively from the
// miniaudio real-time audio callback (or after device.Stop()).
type playbackState struct {
	playing        bool
	hasCurr        bool
	hasLastOut     bool
	prevFrame      [maxResampleChannels]int16
	currFrame      [maxResampleChannels]int16
	lastOutFrame   [maxResampleChannels]int16
	fadeStartFrame [maxResampleChannels]int16
	phase          float64
	fillEMA        float64
	chunkEMA       float64
	integral       float64
	fadeInRem      int
	fadeInLen      int
	fadeFromLast   bool
	scratch        []byte
	scratchOff     int
	scratchLen     int
}

func (s *playbackState) reset() {
	s.playing = false
	s.hasCurr = false
	s.hasLastOut = false
	s.phase = 0
	s.fillEMA = 0
	s.chunkEMA = 0
	s.integral = 0
	s.fadeInRem = 0
	s.fadeInLen = 0
	s.fadeFromLast = false
	s.scratchOff = 0
	s.scratchLen = 0
}

// Player streams raw PCM to an audio output device via miniaudio (malgo).
// It supports selecting a specific output device and automatically follows the
// system default output device when none is explicitly selected.
type Player struct {
	ctx     *lockedAudioContext
	device  *malgo.Device
	monitor *DeviceMonitor

	opMu           sync.Mutex
	mu             sync.Mutex
	ring           *byteRingBuffer
	volumeBits     atomic.Uint64
	lastWriteBytes atomic.Int64
	started        bool
	config         Config
	devID          malgo.DeviceID
	devName        string

	// Consumer-only state (only touched in onData or after device.Stop()).
	pb playbackState

	// Atomic glitch telemetry counters (safe from audio callback and network goroutine).
	underruns     atomic.Uint64
	overflowTrims atomic.Uint64
	ringDrops     atomic.Uint64
	deviceResets  atomic.Uint64
	lastWriteNs   atomic.Int64
	maxWriteGapNs atomic.Int64
}

var monoEpoch = time.Now()

// monoNowNs returns a strictly positive monotonic nanosecond timestamp immune
// to wall-clock / NTP adjustments, with zero heap allocations.
func monoNowNs() int64 {
	return int64(time.Since(monoEpoch)) + 1
}

func recordMaxInt64(dst *atomic.Int64, val int64) {
	for {
		cur := dst.Load()
		if val <= cur {
			return
		}
		if dst.CompareAndSwap(cur, val) {
			return
		}
	}
}

// TakeStats returns and resets glitch telemetry collected since the last call.
func (p *Player) TakeStats() PlayerStats {
	return PlayerStats{
		Underruns:     p.underruns.Swap(0),
		OverflowTrims: p.overflowTrims.Swap(0),
		RingDrops:     p.ringDrops.Swap(0),
		DeviceResets:  p.deviceResets.Swap(0),
		MaxWriteGap:   time.Duration(p.maxWriteGapNs.Swap(0)),
	}
}

func NewPlayer(cfg Config) (*Player, error) {
	ctx, err := newLockedAudioContext(nil)
	if err != nil {
		return nil, err
	}
	p := &Player{
		ctx:    ctx,
		config: cfg,
	}
	ringCap := 512 * 1024 // 512 KB (~2.7s headroom at 48kHz stereo)
	if need := p.maxPendingBytes() * 4; need > ringCap {
		ringCap = need
	}
	p.ring = newByteRingBuffer(ringCap)
	p.volumeBits.Store(math.Float64bits(1.0))
	p.pb.scratch = make([]byte, 64*1024)
	p.monitor = newDeviceMonitor(cfg.Device, func(info *malgo.DeviceInfo) bool {
		return p.followDefault(info)
	})
	return p, nil
}

func (p *Player) SetVolume(v float64) {
	if v < 0 {
		v = 0
	}
	if v > 1 {
		v = 1
	}
	p.volumeBits.Store(math.Float64bits(v))
}

// openAndStartDevice initializes and starts the playback device on the pinned
// audio context thread. Caller must not hold p.mu around this call.
func (p *Player) openAndStartDevice() (*malgo.Device, malgo.DeviceID, error) {
	var (
		dev   *malgo.Device
		devID malgo.DeviceID
	)
	err := p.ctx.do(func(ctx malgo.Context) error {
		info, err := playbackDevice(ctx, p.config.Device)
		if err != nil {
			return err
		}

		p.mu.Lock()
		p.devID = info.ID
		p.devName = info.Name()
		p.mu.Unlock()
		log.Printf("Playback device: %s", info.Name())

		// Keep the hardware callback period crisp (5-10 ms) even when the user
		// selects a larger jitter buffer target (e.g. 30-100 ms), so the extra
		// latency goes into the jitter buffer rather than sluggish hardware periods.
		periodMs := p.config.BufferMs
		if periodMs <= 0 {
			periodMs = 10
		}
		if periodMs > 10 {
			periodMs = 10
		}

		dc := malgo.DefaultDeviceConfig(malgo.Playback)
		dc.Playback.Format = malgo.FormatS16
		dc.Playback.Channels = uint32(p.config.Channels)
		dc.SampleRate = uint32(p.config.SampleRate)
		dc.PeriodSizeInMilliseconds = uint32(periodMs)
		dc.Alsa.NoMMap = 1

		// Pin to a specific device only when one was explicitly requested; otherwise
		// use the default device (which the monitor then follows).
		if p.config.Device != "" {
			dc.Playback.DeviceID = info.ID.Pointer()
		}

		d, err := malgo.InitDevice(ctx, dc, malgo.DeviceCallbacks{Data: p.onData})
		if err != nil {
			return fmt.Errorf("could not init playback device: %w", err)
		}
		if err := d.Start(); err != nil {
			d.Uninit()
			return err
		}
		dev = d
		devID = info.ID
		return nil
	})
	return dev, devID, err
}

func (p *Player) stopDevice(dev *malgo.Device) {
	if dev == nil {
		return
	}
	_ = p.ctx.do(func(_ malgo.Context) error {
		_ = dev.Stop()
		dev.Uninit()
		return nil
	})
}

func (p *Player) Start() error {
	p.opMu.Lock()
	defer p.opMu.Unlock()

	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	p.pb.reset()

	dev, devID, err := p.openAndStartDevice()
	if err != nil {
		return err
	}
	if p.monitor != nil {
		p.monitor.setCurrent(devID.String())
	}

	p.mu.Lock()
	p.device = dev
	p.started = true
	p.mu.Unlock()
	return nil
}

func (p *Player) frameBytes() int {
	fb := p.config.Channels * (p.config.BitsPerSample / 8)
	if fb <= 0 {
		return 2
	}
	return fb
}

func (p *Player) Write(data []byte) error {
	now := monoNowNs()
	if prev := p.lastWriteNs.Swap(now); prev > 0 && now > prev {
		recordMaxInt64(&p.maxWriteGapNs, now-prev)
	}
	fb := p.frameBytes()
	aligned := (len(data) / fb) * fb
	if aligned > 0 {
		// Track observed incoming chunk granularity (capped at 100 ms so large
		// stall bursts don't inflate steady-state latency) to avoid underrun
		// loops when server chunks are larger than the client's callback period.
		chunkCap := p.bytesForMs(100)
		tracked := aligned
		if chunkCap > 0 && tracked > chunkCap {
			tracked = chunkCap
		}
		p.lastWriteBytes.Store(int64(tracked))
	}
	written := p.ring.WriteAligned(data, fb)
	if written < aligned {
		p.ringDrops.Add(1)
	}
	return nil
}

func (p *Player) bytesForMs(ms int) int {
	frameBytes := int64(p.frameBytes())
	bytesPerSec := int64(p.config.SampleRate) * frameBytes
	if bytesPerSec <= 0 || ms <= 0 {
		return 0
	}
	raw := (bytesPerSec * int64(ms)) / 1000
	return int((raw / frameBytes) * frameBytes)
}

// targetPendingBytes returns the desired jitter-buffer cushion (in bytes)
// maintained after each audio callback drains a period.
func (p *Player) targetPendingBytes() int {
	ms := p.config.BufferMs
	if ms <= 0 {
		ms = 10
	}
	return p.bytesForMs(ms)
}

// maxPendingBytes is the upper bound for buffered PCM before hard trimming.
// It provides generous headroom above the target cushion so normal jitter or
// PipeWire quantum fluctuations don't trigger drops, while still capping
// runaway latency after a network stall.
func (p *Player) maxPendingBytes() int {
	frameBytes := p.frameBytes()
	bytesPerSec := p.config.SampleRate * frameBytes
	if bytesPerSec <= 0 {
		return 1 << 16
	}
	targetMs := p.config.BufferMs
	if targetMs <= 0 {
		targetMs = 10
	}
	maxMs := targetMs * 3
	if maxMs < targetMs+40 {
		maxMs = targetMs + 40
	}
	if maxMs < 50 {
		maxMs = 50
	}
	return p.bytesForMs(maxMs)
}

func (p *Player) fadeFrames() int {
	if p.config.SampleRate <= 0 {
		return 1
	}
	f := p.config.SampleRate / 500 // 2 ms
	if f < 1 {
		return 1
	}
	return f
}

func (p *Player) startFadeIn(fromLast bool) {
	f := p.fadeFrames()
	p.pb.fadeInLen = f
	p.pb.fadeInRem = f
	p.pb.fadeFromLast = fromLast && p.pb.hasLastOut
	if p.pb.fadeFromLast {
		p.pb.fadeStartFrame = p.pb.lastOutFrame
	}
}

// onData is called by miniaudio on its real-time audio thread.
// It must remain lock-free and allocation-free to avoid priority inversion
// and GC stalls against the network goroutine.
func (p *Player) onData(out, _ []byte, framecount uint32) {
	if len(out) == 0 {
		return
	}
	frameBytes := p.frameBytes()
	targetCushion := p.targetPendingBytes()

	// Smooth observed incoming chunk granularity with the same slow EMA as
	// fillEMA so transient PipeWire/network chunk fluctuations don't step
	// targetFrames and cause pitch wobble, while steady-state larger server
	// chunks are absorbed without underrun or trim loops.
	if obs := int(p.lastWriteBytes.Load()); obs > 0 {
		if p.pb.chunkEMA == 0 {
			p.pb.chunkEMA = float64(obs)
		} else {
			p.pb.chunkEMA += driftEMAAlpha * (float64(obs) - p.pb.chunkEMA)
		}
	}
	periodBytes := len(out)
	if smoothed := (int(math.Round(p.pb.chunkEMA)) / frameBytes) * frameBytes; smoothed > periodBytes {
		periodBytes = smoothed
	}
	targetBefore := targetCushion + periodBytes + frameBytes

	// Never let targetBefore exceed ring capacity (prevents permanent silence deadlock).
	maxSafeTarget := (((p.ring.Cap() / 4) * 3) / frameBytes) * frameBytes
	if maxSafeTarget < len(out)+frameBytes {
		maxSafeTarget = ((p.ring.Cap() - frameBytes) / frameBytes) * frameBytes
	}
	if maxSafeTarget > 0 && targetBefore > maxSafeTarget {
		targetBefore = maxSafeTarget
	}

	maxBytes := p.maxPendingBytes()
	headroom := periodBytes
	if h20 := p.bytesForMs(20); h20 > headroom {
		headroom = h20
	}
	if maxBytes < targetBefore+headroom {
		maxBytes = targetBefore + headroom
	}
	maxSafeCap := ((p.ring.Cap() - frameBytes) / frameBytes) * frameBytes
	if maxSafeCap > 0 && maxBytes > maxSafeCap {
		maxBytes = maxSafeCap
	}

	avail := p.ring.Len() + (p.pb.scratchLen - p.pb.scratchOff)

	// Pre-buffer until we have our target jitter cushion before starting
	// (or resuming after an underrun).
	if !p.pb.playing {
		if avail < targetBefore {
			clear(out)
			return
		}
		p.pb.playing = true
		p.pb.hasCurr = false
		p.pb.phase = 0
		p.pb.fillEMA = float64(avail / frameBytes)
		p.startFadeIn(false)
	}

	// Hard-trim stale backlog if the buffer exceeded maxBytes (e.g. burst
	// arriving after a stall), snapping cleanly back to targetBefore.
	if avail > maxBytes {
		drop := avail - targetBefore
		if rem := drop % frameBytes; rem != 0 {
			drop += frameBytes - rem
		}
		// Discard any buffered lookahead in scratch first, then from the ring.
		scratchAvail := p.pb.scratchLen - p.pb.scratchOff
		if scratchAvail > 0 {
			if drop >= scratchAvail {
				drop -= scratchAvail
				p.pb.scratchOff = 0
				p.pb.scratchLen = 0
			} else {
				p.pb.scratchOff += drop
				drop = 0
			}
		}
		if drop > 0 {
			p.ring.Discard(drop)
		}
		p.pb.hasCurr = false
		p.pb.phase = 0
		avail = p.ring.Len() + (p.pb.scratchLen - p.pb.scratchOff)
		p.pb.fillEMA = float64(avail / frameBytes)
		// Crossfade from the previous callback's last sample instead of jumping
		// down to zero, preventing an audible discontinuity click.
		p.startFadeIn(true)
		p.overflowTrims.Add(1)
	}

	vol := math.Float64frombits(p.volumeBits.Load())
	channels := p.config.Channels
	if p.config.BitsPerSample != 16 || channels <= 0 || channels > maxResampleChannels || p.config.SampleRate <= 0 {
		n := p.ring.Read(out)
		if vol != 1.0 && p.config.BitsPerSample == 16 {
			scaleS16(out[:n], vol)
		}
		clear(out[n:])
		if n < len(out) {
			p.enterUnderrun()
		}
		return
	}

	outFrames := len(out) / frameBytes
	if outFrames == 0 {
		clear(out)
		return
	}

	// Update smoothed buffer depth and compute adaptive resampling ratio.
	availFrames := float64(avail / frameBytes)
	p.pb.fillEMA += driftEMAAlpha * (availFrames - p.pb.fillEMA)
	targetFrames := float64(targetBefore / frameBytes)
	errSec := (p.pb.fillEMA - targetFrames) / float64(p.config.SampleRate)
	dt := float64(outFrames) / float64(p.config.SampleRate)

	p.pb.integral += driftKi * errSec * dt
	if p.pb.integral > driftMaxIntegral {
		p.pb.integral = driftMaxIntegral
	} else if p.pb.integral < -driftMaxIntegral {
		p.pb.integral = -driftMaxIntegral
	}

	adj := driftKp*errSec + p.pb.integral
	if adj > driftMaxRatio {
		adj = driftMaxRatio
	} else if adj < -driftMaxRatio {
		adj = -driftMaxRatio
	}
	ratio := 1.0 + adj

	// Prefetch all frames needed for this callback in a single ring Read.
	needFrames := int(math.Floor(p.pb.phase + float64(outFrames-1)*ratio))
	if needFrames < 0 {
		needFrames = 0
	}
	if !p.pb.hasCurr {
		needFrames += 2
	}
	// Request 1 extra frame as FP rounding margin only if available.
	p.refillScratch((needFrames+1)*frameBytes, frameBytes)

	if !p.pb.hasCurr {
		if !p.nextFrame(&p.pb.prevFrame, channels, frameBytes) || !p.nextFrame(&p.pb.currFrame, channels, frameBytes) {
			clear(out)
			p.enterUnderrun()
			return
		}
		p.pb.hasCurr = true
		p.pb.phase = 0
	}

	writtenFrames := 0
	for i := 0; i < outFrames; i++ {
		for p.pb.phase >= 1.0 {
			p.pb.phase -= 1.0
			p.pb.prevFrame = p.pb.currFrame
			if !p.nextFrame(&p.pb.currFrame, channels, frameBytes) {
				// Underrun partway through the callback: fade out written tail
				// to avoid a click, zero the rest, and re-buffer cleanly.
				if writtenFrames > 0 {
					fadeOutTailS16(out[:writtenFrames*frameBytes], channels, p.fadeFrames())
				}
				clear(out[writtenFrames*frameBytes:])
				p.enterUnderrun()
				return
			}
		}

		var ramp float64
		fading := p.pb.fadeInRem > 0
		if fading {
			ramp = float64(p.pb.fadeInLen-p.pb.fadeInRem+1) / float64(p.pb.fadeInLen)
			p.pb.fadeInRem--
		}

		base := i * frameBytes
		ph := p.pb.phase
		for c := 0; c < channels; c++ {
			s0 := float64(p.pb.prevFrame[c])
			s1 := float64(p.pb.currFrame[c])
			sample := (s0 + ph*(s1-s0)) * vol
			if fading {
				if p.pb.fadeFromLast {
					sample = float64(p.pb.fadeStartFrame[c])*(1.0-ramp) + sample*ramp
				} else {
					sample *= ramp
				}
			}
			var clamped int16
			if sample >= 32767.0 {
				clamped = 32767
			} else if sample <= -32768.0 {
				clamped = -32768
			} else {
				clamped = int16(math.Round(sample))
			}
			binary.LittleEndian.PutUint16(out[base+c*2:base+c*2+2], uint16(clamped))
		}

		p.pb.phase += ratio
		writtenFrames++
	}

	if writtenFrames > 0 {
		lastBase := (writtenFrames - 1) * frameBytes
		for c := 0; c < channels; c++ {
			p.pb.lastOutFrame[c] = int16(binary.LittleEndian.Uint16(out[lastBase+c*2 : lastBase+c*2+2]))
		}
		p.pb.hasLastOut = true
	}
}

func (p *Player) enterUnderrun() {
	p.underruns.Add(1)
	p.pb.playing = false
	p.pb.hasCurr = false
	p.pb.hasLastOut = false
	p.pb.phase = 0
	p.pb.scratchOff = 0
	p.pb.scratchLen = 0
}

// refillScratch ensures p.pb.scratch holds up to wantBytes (aligned to frameBytes)
// from the ring buffer using at most one ring.Read call.
func (p *Player) refillScratch(wantBytes, frameBytes int) {
	rem := p.pb.scratchLen - p.pb.scratchOff
	if rem >= wantBytes {
		return
	}
	if len(p.pb.scratch) < wantBytes {
		newBuf := make([]byte, wantBytes)
		if rem > 0 {
			copy(newBuf, p.pb.scratch[p.pb.scratchOff:p.pb.scratchLen])
		}
		p.pb.scratch = newBuf
	} else if rem > 0 && p.pb.scratchOff > 0 {
		copy(p.pb.scratch, p.pb.scratch[p.pb.scratchOff:p.pb.scratchLen])
	}
	p.pb.scratchOff = 0
	p.pb.scratchLen = rem

	toRead := wantBytes - rem
	toRead = (toRead / frameBytes) * frameBytes
	if toRead <= 0 {
		return
	}
	n := p.ring.Read(p.pb.scratch[rem : rem+toRead])
	p.pb.scratchLen = rem + n
}

func (p *Player) nextFrame(dst *[maxResampleChannels]int16, channels, frameBytes int) bool {
	if p.pb.scratchLen-p.pb.scratchOff < frameBytes {
		p.refillScratch(frameBytes, frameBytes)
		if p.pb.scratchLen-p.pb.scratchOff < frameBytes {
			return false
		}
	}
	off := p.pb.scratchOff
	for c := 0; c < channels; c++ {
		dst[c] = int16(binary.LittleEndian.Uint16(p.pb.scratch[off+c*2 : off+c*2+2]))
	}
	p.pb.scratchOff += frameBytes
	return true
}

// fadeOutTailS16 applies a linear fade-out to the final frames of b to prevent
// an audible click when transitioning into silence on an underrun.
func fadeOutTailS16(b []byte, channels, fadeFrames int) {
	frameBytes := channels * 2
	if frameBytes <= 0 || fadeFrames <= 0 {
		return
	}
	totalFrames := len(b) / frameBytes
	if totalFrames == 0 {
		return
	}
	n := fadeFrames
	if n > totalFrames {
		n = totalFrames
	}
	startFrame := totalFrames - n
	for i := 0; i < n; i++ {
		gain := float64(n-1-i) / float64(n)
		base := (startFrame + i) * frameBytes
		for c := 0; c < channels; c++ {
			s := int16(binary.LittleEndian.Uint16(b[base+c*2 : base+c*2+2]))
			v := int16(math.Round(float64(s) * gain))
			binary.LittleEndian.PutUint16(b[base+c*2:base+c*2+2], uint16(v))
		}
	}
}

// followDefault re-creates the playback device when the default output changes.
// Returns true when playback is active (or stopped by caller), or false when
// reopening failed transiently so DeviceMonitor retries on the next poll.
func (p *Player) followDefault(info *malgo.DeviceInfo) bool {
	p.opMu.Lock()
	defer p.opMu.Unlock()

	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return true
	}
	// Keep p.started true while swapping so a transient reopen error (e.g.
	// Bluetooth/USB switch in progress) can still recover on the next poll.
	old := p.device
	p.device = nil
	p.devID = info.ID
	p.devName = info.Name()
	p.mu.Unlock()

	p.deviceResets.Add(1)

	// Stop old device before resetting the SPSC ring buffer and consumer state
	// so onData is no longer running.
	p.stopDevice(old)
	p.ring.Reset()
	p.pb.reset()

	dev, _, err := p.openAndStartDevice()
	if err != nil {
		log.Printf("Error reopening device after change: %v", err)
		return false
	}

	p.mu.Lock()
	p.device = dev
	p.mu.Unlock()
	return true
}

func (p *Player) Stop() {
	p.opMu.Lock()
	defer p.opMu.Unlock()

	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	p.started = false
	dev := p.device
	p.device = nil
	p.mu.Unlock()

	// Stop device before resetting the SPSC ring buffer and consumer state
	// so onData is no longer running.
	p.stopDevice(dev)
	p.ring.Reset()
	p.pb.reset()
	p.lastWriteNs.Store(0)
	p.lastWriteBytes.Store(0)
}

// LatencyMs returns the approximate current playback latency in milliseconds,
// derived from how much PCM is currently buffered ahead of the device.
func (p *Player) LatencyMs() float64 {
	b := p.ring.Len()
	bytesPerSec := p.config.SampleRate * p.frameBytes()
	if bytesPerSec <= 0 {
		return 0
	}
	return float64(b) * 1000.0 / float64(bytesPerSec)
}

func (p *Player) Close() {
	if p.monitor != nil {
		p.monitor.close()
	}
	p.Stop()
	if p.ctx != nil {
		p.ctx.close()
	}
}

// scaleS16 applies a linear volume to little-endian signed-16 samples in place.
func scaleS16(b []byte, vol float64) {
	for i := 0; i+2 <= len(b); i += 2 {
		s := int16(binary.LittleEndian.Uint16(b[i : i+2]))
		v := int(float64(s) * vol)
		if v > 32767 {
			v = 32767
		} else if v < -32768 {
			v = -32768
		}
		binary.LittleEndian.PutUint16(b[i:i+2], uint16(int16(v)))
	}
}
