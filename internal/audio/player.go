package audio

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"

	"github.com/gen2brain/malgo"
)

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
	playing    bool
	hasCurr    bool
	prevFrame  [maxResampleChannels]int16
	currFrame  [maxResampleChannels]int16
	phase      float64
	fillEMA    float64
	integral   float64
	fadeInRem  int
	fadeInLen  int
	scratch    []byte
	scratchOff int
	scratchLen int
}

func (s *playbackState) reset() {
	s.playing = false
	s.hasCurr = false
	s.phase = 0
	s.fillEMA = 0
	s.integral = 0
	s.fadeInRem = 0
	s.fadeInLen = 0
	s.scratchOff = 0
	s.scratchLen = 0
}

// Player streams raw PCM to an audio output device via miniaudio (malgo).
// It supports selecting a specific output device and automatically follows the
// system default output device when none is explicitly selected.
type Player struct {
	ctx     *malgo.AllocatedContext
	device  *malgo.Device
	monitor *DeviceMonitor

	opMu       sync.Mutex
	mu         sync.Mutex
	ring       *byteRingBuffer
	volumeBits atomic.Uint64
	started    bool
	config     Config
	devID      malgo.DeviceID
	devName    string

	// Consumer-only state (only touched in onData or after device.Stop()).
	pb playbackState
}

func NewPlayer(cfg Config) (*Player, error) {
	ctx, err := newAudioContext()
	if err != nil {
		return nil, err
	}
	p := &Player{
		ctx:    ctx,
		config: cfg,
		ring:   newByteRingBuffer(512 * 1024), // 512 KB (~2.7s headroom at 48kHz stereo)
	}
	p.volumeBits.Store(math.Float64bits(1.0))
	p.pb.scratch = make([]byte, 16*1024)
	p.monitor = newDeviceMonitor(ctx.Context, cfg.Device, func(info *malgo.DeviceInfo) {
		p.followDefault(info)
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

// createDevice initializes the playback device for the currently chosen
// output. Caller must not hold p.mu around device.Stop/Uninit calls.
func (p *Player) createDevice() (*malgo.Device, error) {
	info, err := playbackDevice(p.ctx.Context, p.config.Device)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.devID = info.ID
	p.devName = info.Name()
	p.mu.Unlock()
	log.Printf("Playback device: %s", info.Name())
	if p.monitor != nil {
		p.monitor.setCurrent(info.ID.String())
	}

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

	device, err := malgo.InitDevice(p.ctx.Context, dc, malgo.DeviceCallbacks{Data: p.onData})
	if err != nil {
		return nil, fmt.Errorf("could not init playback device: %w", err)
	}
	return device, nil
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

	dev, err := p.createDevice()
	if err != nil {
		return err
	}
	if err := dev.Start(); err != nil {
		dev.Uninit()
		return err
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
	p.ring.WriteAligned(data, p.frameBytes())
	return nil
}

// targetPendingBytes returns the desired jitter-buffer cushion (in bytes)
// maintained after each audio callback drains a period.
func (p *Player) targetPendingBytes() int {
	frameBytes := p.frameBytes()
	bytesPerSec := p.config.SampleRate * frameBytes
	if bytesPerSec <= 0 {
		return 0
	}
	ms := p.config.BufferMs
	if ms <= 0 {
		ms = 10
	}
	raw := (bytesPerSec * ms) / 1000
	return (raw / frameBytes) * frameBytes
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
	raw := (bytesPerSec * maxMs) / 1000
	return (raw / frameBytes) * frameBytes
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

func (p *Player) startFadeIn() {
	f := p.fadeFrames()
	p.pb.fadeInLen = f
	p.pb.fadeInRem = f
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
	// Desired ring occupancy right before draining len(out).
	targetBefore := targetCushion + len(out)
	maxBytes := p.maxPendingBytes()
	if maxBytes < targetBefore+len(out) {
		maxBytes = targetBefore + len(out)
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
		p.startFadeIn()
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
		p.startFadeIn()
	}

	vol := math.Float64frombits(p.volumeBits.Load())
	channels := p.config.Channels
	if p.config.BitsPerSample != 16 || channels <= 0 || channels > maxResampleChannels || p.config.SampleRate <= 0 {
		n := p.ring.Read(out)
		if vol != 1.0 {
			scaleS16(out[:n], vol)
		}
		clear(out[n:])
		if n < len(out) {
			p.pb.playing = false
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

		gain := vol
		if p.pb.fadeInRem > 0 {
			ramp := float64(p.pb.fadeInLen-p.pb.fadeInRem+1) / float64(p.pb.fadeInLen)
			gain *= ramp
			p.pb.fadeInRem--
		}

		base := i * frameBytes
		ph := p.pb.phase
		for c := 0; c < channels; c++ {
			s0 := float64(p.pb.prevFrame[c])
			s1 := float64(p.pb.currFrame[c])
			sample := (s0 + ph*(s1-s0)) * gain
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
}

func (p *Player) enterUnderrun() {
	p.pb.playing = false
	p.pb.hasCurr = false
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
		p.pb.scratch = make([]byte, wantBytes)
	}
	if rem > 0 && p.pb.scratchOff > 0 {
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
func (p *Player) followDefault(info *malgo.DeviceInfo) {
	p.opMu.Lock()
	defer p.opMu.Unlock()

	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	p.started = false
	old := p.device
	p.device = nil
	p.devID = info.ID
	p.devName = info.Name()
	p.mu.Unlock()

	// Stop old device before resetting the SPSC ring buffer and consumer state
	// so onData is no longer running.
	if old != nil {
		_ = old.Stop()
		old.Uninit()
	}
	p.ring.Reset()
	p.pb.reset()

	dev, err := p.createDevice()
	if err != nil {
		log.Printf("Error reopening device after change: %v", err)
		return
	}
	if err := dev.Start(); err != nil {
		log.Printf("Error starting device after change: %v", err)
		dev.Uninit()
		return
	}

	p.mu.Lock()
	p.device = dev
	p.started = true
	p.mu.Unlock()
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
	if dev != nil {
		_ = dev.Stop()
		dev.Uninit()
	}
	p.ring.Reset()
	p.pb.reset()
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
		_ = p.ctx.Uninit()
		p.ctx.Free()
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
