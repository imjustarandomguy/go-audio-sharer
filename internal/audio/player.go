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

	dc := malgo.DefaultDeviceConfig(malgo.Playback)
	dc.Playback.Format = malgo.FormatS16
	dc.Playback.Channels = uint32(p.config.Channels)
	dc.SampleRate = uint32(p.config.SampleRate)
	dc.PeriodSizeInMilliseconds = uint32(p.config.BufferMs)
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

// maxPendingBytes is the target upper bound for buffered PCM, in bytes. It
// keeps buffered latency at least one device period but never more than a small
// constant, discarding overflow beyond that.
func (p *Player) maxPendingBytes() int {
	frameBytes := p.frameBytes()
	bytesPerSec := p.config.SampleRate * frameBytes
	if bytesPerSec <= 0 {
		return 1 << 16
	}
	ms := p.config.BufferMs
	if ms < 50 {
		ms = 50
	}
	raw := (bytesPerSec * ms) / 1000
	return (raw / frameBytes) * frameBytes
}

// onData is called by miniaudio on its real-time audio thread.
// It must remain lock-free to avoid priority inversion against the network goroutine.
func (p *Player) onData(out, _ []byte, framecount uint32) {
	frameBytes := p.frameBytes()
	max := p.maxPendingBytes()
	avail := p.ring.Len()
	if avail > max {
		drop := avail - max
		// Align drop up to the next frame boundary so frame alignment is strictly
		// maintained (preventing left/right channel flips or byte-shift distortion).
		if rem := drop % frameBytes; rem != 0 {
			drop += frameBytes - rem
		}
		p.ring.Discard(drop)
	}

	n := p.ring.Read(out)

	vol := math.Float64frombits(p.volumeBits.Load())
	if vol != 1.0 {
		scaleS16(out[:n], vol)
	}
	// zero any remaining space (underflow -> silence)
	for i := n; i < len(out); i++ {
		out[i] = 0
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

	// Stop old device before resetting the SPSC ring buffer so onData is no longer running.
	if old != nil {
		_ = old.Stop()
		old.Uninit()
	}
	p.ring.Reset()

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

	// Stop device before resetting the SPSC ring buffer so onData is no longer running.
	if dev != nil {
		_ = dev.Stop()
		dev.Uninit()
	}
	p.ring.Reset()
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
