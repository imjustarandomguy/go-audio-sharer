package audio

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/gen2brain/malgo"
)

// Player streams raw PCM to an audio output device via miniaudio (malgo).
// It supports selecting a specific output device and automatically follows the
// system default output device when none is explicitly selected.
type Player struct {
	ctx     *malgo.AllocatedContext
	device  *malgo.Device
	monitor *DeviceMonitor

	mu       sync.Mutex
	pending  []byte
	volume   float64
	started  bool
	config   Config
	devID    malgo.DeviceID
	devName  string
	maxBytes int
}

const defaultMaxPending = 1 << 19 // ~512KB of buffered PCM

func NewPlayer(cfg Config) (*Player, error) {
	ctx, err := newAudioContext()
	if err != nil {
		return nil, err
	}
	p := &Player{
		ctx:      ctx,
		config:   cfg,
		volume:   1.0,
		maxBytes: defaultMaxPending,
	}
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
	p.mu.Lock()
	p.volume = v
	p.mu.Unlock()
}

// createDeviceLocked initializes the playback device for the currently chosen
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
	fmt.Printf("Playback device: %s\n", info.Name())
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

func (p *Player) Write(data []byte) error {
	p.mu.Lock()
	p.pending = append(p.pending, data...)
	if len(p.pending) > p.maxBytes {
		drop := len(p.pending) - p.maxBytes
		p.pending = p.pending[drop:]
	}
	p.mu.Unlock()
	return nil
}

// onData is called by miniaudio on its audio thread.
func (p *Player) onData(out, _ []byte, framecount uint32) {
	p.mu.Lock()
	n := copy(out, p.pending)
	p.pending = p.pending[n:]
	vol := p.volume
	p.mu.Unlock()

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
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	p.started = false
	old := p.device
	p.device = nil
	p.pending = p.pending[:0]
	p.devID = info.ID
	p.devName = info.Name()
	p.mu.Unlock()

	if old != nil {
		_ = old.Stop()
		old.Uninit()
	}

	dev, err := p.createDevice()
	if err != nil {
		fmt.Printf("Error reopening device after change: %v\n", err)
		return
	}
	if err := dev.Start(); err != nil {
		fmt.Printf("Error starting device after change: %v\n", err)
		dev.Uninit()
		return
	}

	p.mu.Lock()
	if p.started {
		_ = dev.Stop()
		dev.Uninit()
	} else {
		p.device = dev
		p.started = true
	}
	p.mu.Unlock()
}

func (p *Player) Stop() {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	p.started = false
	dev := p.device
	p.device = nil
	p.pending = p.pending[:0]
	p.mu.Unlock()

	if dev != nil {
		_ = dev.Stop()
		dev.Uninit()
	}
}

func (p *Player) Drain() {}

// LatencyMs returns the approximate current playback latency in milliseconds,
// derived from how much PCM is currently buffered ahead of the device.
func (p *Player) LatencyMs() float64 {
	p.mu.Lock()
	b := len(p.pending)
	cfg := p.config
	p.mu.Unlock()
	bytesPerSec := cfg.SampleRate * cfg.Channels * (cfg.BitsPerSample / 8)
	if bytesPerSec <= 0 {
		return 0
	}
	return float64(b) * 1000.0 / float64(bytesPerSec)
}

func (p *Player) Close() {
	p.Stop()
	if p.monitor != nil {
		p.monitor.close()
	}
	if p.ctx != nil {
		_ = p.ctx.Uninit()
		p.ctx.Free()
	}
}

func (p *Player) RestartCh() <-chan struct{} {
	ch := make(chan struct{})
	return ch
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
