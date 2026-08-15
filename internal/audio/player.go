package audio

import (
	"io"
	"sync"
	"time"

	"github.com/ebitengine/oto/v3"
)

// Player streams raw PCM to the system's default audio output via Oto.
// Only one Oto context may exist per process; create it with NewAudioContext
// once, then create players with NewPlayer.
type Player struct {
	ctx        *oto.Context
	player     *oto.Player
	pipeReader *io.PipeReader
	pipeWriter *io.PipeWriter
	mu         sync.Mutex
	config     Config
	volume     float64
	isPlaying  bool
}

// NewAudioContext creates the single Oto context for the process. The returned
// channel is closed once the audio device is ready.
func NewAudioContext(cfg Config) (*oto.Context, chan struct{}, error) {
	format := oto.FormatSignedInt16LE
	switch cfg.BitsPerSample {
	case 8:
		format = oto.FormatUnsignedInt8
	case 32:
		format = oto.FormatFloat32LE
	}

	op := &oto.NewContextOptions{
		SampleRate:   cfg.SampleRate,
		ChannelCount: cfg.Channels,
		Format:       format,
		BufferSize:   time.Duration(cfg.BufferMs) * time.Millisecond,
	}

	return oto.NewContext(op)
}

func NewPlayer(otoCtx *oto.Context, cfg Config) (*Player, error) {
	pr, pw := io.Pipe()
	return &Player{
		ctx:        otoCtx,
		pipeReader: pr,
		pipeWriter: pw,
		config:     cfg,
		volume:     1.0,
	}, nil
}

// SetVolume sets the playback volume in [0, 1].
func (p *Player) SetVolume(v float64) {
	if v < 0 {
		v = 0
	}
	if v > 1 {
		v = 1
	}
	p.mu.Lock()
	p.volume = v
	player := p.player
	p.mu.Unlock()
	if player != nil {
		player.SetVolume(v)
	}
}

// Start creates the playback player and begins consuming from the pipe.
func (p *Player) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isPlaying {
		p.player.Play()
		return nil
	}

	p.player = p.ctx.NewPlayer(p.pipeReader)
	bytesPerSec := p.config.SampleRate * p.config.Channels * (p.config.BitsPerSample / 8)
	p.player.SetBufferSize((bytesPerSec * p.config.BufferMs) / 1000)
	p.player.SetVolume(p.volume)
	p.player.Play()
	p.isPlaying = true
	return nil
}

func (p *Player) Write(data []byte) error {
	_, err := p.pipeWriter.Write(data)
	return err
}

func (p *Player) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.player != nil {
		p.player.Pause()
		p.player.Close()
		p.player = nil
	}
	p.isPlaying = false
}

func (p *Player) Drain() {}

func (p *Player) Close() {
	p.Stop()
	if p.pipeWriter != nil {
		_ = p.pipeWriter.Close()
	}
}

func (p *Player) RestartCh() <-chan struct{} {
	ch := make(chan struct{})
	return ch
}
