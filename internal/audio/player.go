package audio

import (
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/jfreymuth/pulse"
	"github.com/jfreymuth/pulse/proto"
)

type Player struct {
	pulseClient   *pulse.Client
	playStream    *pulse.PlaybackStream
	pipeReader    *io.PipeReader
	pipeWriter    *io.PipeWriter
	deviceMonitor *DeviceMonitor
	mu            sync.Mutex

	config   Config
	isPlaying bool
	restartCh chan struct{}
}

func NewPlayer(cfg Config) (*Player, error) {
	pulseClient, err := pulse.NewClient()
	if err != nil {
		return nil, fmt.Errorf("could not create pulse client: %w", err)
	}

	pr, pw := io.Pipe()

	p := &Player{
		pulseClient: pulseClient,
		pipeReader:  pr,
		pipeWriter:  pw,
		config:      cfg,
		restartCh:   make(chan struct{}, 1),
	}

	if cfg.Device == "" {
		p.deviceMonitor, err = NewDeviceMonitor()
		if err != nil {
			fmt.Printf("Warning: Could not start device monitor: %v\n", err)
		} else {
			p.deviceMonitor.OnDefaultDeviceChanged(func(eventType string) {
				p.triggerRestart()
			})
			fmt.Println("Device monitor started - will auto-switch on default device changes")
		}
	}

	return p, nil
}

func (p *Player) triggerRestart() {
	select {
	case p.restartCh <- struct{}{}:
		fmt.Println("Device change detected, signaling restart...")
	default:
	}
}

func (p *Player) RestartCh() <-chan struct{} {
	return p.restartCh
}

func (p *Player) Start() error {
	return p.createPlaybackStream()
}

func (p *Player) createPlaybackStream() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.playStream != nil {
		p.playStream.Stop()
		p.playStream = nil
	}

	var playbackOpts []pulse.PlaybackOption
	playbackOpts = append(playbackOpts,
		pulse.PlaybackSampleRate(p.config.SampleRate),
		pulse.PlaybackLatency(float64(p.config.BufferMs)/1000.0),
	)

	if p.config.Channels == 2 {
		playbackOpts = append(playbackOpts, pulse.PlaybackStereo)
	} else {
		playbackOpts = append(playbackOpts, pulse.PlaybackMono)
	}

	if p.config.Device != "" {
		sinks, err := p.pulseClient.ListSinks()
		if err == nil {
			for _, s := range sinks {
				if strings.Contains(strings.ToLower(s.Name()), strings.ToLower(p.config.Device)) ||
					strings.Contains(strings.ToLower(s.ID()), strings.ToLower(p.config.Device)) {
					fmt.Printf("Selected Playback Sink: %s (%s)\n", s.Name(), s.ID())
					playbackOpts = append(playbackOpts, pulse.PlaybackSink(s))
					break
				}
			}
		}
	}

	reader := pulse.NewReader(p.pipeReader, proto.FormatInt16LE)

	var err error
	p.playStream, err = p.pulseClient.NewPlayback(reader, playbackOpts...)
	if err != nil {
		return fmt.Errorf("could not create playback stream: %w", err)
	}

	p.playStream.Start()
	p.isPlaying = true
	fmt.Println("Playback started")

	return nil
}

func (p *Player) Write(data []byte) error {
	_, err := p.pipeWriter.Write(data)
	return err
}

func (p *Player) Stop() {
	p.mu.Lock()
	p.isPlaying = false
	if p.playStream != nil {
		p.playStream.Stop()
	}
	p.mu.Unlock()
}

func (p *Player) Drain() {
	p.mu.Lock()
	if p.playStream != nil {
		p.playStream.Drain()
	}
	p.mu.Unlock()
}

func (p *Player) Close() {
	p.Stop()
	if p.pipeWriter != nil {
		p.pipeWriter.Close()
	}
	if p.deviceMonitor != nil {
		p.deviceMonitor.Close()
	}
	if p.pulseClient != nil {
		p.pulseClient.Close()
	}
}