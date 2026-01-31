package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/jfreymuth/pulse"
	"github.com/jfreymuth/pulse/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"go-audio-sharer/internal/audio"
	pb "go-audio-sharer/proto"
)

type playbackManager struct {
	pulseClient   *pulse.Client
	playStream    *pulse.PlaybackStream
	pipeReader    *io.PipeReader
	pipeWriter    *io.PipeWriter
	deviceMonitor *audio.DeviceMonitor
	mu            sync.Mutex

	// Configuration
	sampleRate int
	channels   int
	latencySec float64
	device     string

	// State
	restartCh chan struct{}
}

func newPlaybackManager(sampleRate, channels int, latencySec float64, device string) (*playbackManager, error) {
	pulseClient, err := pulse.NewClient()
	if err != nil {
		return nil, fmt.Errorf("could not create pulse client: %w", err)
	}

	pr, pw := io.Pipe()

	pm := &playbackManager{
		pulseClient: pulseClient,
		pipeReader:  pr,
		pipeWriter:  pw,
		sampleRate:  sampleRate,
		channels:    channels,
		latencySec:  latencySec,
		device:      device,
		restartCh:   make(chan struct{}, 1),
	}

	return pm, nil
}

func (pm *playbackManager) startDeviceMonitor() {
	if pm.device != "" {
		return
	}

	var err error
	pm.deviceMonitor, err = audio.NewDeviceMonitor()
	if err != nil {
		log.Printf("Warning: Could not start device monitor: %v", err)
		return
	}

	pm.deviceMonitor.OnDefaultDeviceChanged(func(eventType string) {
		pm.triggerRestart()
	})
	log.Println("Device monitor started - will auto-switch on default device changes")
}

func (pm *playbackManager) triggerRestart() {
	select {
	case pm.restartCh <- struct{}{}:
		log.Println("Device change detected, signaling restart...")
	default:
	}
}

func (pm *playbackManager) createPlaybackStream() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Stop existing stream if any
	if pm.playStream != nil {
		pm.playStream.Stop()
		pm.playStream = nil
	}

	// Configure Playback
	var playbackOpts []pulse.PlaybackOption
	playbackOpts = append(playbackOpts,
		pulse.PlaybackSampleRate(pm.sampleRate),
		pulse.PlaybackLatency(pm.latencySec),
	)

	if pm.channels == 2 {
		playbackOpts = append(playbackOpts, pulse.PlaybackStereo)
	} else {
		playbackOpts = append(playbackOpts, pulse.PlaybackMono)
	}

	if pm.device != "" {
		sinks, err := pm.pulseClient.ListSinks()
		if err == nil {
			for _, s := range sinks {
				if strings.Contains(strings.ToLower(s.Name()), strings.ToLower(pm.device)) ||
					strings.Contains(strings.ToLower(s.ID()), strings.ToLower(pm.device)) {
					fmt.Printf("Selected Playback Sink: %s (%s)\n", s.Name(), s.ID())
					playbackOpts = append(playbackOpts, pulse.PlaybackSink(s))
					break
				}
			}
		}
	}

	// Create playback stream using the same pipe reader
	reader := pulse.NewReader(pm.pipeReader, proto.FormatInt16LE)

	var err error
	pm.playStream, err = pm.pulseClient.NewPlayback(reader, playbackOpts...)
	if err != nil {
		return fmt.Errorf("could not create playback stream: %w", err)
	}

	pm.playStream.Start()
	log.Println("Playback started")

	return nil
}

func (pm *playbackManager) write(data []byte) error {
	// We don't lock here as pw.Write is thread-safe and we never change pipeWriter
	_, err := pm.pipeWriter.Write(data)
	return err
}

func (pm *playbackManager) close() {
	pm.mu.Lock()
	if pm.playStream != nil {
		pm.playStream.Stop()
	}
	if pm.pipeWriter != nil {
		pm.pipeWriter.Close()
	}
	pm.mu.Unlock()

	if pm.deviceMonitor != nil {
		pm.deviceMonitor.Close()
	}
	if pm.pulseClient != nil {
		pm.pulseClient.Close()
	}
}

func main() {
	serverAddr := flag.String("server", "localhost:50051", "The server address in the format of host:port")
	device := flag.String("device", "", "Audio device name to play to (optional)")
	latencyMs := flag.Int("latency", 100, "Playback latency in milliseconds")
	flag.Parse()

	log.Printf("Connecting to server: %s", *serverAddr)
	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewAudioStreamClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutting down...")
		cancel()
	}()

	fmtInfo, err := c.GetFormat(ctx, &emptypb.Empty{})
	if err != nil {
		log.Fatalf("could not get audio format: %v", err)
	}
	log.Printf("Audio Format: %s %dHz %dCh", fmtInfo.Codec, fmtInfo.SampleRate, fmtInfo.Channels)

	stream, err := c.GetStream(ctx, &emptypb.Empty{})
	if err != nil {
		log.Fatalf("could not create stream: %v", err)
	}

	latencySec := float64(*latencyMs) / 1000.0
	pm, err := newPlaybackManager(int(fmtInfo.SampleRate), int(fmtInfo.Channels), latencySec, *device)
	if err != nil {
		log.Fatalf("Could not create playback manager: %v", err)
	}
	defer pm.close()

	// Channel to signal when receiving is done
	done := make(chan struct{})

	// 1. Start the data pumping goroutine FIRST
	// This ensures that when NewPlayback/Start are called, there's a writer ready.
	go func() {
		defer close(done)
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("Failed to receive: %v", err)
				}
				return
			}
			if err := pm.write(in.Data); err != nil {
				if ctx.Err() == nil {
					log.Printf("Failed to write to playback: %v", err)
				}
				return
			}
		}
	}()

	// 2. Create initial playback stream
	if err := pm.createPlaybackStream(); err != nil {
		log.Fatalf("Could not start playback: %v", err)
	}

	pm.startDeviceMonitor()

	// Goroutine to handle device changes
	go func() {
		for {
			select {
			case <-pm.restartCh:
				log.Println("Restarting playback on new device...")
				if err := pm.createPlaybackStream(); err != nil {
					log.Printf("Error restarting playback: %v", err)
				}
			case <-done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}

	pm.mu.Lock()
	if pm.playStream != nil {
		pm.playStream.Drain()
	}
	pm.mu.Unlock()
}
