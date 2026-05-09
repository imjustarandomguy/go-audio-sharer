package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"go-audio-sharer/internal/audio"
	pb "go-audio-sharer/proto"
)

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

	cfg := audio.Config{
		SampleRate:    int(fmtInfo.SampleRate),
		Channels:      int(fmtInfo.Channels),
		BitsPerSample: 16,
		BufferMs:      *latencyMs,
		Device:        *device,
	}
	player, err := audio.NewPlayer(cfg)
	if err != nil {
		log.Fatalf("Could not create player: %v", err)
	}
	defer player.Close()

	done := make(chan struct{})

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
			if err := player.Write(in.Data); err != nil {
				if ctx.Err() == nil {
					log.Printf("Failed to write to playback: %v", err)
				}
				return
			}
		}
	}()

	if err := player.Start(); err != nil {
		log.Fatalf("Could not start playback: %v", err)
	}

	go func() {
		for {
			select {
			case <-player.RestartCh():
				log.Println("Restarting playback on new device...")
				if err := player.Start(); err != nil {
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

	player.Drain()
}
