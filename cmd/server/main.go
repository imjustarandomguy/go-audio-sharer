package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"go-audio-sharer/internal/audio"
	// Import the generated proto code.
	// The directory is 'proto', and the package declared inside is now 'proto'
	pb "go-audio-sharer/proto"
)

type server struct {
	pb.UnimplementedAudioStreamServer
	recorder *audio.Recorder
	config   audio.Config
}

func (s *server) GetStream(_ *emptypb.Empty, stream pb.AudioStream_GetStreamServer) error {
	log.Println("New client connected")
	ch := s.recorder.Subscribe()
	defer func() {
		s.recorder.Unsubscribe(ch)
		log.Println("Client disconnected")
	}()

	for block := range ch {
		err := stream.Send(&pb.AudioSample{
			Timestamp: block.Time.Format(time.RFC3339Nano), // ISO8601 Round-trip
			Data:      block.Data,
		})
		if err != nil {
			log.Printf("Error sending stream: %v", err)
			return err
		}
	}
	return nil
}

func (s *server) GetFormat(_ context.Context, _ *emptypb.Empty) (*pb.AudioFormat, error) {
	bytesPerSec := s.config.SampleRate * s.config.Channels * (s.config.BitsPerSample / 8)
	return &pb.AudioFormat{
		AverageBytesPerSecond: int32(bytesPerSec),
		BitsPerSample:         int32(s.config.BitsPerSample),
		BlockAlign:            int32(s.config.Channels * (s.config.BitsPerSample / 8)),
		Channels:              int32(s.config.Channels),
		SampleRate:            int32(s.config.SampleRate),
		Encoding:              "PCM",
		Codec:                 "PCM",
		Timestamp:             time.Now().UTC().Format(time.RFC3339Nano),
	}, nil
}

func main() {
	port := flag.String("port", "50051", "Port to listen on")
	bufferMs := flag.Int("buffer-ms", 10, "Buffer size in milliseconds")
	sampleRate := flag.Int("rate", 48000, "Sample rate")
	channels := flag.Int("channels", 2, "Number of channels")
	bits := flag.Int("bits", 16, "Bits per sample")
	device := flag.String("device", "", "Audio device name to loopback from (optional)")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	config := audio.Config{
		SampleRate:    *sampleRate,
		Channels:      *channels,
		BitsPerSample: *bits,
		BufferMs:      *bufferMs,
		Device:        *device,
	}

	log.Printf("Starting server with config: %+v", config)

	recorder, err := audio.NewRecorder(config)
	if err != nil {
		log.Fatalf("failed to initialize recorder: %v", err)
	}
	defer recorder.Close()

	s := grpc.NewServer()
	pb.RegisterAudioStreamServer(s, &server{
		recorder: recorder,
		config:   config,
	})

	log.Printf("Server listening on %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
