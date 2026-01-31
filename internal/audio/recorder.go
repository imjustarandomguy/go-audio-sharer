package audio

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jfreymuth/pulse"
	"github.com/jfreymuth/pulse/proto"
)

type Config struct {
	SampleRate    int
	Channels      int
	BitsPerSample int
	BufferMs      int
	Device        string
}

type Recorder struct {
	client        *pulse.Client
	stream        *pulse.RecordStream
	broadcaster   *Broadcaster
	config        Config
	deviceMonitor *DeviceMonitor
	mu            sync.Mutex
	isRecording   bool
}

// AudioWriter implements pulse.Writer interface
type AudioWriter struct {
	callback func([]byte)
}

func (w *AudioWriter) Write(p []byte) (int, error) {
	data := make([]byte, len(p))
	copy(data, p)
	w.callback(data)
	return len(p), nil
}

func (w *AudioWriter) Format() byte {
	return proto.FormatInt16LE
}

func NewRecorder(cfg Config) (*Recorder, error) {
	c, err := pulse.NewClient()
	if err != nil {
		return nil, err
	}

	r := &Recorder{
		client: c,
		config: cfg,
	}

	r.broadcaster = NewBroadcaster(r.startRecording, r.stopRecording)

	// Only monitor for default device changes if no specific device is configured
	if cfg.Device == "" {
		r.deviceMonitor, err = NewDeviceMonitor()
		if err != nil {
			fmt.Printf("Warning: Could not start device monitor: %v\n", err)
			// Continue without monitoring - not a fatal error
		} else {
			r.deviceMonitor.OnDefaultDeviceChanged(func(eventType string) {
				r.handleDeviceChange()
			})
			fmt.Println("Device monitor started - will auto-switch on default device changes")
		}
	}

	return r, nil
}

// handleDeviceChange is called when the default audio device changes
func (r *Recorder) handleDeviceChange() {
	r.mu.Lock()
	wasRecording := r.isRecording
	r.mu.Unlock()

	if wasRecording {
		fmt.Println("Default device changed, restarting recording...")
		r.stopRecording()
		r.startRecording()
	}
}

func (r *Recorder) Subscribe() chan CapturedBlock {
	return r.broadcaster.SubscribeWithChan()
}

func (r *Recorder) Unsubscribe(ch chan CapturedBlock) {
	r.broadcaster.UnsubscribeWithChan(ch)
}

func (r *Recorder) startRecording() {
	var opts []pulse.RecordOption

	// 1. Device Selection
	if r.config.Device != "" {
		// Try to find a sink (for loopback) matching the name
		sinks, err := r.client.ListSinks()
		if err == nil {
			for _, s := range sinks {
				if strings.Contains(strings.ToLower(s.Name()), strings.ToLower(r.config.Device)) ||
					strings.Contains(strings.ToLower(s.ID()), strings.ToLower(r.config.Device)) {
					fmt.Printf("Selected Sink Monitor: %s (%s)\n", s.Name(), s.ID())
					opts = append(opts, pulse.RecordMonitor(s))
					goto DeviceFound
				}
			}
		}

		// Try to find a source matching the name
		sources, err := r.client.ListSources()
		if err == nil {
			for _, s := range sources {
				if strings.Contains(strings.ToLower(s.Name()), strings.ToLower(r.config.Device)) ||
					strings.Contains(strings.ToLower(s.ID()), strings.ToLower(r.config.Device)) {
					fmt.Printf("Selected Source: %s (%s)\n", s.Name(), s.ID())
					opts = append(opts, pulse.RecordSource(s))
					goto DeviceFound
				}
			}
		}
		fmt.Printf("Warning: Device '%s' not found, using default.\n", r.config.Device)
	}

	if len(opts) == 0 {
		sink, err := r.client.DefaultSink()
		if err != nil {
			fmt.Printf("Error getting default sink: %v\n", err)
			return
		}
		fmt.Printf("Using Default Sink Monitor: %s\n", sink.Name())
		opts = append(opts, pulse.RecordMonitor(sink))
	}

DeviceFound:
	// 2. Audio Format
	opts = append(opts, pulse.RecordSampleRate(r.config.SampleRate))
	switch r.config.Channels {
	case 2:
		opts = append(opts, pulse.RecordStereo)
	case 1:
		opts = append(opts, pulse.RecordMono)
	default:
		// Fallback for custom channels if needed, but simple Stereo/Mono covers most
		opts = append(opts, pulse.RecordStereo)
	}

	// 3. Buffer Size Calculation
	// Bytes = (SampleRate * Channels * Bits/8) * (Ms / 1000)
	bytesPerSample := r.config.BitsPerSample / 8
	frameSize := r.config.Channels * bytesPerSample
	bufferBytes := (r.config.SampleRate * frameSize * r.config.BufferMs) / 1000

	// Ensure alignment
	if bufferBytes%frameSize != 0 {
		bufferBytes += frameSize - (bufferBytes % frameSize)
	}

	fmt.Printf("Buffer Config: %d ms -> %d bytes\n", r.config.BufferMs, bufferBytes)
	opts = append(opts, pulse.RecordBufferFragmentSize(uint32(bufferBytes)))

	writer := &AudioWriter{
		callback: func(data []byte) {
			r.broadcaster.Broadcast(CapturedBlock{
				Data: data,
				Time: time.Now().UTC(),
			})
		},
	}

	var err error
	r.stream, err = r.client.NewRecord(writer, opts...)
	if err != nil {
		fmt.Printf("Error creating record stream: %v\n", err)
		return
	}

	r.stream.Start()

	r.mu.Lock()
	r.isRecording = true
	r.mu.Unlock()

	fmt.Println("Started recording loopback audio")
}

func (r *Recorder) stopRecording() {
	r.mu.Lock()
	r.isRecording = false
	r.mu.Unlock()

	if r.stream != nil {
		r.stream.Stop()
		r.stream.Close()
		r.stream = nil
		fmt.Println("Stopped recording loopback audio")
	}
}

func (r *Recorder) Close() {
	r.stopRecording()
	if r.deviceMonitor != nil {
		r.deviceMonitor.Close()
	}
	if r.client != nil {
		r.client.Close()
	}
}
