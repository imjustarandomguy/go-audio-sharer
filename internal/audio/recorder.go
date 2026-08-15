package audio

import (
	"fmt"
	"sync"
	"time"

	"github.com/gen2brain/malgo"
)

type Config struct {
	SampleRate    int
	Channels      int
	BitsPerSample int
	BufferMs      int
	Device        string
}

type Recorder struct {
	client      *malgo.AllocatedContext
	stream      *malgo.Device
	broadcaster *Broadcaster
	config      Config
	monitor     *DeviceMonitor
	mu          sync.Mutex
	isRecording bool
	captureID   malgo.DeviceID
}

func NewRecorder(cfg Config) (*Recorder, error) {
	// Prefer PulseAudio on Linux for loopback support (ALSA has no loopback);
	// falls back to ALSA/JACK where no PulseAudio server exists.
	c, err := newAudioContextWith([]malgo.Backend{
		malgo.BackendPulseaudio,
		malgo.BackendAlsa,
		malgo.BackendJack,
	})
	if err != nil {
		return nil, err
	}

	r := &Recorder{
		client: c,
		config: cfg,
	}
	r.broadcaster = NewBroadcaster(r.startRecording, r.stopRecording)
	r.monitor = newDeviceMonitor(c.Context, cfg.Device, func(info *malgo.DeviceInfo) {
		r.handleDeviceChange(info)
	})
	return r, nil
}

// handleDeviceChange is called when the default audio device changes.
func (r *Recorder) handleDeviceChange(info *malgo.DeviceInfo) {
	r.mu.Lock()
	wasRecording := r.isRecording
	r.mu.Unlock()
	if wasRecording {
		fmt.Println("Default device changed, restarting capture...")
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

// Restart stops and restarts capturing so the target device is re-detected.
func (r *Recorder) Restart() {
	r.stopRecording()
	time.Sleep(200 * time.Millisecond)
	r.startRecording()
	fmt.Println("Recorder restarted")
}

func (r *Recorder) startRecording() {
	r.mu.Lock()
	if r.isRecording {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	// The monitor tracks the playback (sink) device; find it so we can seed its
	// current id correctly.
	play, err := playbackDevice(r.client.Context, r.config.Device)
	if err != nil {
		fmt.Printf("Error selecting output to loop back: %v\n", err)
		return
	}
	if r.monitor != nil {
		r.monitor.setCurrent(play.ID.String())
	}

	// Capture the monitor of the chosen/current default output device.
	info, err := loopbackCaptureDevice(r.client.Context, r.config.Device)
	if err != nil {
		fmt.Printf("Error selecting monitor to loop back: %v\n", err)
		return
	}
	r.mu.Lock()
	r.captureID = info.ID
	r.mu.Unlock()
	fmt.Printf("Looping back output monitor: %s\n", info.Name())

	dc := malgo.DefaultDeviceConfig(malgo.Capture)
	dc.Capture.Format = malgo.FormatS16
	dc.Capture.Channels = uint32(r.config.Channels)
	dc.SampleRate = uint32(r.config.SampleRate)
	dc.PeriodSizeInMilliseconds = uint32(r.config.BufferMs)
	dc.Alsa.NoMMap = 1
	dc.Capture.DeviceID = info.ID.Pointer()

	stream, err := malgo.InitDevice(r.client.Context, dc, malgo.DeviceCallbacks{Data: r.onData})
	if err != nil {
		fmt.Printf("Error creating loopback capture: %v\n", err)
		return
	}
	if err := stream.Start(); err != nil {
		fmt.Printf("Error starting loopback capture: %v\n", err)
		stream.Uninit()
		return
	}

	r.mu.Lock()
	r.stream = stream
	r.isRecording = true
	r.mu.Unlock()
	fmt.Println("Started loopback capture")
}

func (r *Recorder) onData(_, in []byte, framecount uint32) {
	if len(in) == 0 {
		return
	}
	block := make([]byte, len(in))
	copy(block, in)
	r.broadcaster.Broadcast(CapturedBlock{
		Data: block,
		Time: time.Now().UTC(),
	})
}

func (r *Recorder) stopRecording() {
	r.mu.Lock()
	r.isRecording = false
	stream := r.stream
	r.stream = nil
	r.mu.Unlock()

	if stream != nil {
		_ = stream.Stop()
		stream.Uninit()
		fmt.Println("Stopped loopback capture")
	}
}

func (r *Recorder) Close() {
	r.stopRecording()
	if r.monitor != nil {
		r.monitor.close()
	}
	if r.client != nil {
		_ = r.client.Uninit()
		r.client.Free()
	}
}
