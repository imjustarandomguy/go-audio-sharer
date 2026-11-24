package audio

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gen2brain/malgo"
)

// RecorderStats reports capture and broadcast glitch telemetry collected since
// the previous TakeStats call.
type RecorderStats struct {
	DroppedBlocks uint64
	MaxCaptureGap time.Duration
}

type Config struct {
	SampleRate    int
	Channels      int
	BitsPerSample int
	BufferMs      int
	Device        string
}

type Recorder struct {
	client      *lockedAudioContext
	stream      *malgo.Device
	broadcaster *Broadcaster
	config      Config
	monitor     *DeviceMonitor
	opMu        sync.Mutex
	mu          sync.Mutex
	isRecording bool
	captureID   malgo.DeviceID
	bufPool     sync.Pool

	lastCaptureNs   atomic.Int64
	maxCaptureGapNs atomic.Int64
}

func NewRecorder(cfg Config) (*Recorder, error) {
	// Prefer PulseAudio on Linux for loopback support (ALSA has no loopback);
	// falls back to ALSA/JACK where no PulseAudio server exists.
	c, err := newLockedAudioContext(recorderBackends)
	if err != nil {
		return nil, err
	}

	r := &Recorder{
		client: c,
		config: cfg,
	}
	r.broadcaster = NewBroadcaster(r.startRecording, r.stopRecording)
	r.monitor = newDeviceMonitorForBackends("recorder", recorderBackends, cfg.Device, func(info *malgo.DeviceInfo) bool {
		return r.handleDeviceChange(info)
	})
	return r, nil
}

// handleDeviceChange is called when the default audio device changes.
// Returns true when capture is active (or no subscribers exist), or false when
// restarting failed transiently so DeviceMonitor retries on the next poll.
func (r *Recorder) handleDeviceChange(info *malgo.DeviceInfo) bool {
	r.opMu.Lock()
	defer r.opMu.Unlock()

	r.mu.Lock()
	wasRecording := r.isRecording
	r.mu.Unlock()
	if wasRecording || r.broadcaster.HasSubscribers() {
		log.Println("Default device changed, restarting capture...")
		r.stopRecordingLocked()
		return r.startRecordingLocked()
	}
	return true
}

func (r *Recorder) Subscribe() chan CapturedBlock {
	return r.broadcaster.SubscribeWithChan()
}

func (r *Recorder) Unsubscribe(ch chan CapturedBlock) {
	r.broadcaster.UnsubscribeWithChan(ch)
}

// Restart stops and restarts capturing (when clients are connected) so the
// target device is re-detected.
func (r *Recorder) Restart() {
	r.opMu.Lock()
	defer r.opMu.Unlock()

	r.stopRecordingLocked()
	if r.broadcaster.HasSubscribers() {
		time.Sleep(200 * time.Millisecond)
		r.startRecordingLocked()
	}
	log.Println("Recorder restarted")
}

// SetDevice changes the loopback target device (substring match) and restarts
// capture if it is currently recording.
func (r *Recorder) SetDevice(name string) {
	r.opMu.Lock()
	defer r.opMu.Unlock()

	r.mu.Lock()
	changed := r.config.Device != name
	r.config.Device = name
	wasRecording := r.isRecording
	r.mu.Unlock()

	if r.monitor != nil {
		r.monitor.setFilter(name)
	}
	if changed {
		log.Printf("Loopback device set to %q", name)
	}
	if changed && (wasRecording || r.broadcaster.HasSubscribers()) {
		r.stopRecordingLocked()
		r.startRecordingLocked()
	}
}

// Device returns the currently configured loopback device substring.
func (r *Recorder) Device() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.config.Device
}

func (r *Recorder) startRecording() {
	r.opMu.Lock()
	defer r.opMu.Unlock()
	r.startRecordingLocked()
}

func (r *Recorder) startRecordingLocked() bool {
	r.mu.Lock()
	if r.isRecording {
		r.mu.Unlock()
		return true
	}
	devFilter := r.config.Device
	r.mu.Unlock()

	var (
		playID string
		stream *malgo.Device
	)
	err := r.client.do(func(ctx malgo.Context) error {
		// The monitor tracks the playback (sink) device; find it so we can seed its
		// current id once capture starts cleanly.
		play, err := playbackDevice(ctx, devFilter)
		if err != nil {
			log.Printf("Error selecting output to loop back: %v", err)
			return err
		}

		// Capture the monitor of the chosen/current default output device.
		info, err := loopbackCaptureDevice(ctx, devFilter)
		if err != nil {
			log.Printf("Error selecting monitor to loop back: %v", err)
			return err
		}
		r.mu.Lock()
		r.captureID = info.ID
		r.mu.Unlock()
		log.Printf("Looping back output monitor: %s", info.Name())

		dc := malgo.DefaultDeviceConfig(malgo.Capture)
		dc.Capture.Format = malgo.FormatS16
		dc.Capture.Channels = uint32(r.config.Channels)
		dc.SampleRate = uint32(r.config.SampleRate)
		dc.PeriodSizeInMilliseconds = uint32(r.config.BufferMs)
		dc.Alsa.NoMMap = 1
		dc.Capture.DeviceID = info.ID.Pointer()

		s, err := malgo.InitDevice(ctx, dc, malgo.DeviceCallbacks{Data: r.onData})
		if err != nil {
			log.Printf("Error creating loopback capture: %v", err)
			return err
		}
		if err := s.Start(); err != nil {
			log.Printf("Error starting loopback capture: %v", err)
			s.Uninit()
			return err
		}
		playID = play.ID.String()
		stream = s
		return nil
	})
	if err != nil {
		return false
	}

	if r.monitor != nil {
		r.monitor.setCurrent(playID)
	}
	r.mu.Lock()
	r.stream = stream
	r.isRecording = true
	r.mu.Unlock()
	log.Println("Started loopback capture")
	return true
}

// TakeStats returns and resets capture glitch telemetry collected since the last call.
func (r *Recorder) TakeStats() RecorderStats {
	return RecorderStats{
		DroppedBlocks: r.broadcaster.TakeDropped(),
		MaxCaptureGap: time.Duration(r.maxCaptureGapNs.Swap(0)),
	}
}

func (r *Recorder) allocBlock(in []byte) CapturedBlock {
	var pb *pooledBuf
	if v := r.bufPool.Get(); v != nil {
		pb = v.(*pooledBuf)
	} else {
		pb = &pooledBuf{pool: &r.bufPool}
	}
	if cap(pb.data) < len(in) {
		pb.data = make([]byte, len(in))
	} else {
		pb.data = pb.data[:len(in)]
	}
	copy(pb.data, in)
	pb.refs.Store(1)
	return CapturedBlock{
		Data: pb.data,
		buf:  pb,
	}
}

func (r *Recorder) onData(_, in []byte, framecount uint32) {
	if len(in) == 0 {
		return
	}
	now := monoNowNs()
	if prev := r.lastCaptureNs.Swap(now); prev > 0 && now > prev {
		recordMaxInt64(&r.maxCaptureGapNs, now-prev)
	}
	r.broadcaster.Broadcast(r.allocBlock(in))
}

func (r *Recorder) stopRecording() {
	r.opMu.Lock()
	defer r.opMu.Unlock()
	r.stopRecordingLocked()
}

func (r *Recorder) stopRecordingLocked() {
	r.mu.Lock()
	r.isRecording = false
	stream := r.stream
	r.stream = nil
	r.mu.Unlock()

	if stream != nil {
		_ = r.client.do(func(_ malgo.Context) error {
			_ = stream.Stop()
			stream.Uninit()
			return nil
		})
		r.lastCaptureNs.Store(0)
		log.Println("Stopped loopback capture")
	}
}

func (r *Recorder) Close() {
	if r.monitor != nil {
		r.monitor.close()
	}
	r.stopRecording()
	if r.client != nil {
		r.client.close()
	}
}
