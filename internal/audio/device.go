package audio

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gen2brain/malgo"
)

const (
	devicePollInterval = 5 * time.Second
	deviceDebounce     = 250 * time.Millisecond
)

var (
	errNoPlaybackDevice   = errors.New("no playback device available")
	errAudioContextClosed = errors.New("audio context closed")

	// recorderBackends matches Recorder's preferred backend order on Linux
	// (PulseAudio first for monitor loopback support, avoiding the null backend).
	recorderBackends = []malgo.Backend{
		malgo.BackendPulseaudio,
		malgo.BackendAlsa,
		malgo.BackendJack,
	}
)

// newAudioContext creates the process audio context (miniaudio).
func newAudioContext() (*malgo.AllocatedContext, error) {
	return newAudioContextWith(nil)
}

// newAudioContextWith creates an audio context. A nil backends slice uses the
// platform defaults; providing one lets callers prefer a backend (e.g. PulseAudio
// on Linux, which is required for loopback capture).
func newAudioContextWith(backends []malgo.Backend) (*malgo.AllocatedContext, error) {
	ctx, err := malgo.InitContext(backends, malgo.ContextConfig{}, nil)
	if err != nil {
		return nil, fmt.Errorf("could not init audio context: %w", err)
	}
	return ctx, nil
}

type ctxReq struct {
	fn   func(malgo.Context) error
	resp chan error
}

// lockedAudioContext owns a malgo.AllocatedContext pinned to a single OS thread
// via runtime.LockOSThread(). This ensures Windows WASAPI COM initialization
// (CoInitializeEx), device control, and teardown (CoUninitialize) always run on
// the same OS thread, regardless of which Go goroutine calls Start/Stop/Close.
type lockedAudioContext struct {
	reqCh  chan ctxReq
	stopCh chan struct{}
	done   chan struct{}
	once   sync.Once
}

func newLockedAudioContext(backends []malgo.Backend) (*lockedAudioContext, error) {
	reqCh := make(chan ctxReq)
	stopCh := make(chan struct{})
	done := make(chan struct{})
	initErr := make(chan error, 1)

	go func() {
		runtime.LockOSThread()
		defer close(done)

		c, err := newAudioContextWith(backends)
		if err != nil {
			initErr <- err
			return
		}
		initErr <- nil
		defer func() {
			_ = c.Uninit()
			c.Free()
		}()

		for {
			select {
			case <-stopCh:
				return
			case req := <-reqCh:
				req.resp <- req.fn(c.Context)
			}
		}
	}()

	if err := <-initErr; err != nil {
		<-done
		return nil, err
	}
	return &lockedAudioContext{
		reqCh:  reqCh,
		stopCh: stopCh,
		done:   done,
	}, nil
}

func (l *lockedAudioContext) do(fn func(malgo.Context) error) error {
	resp := make(chan error, 1)
	select {
	case <-l.stopCh:
		return errAudioContextClosed
	case l.reqCh <- ctxReq{fn: fn, resp: resp}:
	}
	select {
	case err := <-resp:
		return err
	case <-l.done:
		return errAudioContextClosed
	}
}

func (l *lockedAudioContext) close() {
	l.once.Do(func() {
		close(l.stopCh)
		<-l.done
	})
}

// playbackDevice selects the playback device to use: a substring match against
// filter when set, otherwise the system default device.
func playbackDevice(ctx malgo.Context, filter string) (*malgo.DeviceInfo, error) {
	devs, err := ctx.Devices(malgo.Playback)
	if err != nil {
		return nil, fmt.Errorf("could not enumerate audio devices: %w", err)
	}
	if filter != "" {
		for i := range devs {
			if strings.Contains(strings.ToLower(devs[i].Name()), strings.ToLower(filter)) {
				return &devs[i], nil
			}
		}
	}
	for i := range devs {
		if devs[i].IsDefault != 0 {
			return &devs[i], nil
		}
	}
	if len(devs) > 0 {
		return &devs[0], nil
	}
	return nil, errNoPlaybackDevice
}

// listPlaybackDevices returns all playback devices for UI display.
func listPlaybackDevices(ctx malgo.Context) ([]malgo.DeviceInfo, error) {
	return ctx.Devices(malgo.Playback)
}

// DeviceDesc is a playback device description for UI display.
type DeviceDesc struct {
	Name      string
	ID        string
	IsDefault bool
}

type discoveryReq struct {
	key      string
	backends []malgo.Backend
	fn       func(malgo.Context) error
	resp     chan error
}

var (
	discoveryOnce sync.Once
	discoveryCh   chan discoveryReq
)

func startDiscoveryWorker() {
	discoveryCh = make(chan discoveryReq)
	go func() {
		// Pin the discovery worker to a single OS thread so Windows WASAPI
		// CoInitializeEx / CoCreateInstance / CoUninitialize always execute
		// on the same thread (preserving COM apartment and refcount invariants).
		runtime.LockOSThread()
		ctxs := make(map[string]*malgo.AllocatedContext)

		for req := range discoveryCh {
			c := ctxs[req.key]
			if c == nil {
				var err error
				c, err = newAudioContextWith(req.backends)
				if err != nil {
					req.resp <- err
					continue
				}
				ctxs[req.key] = c
			}

			err := req.fn(c.Context)
			// Only tear down and reinitialize the context on actual backend
			// failures, not when zero playback devices are currently attached.
			if err != nil && !errors.Is(err, errNoPlaybackDevice) {
				_ = c.Uninit()
				c.Free()
				delete(ctxs, req.key)

				c2, err2 := newAudioContextWith(req.backends)
				if err2 != nil {
					req.resp <- err2
					continue
				}
				ctxs[req.key] = c2
				err = req.fn(c2.Context)
			}
			req.resp <- err
		}
	}()
}

// withDiscoveryContext runs fn against an isolated, OS-thread-pinned discovery
// audio context. Isolating enumeration from the live streaming malgo.Context
// prevents OS driver / PulseAudio mainloop locks from stalling audio callbacks.
func withDiscoveryContext(key string, backends []malgo.Backend, fn func(malgo.Context) error) error {
	discoveryOnce.Do(startDiscoveryWorker)
	resp := make(chan error, 1)
	discoveryCh <- discoveryReq{
		key:      key,
		backends: backends,
		fn:       fn,
		resp:     resp,
	}
	return <-resp
}

// ListPlaybackDeviceDescriptions returns the playback devices for display.
// It reuses a shared discovery audio context to avoid re-initializing the OS
// audio driver on every poll.
func ListPlaybackDeviceDescriptions() ([]DeviceDesc, error) {
	var desc []DeviceDesc
	err := withDiscoveryContext("default", nil, func(ctx malgo.Context) error {
		var err error
		desc, err = ListPlaybackDeviceDescriptionsWith(ctx)
		return err
	})
	return desc, err
}

func defaultPlaybackDeviceFromDiscovery(key string, backends []malgo.Backend) (*malgo.DeviceInfo, error) {
	var info *malgo.DeviceInfo
	err := withDiscoveryContext(key, backends, func(ctx malgo.Context) error {
		var err error
		info, err = playbackDevice(ctx, "")
		return err
	})
	return info, err
}

// ListPlaybackDeviceDescriptionsWith returns playback devices from an existing context.
func ListPlaybackDeviceDescriptionsWith(ctx malgo.Context) ([]DeviceDesc, error) {
	devs, err := listPlaybackDevices(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]DeviceDesc, 0, len(devs))
	for i := range devs {
		out = append(out, DeviceDesc{
			Name:      devs[i].Name(),
			ID:        devs[i].ID.String(),
			IsDefault: devs[i].IsDefault != 0,
		})
	}
	return out, nil
}

// loopbackCaptureDevice finds the monitor capture device for the chosen/default
// playback device. On Linux/PulseAudio this is the "Monitor of <sink>" capture
// device, which streams whatever is currently playing (system loopback).
func loopbackCaptureDevice(ctx malgo.Context, filter string) (*malgo.DeviceInfo, error) {
	play, err := playbackDevice(ctx, filter)
	if err != nil {
		return nil, err
	}
	capDevs, err := ctx.Devices(malgo.Capture)
	if err != nil {
		return nil, err
	}
	monitorPrefix := "monitor of "
	for i := range capDevs {
		name := strings.ToLower(capDevs[i].Name())
		pn := strings.ToLower(play.Name())
		if strings.HasPrefix(name, monitorPrefix) && strings.Contains(name, pn) {
			return &capDevs[i], nil
		}
		if name == pn {
			return &capDevs[i], nil
		}
	}
	return nil, fmt.Errorf("no monitor capture device found for %q", play.Name())
}

// DeviceMonitor watches for the default playback device changing so the audio
// device can be re-created to follow it. Only meaningful when no specific
// device filter was chosen.
//
// Device enumeration runs against the isolated discovery context rather than
// the live streaming context so polling never contends with real-time audio.
type DeviceMonitor struct {
	pollFn   func() (*malgo.DeviceInfo, error)
	interval time.Duration
	debounce time.Duration

	filter   string
	mu       sync.Mutex
	current  string
	onChange func(info *malgo.DeviceInfo) bool
	stopCh   chan struct{}
	done     chan struct{}
	once     sync.Once
}

func newDeviceMonitor(filter string, onChange func(*malgo.DeviceInfo) bool) *DeviceMonitor {
	return newDeviceMonitorForBackends("default", nil, filter, onChange)
}

func newDeviceMonitorForBackends(key string, backends []malgo.Backend, filter string, onChange func(*malgo.DeviceInfo) bool) *DeviceMonitor {
	pollFn := func() (*malgo.DeviceInfo, error) {
		return defaultPlaybackDeviceFromDiscovery(key, backends)
	}
	return newDeviceMonitorWith(pollFn, devicePollInterval, deviceDebounce, filter, onChange)
}

func newDeviceMonitorWith(
	pollFn func() (*malgo.DeviceInfo, error),
	interval, debounce time.Duration,
	filter string,
	onChange func(*malgo.DeviceInfo) bool,
) *DeviceMonitor {
	dm := &DeviceMonitor{
		pollFn:   pollFn,
		interval: interval,
		debounce: debounce,
		filter:   filter,
		onChange: onChange,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}
	go dm.run()
	return dm
}

func (dm *DeviceMonitor) run() {
	defer close(dm.done)
	t := time.NewTicker(dm.interval)
	defer t.Stop()
	for {
		select {
		case <-dm.stopCh:
			return
		case <-t.C:
			dm.pollOnce()
		}
	}
}

func (dm *DeviceMonitor) pollOnce() {
	dm.mu.Lock()
	filter := dm.filter
	dm.mu.Unlock()
	if filter != "" || dm.pollFn == nil {
		return
	}

	info, err := dm.pollFn()
	if err != nil || info == nil {
		return
	}
	id := info.ID.String()

	dm.mu.Lock()
	if dm.filter != "" {
		dm.mu.Unlock()
		return
	}
	if dm.current == "" {
		dm.current = id
		dm.mu.Unlock()
		return
	}
	if id == dm.current {
		dm.mu.Unlock()
		return
	}
	dm.mu.Unlock()

	// Confirm the change after a short debounce so transient OS/driver
	// enumeration hiccups don't reset an active audio stream.
	if dm.debounce > 0 {
		timer := time.NewTimer(dm.debounce)
		select {
		case <-dm.stopCh:
			timer.Stop()
			return
		case <-timer.C:
		}
		info2, err := dm.pollFn()
		if err != nil || info2 == nil {
			return
		}
		id2 := info2.ID.String()
		if id2 != id {
			return
		}
		info = info2
	}

	// Avoid a wasted device reopen if close() was called while debouncing.
	select {
	case <-dm.stopCh:
		return
	default:
	}

	dm.mu.Lock()
	changed := dm.filter == "" && dm.current != "" && id != dm.current
	dm.mu.Unlock()

	if !changed {
		return
	}

	// Only commit dm.current after onChange reports success, so transient
	// reopen failures (e.g. Bluetooth/USB switch in progress) automatically
	// retry on the next poll instead of leaving audio permanently stalled.
	applied := true
	if dm.onChange != nil {
		applied = dm.onChange(info)
	}
	if applied {
		dm.mu.Lock()
		if dm.filter == "" {
			dm.current = id
		}
		dm.mu.Unlock()
	}
}

// setFilter updates the device filter. When empty, the monitor follows default
// device changes; when non-empty, default-device polling is paused.
func (dm *DeviceMonitor) setFilter(filter string) {
	dm.mu.Lock()
	dm.filter = filter
	if filter != "" {
		dm.current = ""
	}
	dm.mu.Unlock()
}

// setCurrent seeds the monitored device id (used right after opening a device)
// so the monitor does not immediately report a spurious change.
func (dm *DeviceMonitor) setCurrent(id string) {
	dm.mu.Lock()
	dm.current = id
	dm.mu.Unlock()
}

func (dm *DeviceMonitor) close() {
	dm.once.Do(func() {
		select {
		case <-dm.done:
			return
		default:
		}
		close(dm.stopCh)
		<-dm.done
	})
}
