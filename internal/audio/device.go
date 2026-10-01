package audio

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gen2brain/malgo"
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
	return nil, fmt.Errorf("no playback device available")
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

var (
	discoveryMu  sync.Mutex
	discoveryCtx *malgo.AllocatedContext
)

func getDiscoveryContext() (*malgo.AllocatedContext, error) {
	discoveryMu.Lock()
	defer discoveryMu.Unlock()
	if discoveryCtx != nil {
		return discoveryCtx, nil
	}
	c, err := newAudioContext()
	if err != nil {
		return nil, err
	}
	discoveryCtx = c
	return discoveryCtx, nil
}

// ListPlaybackDeviceDescriptions returns the playback devices for display.
// It reuses a shared discovery audio context to avoid re-initializing the OS
// audio driver on every poll.
func ListPlaybackDeviceDescriptions() ([]DeviceDesc, error) {
	c, err := getDiscoveryContext()
	if err != nil {
		return nil, err
	}
	desc, err := ListPlaybackDeviceDescriptionsWith(c.Context)
	if err != nil {
		// If querying failed (e.g. audio server restarted), invalidate and retry once.
		discoveryMu.Lock()
		if discoveryCtx == c {
			_ = discoveryCtx.Uninit()
			discoveryCtx.Free()
			discoveryCtx = nil
		}
		discoveryMu.Unlock()

		c2, err2 := getDiscoveryContext()
		if err2 != nil {
			return nil, err2
		}
		return ListPlaybackDeviceDescriptionsWith(c2.Context)
	}
	return desc, nil
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
type DeviceMonitor struct {
	ctx      malgo.Context
	filter   string
	mu       sync.Mutex
	current  string
	onChange func(info *malgo.DeviceInfo)
	stopCh   chan struct{}
	done     chan struct{}
	once     sync.Once
}

func newDeviceMonitor(ctx malgo.Context, filter string, onChange func(*malgo.DeviceInfo)) *DeviceMonitor {
	dm := &DeviceMonitor{
		ctx:      ctx,
		filter:   filter,
		onChange: onChange,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}
	if filter == "" {
		go dm.run()
	} else {
		close(dm.done)
	}
	return dm
}

func (dm *DeviceMonitor) run() {
	defer close(dm.done)
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-dm.stopCh:
			return
		case <-t.C:
			info, err := playbackDevice(dm.ctx, dm.filter)
			if err != nil {
				continue
			}
			id := info.ID.String()
			dm.mu.Lock()
			changed := dm.current != "" && id != dm.current
			dm.current = id
			dm.mu.Unlock()
			if changed && dm.onChange != nil {
				dm.onChange(info)
			}
		}
	}
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
