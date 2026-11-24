package audio

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gen2brain/malgo"
)

func fakeDeviceInfo(tag byte) *malgo.DeviceInfo {
	var info malgo.DeviceInfo
	info.ID[0] = tag
	return &info
}

func TestDeviceMonitorCloseIdempotent(t *testing.T) {
	dm := &DeviceMonitor{
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
	close(dm.done) // Simulate run loop exiting or done initialized

	// Multiple calls should not panic
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dm.close()
		}()
	}
	wg.Wait()
}

func TestDeviceMonitorSetCurrent(t *testing.T) {
	dm := &DeviceMonitor{
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
	dm.setCurrent("device-123")
	dm.mu.Lock()
	cur := dm.current
	dm.mu.Unlock()

	if cur != "device-123" {
		t.Fatalf("current = %q, want 'device-123'", cur)
	}

	dm.setFilter("HDMI")
	dm.mu.Lock()
	f := dm.filter
	cur = dm.current
	dm.mu.Unlock()
	if f != "HDMI" || cur != "" {
		t.Fatalf("after setFilter('HDMI'), filter=%q current=%q, want 'HDMI' and ''", f, cur)
	}
}

func TestDeviceMonitorDebounceTransientGlitch(t *testing.T) {
	devA := fakeDeviceInfo('A')
	devB := fakeDeviceInfo('B')

	var calls atomic.Int32
	var changes atomic.Int32

	dm := &DeviceMonitor{
		pollFn: func(_ string) (*malgo.DeviceInfo, error) {
			// First poll sees transient B, confirmation re-poll returns back to A.
			if calls.Add(1) == 1 {
				return devB, nil
			}
			return devA, nil
		},
		debounce: 10 * time.Millisecond,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
		onChange: func(info *malgo.DeviceInfo) bool {
			changes.Add(1)
			return true
		},
	}
	dm.setCurrent(devA.ID.String())

	dm.pollOnce()
	if changes.Load() != 0 {
		t.Fatalf("expected transient glitch to be ignored, got %d changes", changes.Load())
	}
}

func TestDeviceMonitorConfirmedChange(t *testing.T) {
	devA := fakeDeviceInfo('A')
	devB := fakeDeviceInfo('B')

	var changes atomic.Int32
	dm := &DeviceMonitor{
		pollFn: func(_ string) (*malgo.DeviceInfo, error) {
			return devB, nil
		},
		debounce: 10 * time.Millisecond,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
		onChange: func(info *malgo.DeviceInfo) bool {
			changes.Add(1)
			return true
		},
	}
	dm.setCurrent(devA.ID.String())

	dm.pollOnce()
	if changes.Load() != 1 {
		t.Fatalf("expected confirmed change to fire once, got %d", changes.Load())
	}

	// Subsequent poll with same device should not fire again.
	dm.pollOnce()
	if changes.Load() != 1 {
		t.Fatalf("expected no duplicate change callback, got %d", changes.Load())
	}
}

func TestDeviceMonitorRetriesFailedOnChange(t *testing.T) {
	devA := fakeDeviceInfo('A')
	devB := fakeDeviceInfo('B')

	var attempts atomic.Int32
	dm := &DeviceMonitor{
		pollFn: func(_ string) (*malgo.DeviceInfo, error) {
			return devB, nil
		},
		debounce: 5 * time.Millisecond,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
		onChange: func(info *malgo.DeviceInfo) bool {
			// First two attempts fail transiently; third succeeds.
			return attempts.Add(1) >= 3
		},
	}
	dm.setCurrent(devA.ID.String())

	dm.pollOnce()
	dm.pollOnce()
	dm.pollOnce()
	dm.pollOnce() // should not fire again once attempt 3 succeeded

	if got := attempts.Load(); got != 3 {
		t.Fatalf("onChange attempts = %d, want 3 (2 retries + 1 success)", got)
	}
}

func TestDeviceMonitorRetriesAfterFallbackToPreviousDevice(t *testing.T) {
	devA := fakeDeviceInfo('A')
	devB := fakeDeviceInfo('B')

	var active atomic.Pointer[malgo.DeviceInfo]
	active.Store(devB)

	var lastApplied atomic.Pointer[malgo.DeviceInfo]
	var attempts atomic.Int32

	dm := &DeviceMonitor{
		pollFn: func(_ string) (*malgo.DeviceInfo, error) {
			return active.Load(), nil
		},
		debounce: 5 * time.Millisecond,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
		onChange: func(info *malgo.DeviceInfo) bool {
			if attempts.Add(1) == 1 {
				// Opening devB fails transiently (old devA was already stopped).
				return false
			}
			lastApplied.Store(info)
			return true
		},
	}
	dm.setCurrent(devA.ID.String())

	// Poll 1: sees devB, onChange(B) fails -> dirty = true.
	dm.pollOnce()
	// OS falls back to devA before next poll.
	active.Store(devA)
	// Poll 2: even though devA == dm.current, dirty forces reopen of devA!
	dm.pollOnce()

	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}
	if got := lastApplied.Load(); got == nil || got.ID != devA.ID {
		t.Fatalf("expected fallback device A to be reopened, got %v", got)
	}
}

func TestDeviceMonitorMarkDirtyReopensSameGUIDAfterDockReplug(t *testing.T) {
	// Simulates a USB-C dock unplugged and replugged with the exact same WASAPI
	// endpoint GUID (devA): markDirty from miniaudio Stop callback must reopen it.
	devA := fakeDeviceInfo('A')
	var dockPresent atomic.Bool

	reopened := make(chan struct{}, 1)
	dm := newDeviceMonitorWith(
		func(_ string) (*malgo.DeviceInfo, error) {
			if !dockPresent.Load() {
				return nil, errNoPlaybackDevice
			}
			return devA, nil
		},
		20*time.Millisecond,
		5*time.Millisecond,
		"",
		func(info *malgo.DeviceInfo) bool {
			select {
			case reopened <- struct{}{}:
			default:
			}
			return true
		},
	)
	defer dm.close()

	dm.setCurrent(devA.ID.String())

	// Dock unplugged -> unexpected Stop callback calls markDirty().
	dm.markDirty()
	time.Sleep(35 * time.Millisecond)

	// Dock replugged with the exact same GUID.
	dockPresent.Store(true)
	select {
	case <-reopened:
	case <-time.After(time.Second):
		t.Fatal("expected DeviceMonitor to reopen same-GUID device after dock replug")
	}
}

func TestDeviceMonitorPinnedDeviceReopensAfterDockReplug(t *testing.T) {
	// Simulates a pinned dock device ("USB Dock DAC") unplugged and reattached.
	dockDev := fakeDeviceInfo('D')
	var dockPresent atomic.Bool
	var attempts atomic.Int32

	dm := &DeviceMonitor{
		filter: "USB Dock DAC",
		pollFn: func(filter string) (*malgo.DeviceInfo, error) {
			if filter != "USB Dock DAC" || !dockPresent.Load() {
				return nil, errNoPlaybackDevice
			}
			return dockDev, nil
		},
		debounce: 5 * time.Millisecond,
		wakeCh:   make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
		onChange: func(info *malgo.DeviceInfo) bool {
			attempts.Add(1)
			return true
		},
	}
	dm.setCurrent(dockDev.ID.String())

	// While healthy (not dirty), pollOnce is a no-op for pinned devices.
	dockPresent.Store(true)
	dm.pollOnce()
	if attempts.Load() != 0 {
		t.Fatalf("expected 0 polls when pinned device is healthy, got %d", attempts.Load())
	}

	// Dock unplugged -> Stop callback marks dirty; poll while absent does nothing.
	dockPresent.Store(false)
	dm.markDirty()
	dm.pollOnce()
	if attempts.Load() != 0 {
		t.Fatalf("expected 0 reopens while dock absent, got %d", attempts.Load())
	}

	// Dock reattached -> pollOnce reopens pinned device and clears dirty.
	dockPresent.Store(true)
	dm.pollOnce()
	if attempts.Load() != 1 {
		t.Fatalf("expected 1 reopen once pinned dock device returned, got %d", attempts.Load())
	}

	// Subsequent poll after recovery stays idle.
	dm.pollOnce()
	if attempts.Load() != 1 {
		t.Fatalf("expected no duplicate reopen after recovery, got %d", attempts.Load())
	}
}

func TestDeviceMonitorCloseCancelsMidDebounce(t *testing.T) {
	devA := fakeDeviceInfo('A')
	devB := fakeDeviceInfo('B')

	enteredPoll := make(chan struct{}, 1)
	var changes atomic.Int32

	dm := newDeviceMonitorWith(
		func(_ string) (*malgo.DeviceInfo, error) {
			select {
			case enteredPoll <- struct{}{}:
			default:
			}
			return devB, nil
		},
		10*time.Millisecond,
		5*time.Second, // long debounce so close() must abort it immediately
		"",
		func(info *malgo.DeviceInfo) bool {
			changes.Add(1)
			return true
		},
	)
	dm.setCurrent(devA.ID.String())

	// Wait until run() enters pollOnce and hits the debounce wait.
	select {
	case <-enteredPoll:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for pollOnce")
	}

	start := time.Now()
	dm.close()
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("close() blocked for %v instead of cancelling debounce immediately", elapsed)
	}
	if changes.Load() != 0 {
		t.Fatalf("expected no onChange callback after close() cancelled debounce, got %d", changes.Load())
	}
}

func TestDeviceMonitorSetFilterDuringDebounceSuppressesChange(t *testing.T) {
	devA := fakeDeviceInfo('A')
	devB := fakeDeviceInfo('B')

	var changes atomic.Int32
	dm := &DeviceMonitor{
		pollFn: func(_ string) (*malgo.DeviceInfo, error) {
			return devB, nil
		},
		debounce: 40 * time.Millisecond,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
		onChange: func(info *malgo.DeviceInfo) bool {
			changes.Add(1)
			return true
		},
	}
	dm.setCurrent(devA.ID.String())

	done := make(chan struct{})
	go func() {
		defer close(done)
		dm.pollOnce()
	}()

	time.Sleep(10 * time.Millisecond)
	dm.setFilter("Pinned Device")
	<-done

	if changes.Load() != 0 {
		t.Fatalf("expected setFilter during debounce to suppress onChange, got %d", changes.Load())
	}
}
