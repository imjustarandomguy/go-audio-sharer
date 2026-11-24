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
		pollFn: func() (*malgo.DeviceInfo, error) {
			// First poll sees transient B, confirmation re-poll returns back to A.
			if calls.Add(1) == 1 {
				return devB, nil
			}
			return devA, nil
		},
		debounce: 10 * time.Millisecond,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
		onChange: func(info *malgo.DeviceInfo) {
			changes.Add(1)
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
		pollFn: func() (*malgo.DeviceInfo, error) {
			return devB, nil
		},
		debounce: 10 * time.Millisecond,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
		onChange: func(info *malgo.DeviceInfo) {
			changes.Add(1)
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
