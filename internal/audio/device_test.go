package audio

import (
	"sync"
	"testing"
)

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
}
