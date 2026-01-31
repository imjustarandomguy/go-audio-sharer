package audio

import (
	"fmt"
	"sync"

	"github.com/jfreymuth/pulse/proto"
)

// DeviceMonitor watches for default device changes in PulseAudio
type DeviceMonitor struct {
	client      *proto.Client
	conn        interface{ Close() error }
	mu          sync.Mutex
	callbacks   []func(eventType string)
	currentSink string
	running     bool
	stopCh      chan struct{}
	checkCh     chan struct{} // Channel to trigger device check
}

// NewDeviceMonitor creates a new device monitor
func NewDeviceMonitor() (*DeviceMonitor, error) {
	client, conn, err := proto.Connect("")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PulseAudio: %w", err)
	}

	dm := &DeviceMonitor{
		client:  client,
		conn:    conn,
		stopCh:  make(chan struct{}),
		checkCh: make(chan struct{}, 1), // Buffered to avoid blocking callback
	}

	// Set client name first (required before other operations)
	props := proto.PropList{
		"application.name": proto.PropListString("go-audio-sharer-monitor"),
	}
	err = client.Request(&proto.SetClientName{Props: props}, nil)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set client name: %w", err)
	}

	// Get initial default sink
	dm.currentSink, _ = dm.getDefaultSinkName()

	// Start background goroutine to handle device checks
	// This is needed because we can't make requests from within the callback
	go dm.checkLoop()

	// Set up the callback for events
	client.Callback = func(val interface{}) {
		switch event := val.(type) {
		case *proto.SubscribeEvent:
			// EventServer indicates server-level changes (like default device changes)
			if event.Event.GetFacility() == proto.EventServer && event.Event.GetType() == proto.EventChange {
				// Signal the check goroutine instead of checking directly
				select {
				case dm.checkCh <- struct{}{}:
				default:
					// Already pending check
				}
			}
		}
	}

	// Subscribe to server events (use SubscriptionMaskAll to ensure we get server events)
	err = client.Request(&proto.Subscribe{Mask: proto.SubscriptionMaskAll}, nil)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to subscribe to PulseAudio events: %w", err)
	}

	dm.running = true
	return dm, nil
}

// checkLoop runs in a separate goroutine and handles device change checks
func (dm *DeviceMonitor) checkLoop() {
	for {
		select {
		case <-dm.checkCh:
			dm.checkDefaultSinkChanged()
		case <-dm.stopCh:
			return
		}
	}
}

// getDefaultSinkName returns the current default sink name
func (dm *DeviceMonitor) getDefaultSinkName() (string, error) {
	var reply proto.GetServerInfoReply
	err := dm.client.Request(&proto.GetServerInfo{}, &reply)
	if err != nil {
		return "", err
	}
	return reply.DefaultSinkName, nil
}

// checkDefaultSinkChanged checks if the default sink has changed and notifies callbacks
func (dm *DeviceMonitor) checkDefaultSinkChanged() {
	newSink, err := dm.getDefaultSinkName()
	if err != nil {
		fmt.Printf("Error getting default sink: %v\n", err)
		return
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()

	if newSink != dm.currentSink {
		oldSink := dm.currentSink
		dm.currentSink = newSink
		fmt.Printf("Default sink changed: %s -> %s\n", oldSink, newSink)

		// Notify all callbacks
		for _, cb := range dm.callbacks {
			go cb("sink_changed")
		}
	}
}

// OnDefaultDeviceChanged registers a callback to be called when the default device changes
// The callback receives the event type ("sink_changed")
func (dm *DeviceMonitor) OnDefaultDeviceChanged(callback func(eventType string)) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.callbacks = append(dm.callbacks, callback)
}

// Close stops monitoring and closes the connection
func (dm *DeviceMonitor) Close() {
	dm.mu.Lock()
	dm.running = false
	dm.mu.Unlock()

	close(dm.stopCh)

	if dm.conn != nil {
		dm.conn.Close()
	}
}
