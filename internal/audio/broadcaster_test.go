package audio

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBroadcasterLifecycle(t *testing.T) {
	var started, stopped atomic.Int32
	var startWg, stopWg sync.WaitGroup
	startWg.Add(1)
	stopWg.Add(1)

	b := NewBroadcaster(
		func() {
			started.Add(1)
			startWg.Done()
		},
		func() {
			stopped.Add(1)
			stopWg.Done()
		},
	)

	// First subscriber triggers onStart
	if b.HasSubscribers() {
		t.Fatal("expected HasSubscribers() == false initially")
	}
	ch1 := b.SubscribeWithChan()
	if !b.HasSubscribers() {
		t.Fatal("expected HasSubscribers() == true after subscribe")
	}
	startWg.Wait()
	if started.Load() != 1 {
		t.Fatalf("onStart count = %d, want 1", started.Load())
	}

	// Second subscriber does NOT trigger onStart again
	ch2 := b.SubscribeWithChan()
	if started.Load() != 1 {
		t.Fatalf("onStart count after 2nd sub = %d, want 1", started.Load())
	}

	// Broadcast sends to both
	testData := []byte("audio-sample")
	now := time.Now()
	b.Broadcast(CapturedBlock{Data: testData, Time: now})

	select {
	case block := <-ch1:
		if string(block.Data) != string(testData) {
			t.Fatalf("ch1 got %s, want %s", block.Data, testData)
		}
	case <-time.After(time.Second):
		t.Fatal("ch1 timed out")
	}

	select {
	case block := <-ch2:
		if string(block.Data) != string(testData) {
			t.Fatalf("ch2 got %s, want %s", block.Data, testData)
		}
	case <-time.After(time.Second):
		t.Fatal("ch2 timed out")
	}

	// Unsubscribe first: onStop should not trigger yet
	b.UnsubscribeWithChan(ch1)
	if stopped.Load() != 0 {
		t.Fatalf("onStop triggered prematurely, count = %d", stopped.Load())
	}

	// Unsubscribe second: onStop triggers
	b.UnsubscribeWithChan(ch2)
	if b.HasSubscribers() {
		t.Fatal("expected HasSubscribers() == false after last unsubscribe")
	}
	stopWg.Wait()
	if stopped.Load() != 1 {
		t.Fatalf("onStop count = %d, want 1", stopped.Load())
	}
}

func TestBroadcasterNonBlockingDrop(t *testing.T) {
	b := NewBroadcaster(nil, nil)
	ch := b.SubscribeWithChan()
	defer b.UnsubscribeWithChan(ch)

	// Capacity of ch is 100. Fill it up to 100 blocks.
	for i := 0; i < 100; i++ {
		b.Broadcast(CapturedBlock{Data: []byte{byte(i)}})
	}

	// 101st broadcast should not block and should simply drop
	done := make(chan struct{})
	go func() {
		b.Broadcast(CapturedBlock{Data: []byte("overflow")})
		close(done)
	}()

	select {
	case <-done:
		// success: did not block
	case <-time.After(time.Second):
		t.Fatal("Broadcast blocked on full channel")
	}
}
