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
	b.Broadcast(CapturedBlock{Data: testData})

	select {
	case block := <-ch1:
		if string(block.Data) != string(testData) {
			t.Fatalf("ch1 got %s, want %s", block.Data, testData)
		}
		block.Release()
	case <-time.After(time.Second):
		t.Fatal("ch1 timed out")
	}

	select {
	case block := <-ch2:
		if string(block.Data) != string(testData) {
			t.Fatalf("ch2 got %s, want %s", block.Data, testData)
		}
		block.Release()
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

func TestBroadcasterDropOldestOnFull(t *testing.T) {
	b := NewBroadcaster(nil, nil)
	ch := b.SubscribeWithChan()
	defer b.UnsubscribeWithChan(ch)

	total := subscriberQueueSize + 5
	for i := 0; i < total; i++ {
		b.Broadcast(CapturedBlock{Data: []byte{byte(i)}})
	}

	if dropped := b.TakeDropped(); dropped != 5 {
		t.Fatalf("dropped = %d, want 5", dropped)
	}

	// Subscriber should receive the newest subscriberQueueSize blocks (5..total-1).
	for want := 5; want < total; want++ {
		select {
		case blk := <-ch:
			if len(blk.Data) != 1 || blk.Data[0] != byte(want) {
				t.Fatalf("got block %v, want [%d]", blk.Data, want)
			}
			blk.Release()
		default:
			t.Fatalf("expected queued block %d, channel was empty", want)
		}
	}
}

func TestBroadcasterPooledBlocksConcurrent(t *testing.T) {
	r := &Recorder{
		broadcaster: NewBroadcaster(nil, nil),
	}

	ch1 := r.broadcaster.SubscribeWithChan()
	ch2 := r.broadcaster.SubscribeWithChan()

	const count = 200
	var wg sync.WaitGroup
	wg.Add(2)

	consume := func(ch chan CapturedBlock) {
		defer wg.Done()
		for blk := range ch {
			// Touch payload to let -race detect any premature buffer reuse.
			if len(blk.Data) != 1920 || blk.Data[0] != blk.Data[1919] {
				t.Errorf("corrupted pooled block: len=%d first=%d last=%d", len(blk.Data), blk.Data[0], blk.Data[len(blk.Data)-1])
			}
			blk.Release()
		}
	}
	go consume(ch1)
	go consume(ch2)

	raw := make([]byte, 1920)
	for i := 0; i < count; i++ {
		v := byte(i)
		raw[0] = v
		raw[1919] = v
		r.broadcaster.Broadcast(r.allocBlock(raw))
	}

	r.broadcaster.UnsubscribeWithChan(ch1)
	r.broadcaster.UnsubscribeWithChan(ch2)
	wg.Wait()
}
