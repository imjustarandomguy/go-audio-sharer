package audio

import (
	"sync"
	"time"
)

type CapturedBlock struct {
	Data []byte
	Time time.Time
}

type Broadcaster struct {
	mu          sync.Mutex
	lifecycleMu sync.Mutex
	subscribers map[chan<- CapturedBlock]struct{}
	onStart     func()
	onStop      func()
	active      bool
}

func NewBroadcaster(onStart, onStop func()) *Broadcaster {
	return &Broadcaster{
		subscribers: make(map[chan<- CapturedBlock]struct{}),
		onStart:     onStart,
		onStop:      onStop,
	}
}

func (b *Broadcaster) SubscribeWithChan() chan CapturedBlock {
	b.mu.Lock()
	ch := make(chan CapturedBlock, 100)
	b.subscribers[ch] = struct{}{}
	shouldReconcile := len(b.subscribers) == 1
	b.mu.Unlock()

	if shouldReconcile {
		go b.reconcile()
	}

	return ch
}

func (b *Broadcaster) UnsubscribeWithChan(ch chan CapturedBlock) {
	b.mu.Lock()
	if _, ok := b.subscribers[ch]; ok {
		delete(b.subscribers, ch)
		close(ch)
	}
	shouldReconcile := len(b.subscribers) == 0
	b.mu.Unlock()

	if shouldReconcile {
		go b.reconcile()
	}
}

func (b *Broadcaster) reconcile() {
	b.lifecycleMu.Lock()
	defer b.lifecycleMu.Unlock()

	b.mu.Lock()
	wantActive := len(b.subscribers) > 0
	b.mu.Unlock()

	if wantActive && !b.active {
		b.active = true
		if b.onStart != nil {
			b.onStart()
		}
	} else if !wantActive && b.active {
		b.active = false
		if b.onStop != nil {
			b.onStop()
		}
	}
}

// HasSubscribers reports whether at least one subscriber is currently registered.
func (b *Broadcaster) HasSubscribers() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subscribers) > 0
}

func (b *Broadcaster) Broadcast(block CapturedBlock) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for ch := range b.subscribers {
		select {
		case ch <- block:
		default:
			// fast non-blocking drop
		}
	}
}
