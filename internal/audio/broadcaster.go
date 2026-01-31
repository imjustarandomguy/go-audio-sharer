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
	defer b.mu.Unlock()

	ch := make(chan CapturedBlock, 100)
	b.subscribers[ch] = struct{}{}

	if len(b.subscribers) == 1 && b.onStart != nil {
		b.active = true
		go b.onStart()
	}

	return ch
}

func (b *Broadcaster) UnsubscribeWithChan(ch chan CapturedBlock) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.subscribers[ch]; ok {
		delete(b.subscribers, ch)
		close(ch)
	}

	if len(b.subscribers) == 0 && b.active && b.onStop != nil {
		b.active = false
		go b.onStop()
	}
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
