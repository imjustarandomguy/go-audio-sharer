package audio

import (
	"sync"
	"sync/atomic"
)

const subscriberQueueSize = 8 // ~80 ms at 10 ms/block; keeps slow clients live

type pooledBuf struct {
	data []byte
	refs atomic.Int32
	pool *sync.Pool
}

// CapturedBlock holds a chunk of captured PCM audio. Subscribers should call
// Release once they are done reading Data so pooled buffers can be reused.
type CapturedBlock struct {
	Data []byte
	buf  *pooledBuf
}

// Release decrements the reference count on a pooled capture buffer and returns
// it to the pool when no consumers remain. Safe to call on unpooled blocks.
func (c CapturedBlock) Release() {
	if c.buf == nil {
		return
	}
	if c.buf.refs.Add(-1) == 0 && c.buf.pool != nil {
		c.buf.pool.Put(c.buf)
	}
}

type Broadcaster struct {
	mu          sync.Mutex
	lifecycleMu sync.Mutex
	subscribers map[chan CapturedBlock]struct{}
	onStart     func()
	onStop      func()
	active      bool
	dropped     atomic.Uint64
}

func NewBroadcaster(onStart, onStop func()) *Broadcaster {
	return &Broadcaster{
		subscribers: make(map[chan CapturedBlock]struct{}),
		onStart:     onStart,
		onStop:      onStop,
	}
}

func (b *Broadcaster) SubscribeWithChan() chan CapturedBlock {
	b.mu.Lock()
	ch := make(chan CapturedBlock, subscriberQueueSize)
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
		// Release any queued pooled blocks that will never be consumed.
		for blk := range ch {
			blk.Release()
		}
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

// TakeDropped returns and resets the number of blocks dropped due to slow
// subscribers since the previous call.
func (b *Broadcaster) TakeDropped() uint64 {
	return b.dropped.Swap(0)
}

// Broadcast fans out block to all subscribers without blocking. If a
// subscriber's queue is full, the oldest queued block is dropped so the
// subscriber stays on live audio rather than falling behind.
//
// Broadcast takes ownership of the caller's initial reference on pooled blocks.
func (b *Broadcaster) Broadcast(block CapturedBlock) {
	b.mu.Lock()
	for ch := range b.subscribers {
		if block.buf != nil {
			block.buf.refs.Add(1)
		}
		select {
		case ch <- block:
		default:
			// Drop oldest queued block to make room for the newest live audio.
			select {
			case old := <-ch:
				old.Release()
			default:
			}
			b.dropped.Add(1)
			select {
			case ch <- block:
			default:
				block.Release()
			}
		}
	}
	b.mu.Unlock()

	// Release Broadcast's own initial reference.
	block.Release()
}
