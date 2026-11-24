package udpstream

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultKeepaliveInterval = 2 * time.Second
	defaultReadTimeout       = 5 * time.Second
	handshakeRetryInterval   = 400 * time.Millisecond
	resyncConsecutiveStale   = 5
)

// ErrFormatChanged is returned by Session.Run if the server begins streaming
// with a different sample rate, channel count, or bit depth.
var ErrFormatChanged = errors.New("audio format changed")

// ClientStats reports UDP transport anomalies since the previous TakeStats call.
type ClientStats struct {
	PacketsLost uint64
	OutOfOrder  uint64
}

// Session manages one connected UDP client streaming session.
type Session struct {
	conn          *net.UDPConn
	clientID      uint64
	format        Format
	epoch         uint16
	handshakeTime time.Time

	keepaliveInterval time.Duration
	readTimeout       time.Duration

	// Optional first audio packet if TypeAudio arrived during Handshake.
	initialSeq     uint32
	initialPayload []byte

	packetsLost atomic.Uint64
	outOfOrder  atomic.Uint64

	ctrlMu  sync.Mutex
	byeSent bool
}

func randomClientID() uint64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	id := binary.LittleEndian.Uint64(b[:])
	if id == 0 {
		id = 1
	}
	return id
}

// Handshake sends HELLO datagrams over conn until the server replies with an
// ACK or AUDIO packet containing the stream Format, or until timeout/ctx expires.
func Handshake(ctx context.Context, conn *net.UDPConn, timeout time.Duration) (*Session, Format, error) {
	_ = conn.SetReadBuffer(socketBufferBytes)
	_ = conn.SetWriteBuffer(socketBufferBytes)

	clientID := randomClientID()
	var hello [ControlSize]byte
	EncodeControl(hello[:], TypeHello, clientID)

	deadline := time.Now().Add(timeout)
	buf := make([]byte, 2048)

	// Unblock Read immediately if ctx is cancelled during handshake, and always
	// join this goroutine before Handshake returns so it cannot touch conn later.
	done := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			_ = conn.SetReadDeadline(time.Now())
		case <-done:
		}
	}()
	defer func() {
		close(done)
		<-watcherDone
	}()

	for {
		if ctx.Err() != nil {
			return nil, Format{}, ctx.Err()
		}
		_, _ = conn.Write(hello[:])

		waitUntil := time.Now().Add(handshakeRetryInterval)
		if waitUntil.After(deadline) {
			waitUntil = deadline
		}
		_ = conn.SetReadDeadline(waitUntil)

		for {
			n, err := conn.Read(buf)
			if err != nil {
				if ctx.Err() != nil {
					return nil, Format{}, ctx.Err()
				}
				now := time.Now()
				if !now.Before(deadline) {
					return nil, Format{}, context.DeadlineExceeded
				}
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					break // retry HELLO
				}
				// On connected UDP sockets, ICMP Port Unreachable while a server is
				// starting/restarting yields WSAECONNRESET (Windows) or ECONNREFUSED
				// (Linux). Wait briefly and keep retrying until the handshake deadline.
				sleep := time.Until(waitUntil)
				if sleep > 0 {
					timer := time.NewTimer(sleep)
					select {
					case <-ctx.Done():
						timer.Stop()
						return nil, Format{}, ctx.Err()
					case <-timer.C:
					}
				}
				break
			}

			pktType, seq, epoch, f, payload, ok := DecodeDataPacket(buf[:n])
			if !ok {
				continue
			}

			// Clear handshake read deadline before returning control of conn.
			_ = conn.SetReadDeadline(time.Time{})

			sess := &Session{
				conn:              conn,
				clientID:          clientID,
				format:            f,
				epoch:             epoch,
				handshakeTime:     time.Now(),
				keepaliveInterval: defaultKeepaliveInterval,
				readTimeout:       defaultReadTimeout,
			}
			if pktType == TypeAudio && len(payload) > 0 {
				sess.initialSeq = seq
				sess.initialPayload = append([]byte(nil), payload...)
			}
			return sess, f, nil
		}
	}
}

func (s *Session) sendHello() {
	s.ctrlMu.Lock()
	defer s.ctrlMu.Unlock()
	if s.byeSent {
		return
	}
	var hello [ControlSize]byte
	EncodeControl(hello[:], TypeHello, s.clientID)
	_, _ = s.conn.Write(hello[:])
}

// Close sends a best-effort BYE packet once and suppresses any further HELLO
// keepalives so a disconnected client is never re-registered on the server.
func (s *Session) Close() {
	s.ctrlMu.Lock()
	if s.byeSent {
		s.ctrlMu.Unlock()
		return
	}
	s.byeSent = true
	var bye [ControlSize]byte
	EncodeControl(bye[:], TypeBye, s.clientID)
	_ = s.conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	_, _ = s.conn.Write(bye[:])
	s.ctrlMu.Unlock()
}

// TakeStats returns and resets UDP packet-loss and out-of-order counters.
func (s *Session) TakeStats() ClientStats {
	return ClientStats{
		PacketsLost: s.packetsLost.Swap(0),
		OutOfOrder:  s.outOfOrder.Swap(0),
	}
}

// Run receives live UDP audio packets until ctx is cancelled, the server stops
// responding, the audio format changes, or onAudio returns an error.
// The receive loop performs zero heap allocations per packet.
func (s *Session) Run(ctx context.Context, onAudio func(pcm []byte) error) error {
	var (
		hasSeq       bool
		lastSeq      uint32
		staleRun     int
		lastStaleSeq uint32
	)
	if len(s.initialPayload) > 0 {
		hasSeq = true
		lastSeq = s.initialSeq
		if err := onAudio(s.initialPayload); err != nil {
			s.Close()
			return err
		}
		s.initialPayload = nil
	}

	// If opening the audio playback device took a while after Handshake,
	// immediately refresh the server peer lease before waiting a full interval.
	if time.Since(s.handshakeTime) > s.keepaliveInterval/2 {
		s.sendHello()
	}

	loopDone := make(chan struct{})
	var wg sync.WaitGroup

	// Keepalive loop: sends HELLO periodically so the server keeps streaming and
	// replies with ACK (keeping the read deadline alive even during silence).
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(s.keepaliveInterval)
		defer t.Stop()
		for {
			select {
			case <-loopDone:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				s.sendHello()
			}
		}
	}()

	// Cancellation watcher: sends BYE (serialized against keepalive HELLO) and
	// closes the socket immediately on ctx.Done() so Read exits cleanly.
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			s.Close()
			_ = s.conn.Close()
		case <-loopDone:
		}
	}()

	defer func() {
		close(loopDone)
		s.Close()
		wg.Wait()
	}()

	buf := make([]byte, 2048)
	var nextDeadline time.Time
	refreshMargin := s.readTimeout / 2

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		now := time.Now()
		if nextDeadline.IsZero() || now.Add(refreshMargin).After(nextDeadline) {
			nextDeadline = now.Add(s.readTimeout)
			_ = s.conn.SetReadDeadline(nextDeadline)
		}

		n, err := s.conn.Read(buf)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}

		pktType, seq, epoch, f, payload, ok := DecodeDataPacket(buf[:n])
		if !ok {
			continue
		}
		if f != s.format {
			return ErrFormatChanged
		}
		// Detect server restart via boot epoch change and seamlessly resync seq.
		if epoch != s.epoch {
			s.epoch = epoch
			hasSeq = false
			staleRun = 0
		}
		if pktType == TypeAck || len(payload) == 0 {
			continue
		}

		if hasSeq {
			diff := int32(seq - lastSeq)
			if diff <= 0 {
				// Belt-and-suspenders resync if a server restarted with the same
				// 16-bit epoch: consecutive monotonically increasing packets with
				// diff <= 0 indicate a new sequence baseline.
				if staleRun > 0 && int32(seq-lastStaleSeq) > 0 {
					staleRun++
				} else {
					staleRun = 1
				}
				lastStaleSeq = seq
				if staleRun < resyncConsecutiveStale {
					s.outOfOrder.Add(1)
					continue
				}
				// Resync accepted: fall through and treat seq as the new baseline.
				staleRun = 0
			} else {
				staleRun = 0
				if diff > 1 {
					s.packetsLost.Add(uint64(diff - 1))
				}
			}
		}
		hasSeq = true
		lastSeq = seq

		if err := onAudio(payload); err != nil {
			return err
		}
	}
}
