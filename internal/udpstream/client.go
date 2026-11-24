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
	conn     *net.UDPConn
	clientID uint64
	format   Format

	keepaliveInterval time.Duration
	readTimeout       time.Duration

	// Optional first audio packet if TypeAudio arrived during Handshake.
	initialSeq     uint32
	initialPayload []byte

	packetsLost atomic.Uint64
	outOfOrder  atomic.Uint64
	byeOnce     sync.Once
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

	// Unblock Read immediately if ctx is cancelled during handshake.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.SetReadDeadline(time.Now())
		case <-done:
		}
	}()

	for {
		if ctx.Err() != nil {
			return nil, Format{}, ctx.Err()
		}
		if _, err := conn.Write(hello[:]); err != nil {
			return nil, Format{}, err
		}

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
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					if time.Now().After(deadline) {
						return nil, Format{}, context.DeadlineExceeded
					}
					break // resend HELLO
				}
				return nil, Format{}, err
			}

			pktType, seq, f, payload, ok := DecodeDataPacket(buf[:n])
			if !ok {
				continue
			}

			sess := &Session{
				conn:              conn,
				clientID:          clientID,
				format:            f,
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

func (s *Session) sendBye() {
	s.byeOnce.Do(func() {
		var bye [ControlSize]byte
		EncodeControl(bye[:], TypeBye, s.clientID)
		_ = s.conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		_, _ = s.conn.Write(bye[:])
	})
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
		hasSeq  bool
		lastSeq uint32
	)
	if len(s.initialPayload) > 0 {
		hasSeq = true
		lastSeq = s.initialSeq
		if err := onAudio(s.initialPayload); err != nil {
			s.sendBye()
			return err
		}
		s.initialPayload = nil
	}

	loopDone := make(chan struct{})
	var wg sync.WaitGroup

	// Keepalive loop: sends HELLO periodically so the server keeps streaming and
	// replies with ACK (keeping the read deadline alive even during silence).
	wg.Add(1)
	go func() {
		defer wg.Done()
		var hello [ControlSize]byte
		EncodeControl(hello[:], TypeHello, s.clientID)
		t := time.NewTicker(s.keepaliveInterval)
		defer t.Stop()
		for {
			select {
			case <-loopDone:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				_, _ = s.conn.Write(hello[:])
			}
		}
	}()

	// Cancellation watcher: sends BYE and closes the socket immediately on ctx.Done()
	// so the blocking Read exits without racing SetReadDeadline.
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			s.sendBye()
			_ = s.conn.Close()
		case <-loopDone:
		}
	}()

	defer func() {
		close(loopDone)
		s.sendBye()
		wg.Wait()
	}()

	buf := make([]byte, 2048)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_ = s.conn.SetReadDeadline(time.Now().Add(s.readTimeout))
		n, err := s.conn.Read(buf)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}

		pktType, seq, f, payload, ok := DecodeDataPacket(buf[:n])
		if !ok {
			continue
		}
		if f != s.format {
			return ErrFormatChanged
		}
		if pktType == TypeAck || len(payload) == 0 {
			continue
		}

		if hasSeq {
			diff := int32(seq - lastSeq)
			if diff <= 0 {
				// Stale, duplicate, or out-of-order packet: drop to keep audio live.
				s.outOfOrder.Add(1)
				continue
			}
			if diff > 1 {
				s.packetsLost.Add(uint64(diff - 1))
			}
		}
		hasSeq = true
		lastSeq = seq

		if err := onAudio(payload); err != nil {
			return err
		}
	}
}
