package udpstream

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-audio-sharer/internal/audio"
	"go-audio-sharer/internal/server"
)

type mockAudioSource struct {
	b       *audio.Broadcaster
	started atomic.Int32
	stopped atomic.Int32
}

func newMockAudioSource() *mockAudioSource {
	m := &mockAudioSource{}
	m.b = audio.NewBroadcaster(
		func() { m.started.Add(1) },
		func() { m.stopped.Add(1) },
	)
	return m
}

func (m *mockAudioSource) Subscribe() chan audio.CapturedBlock     { return m.b.SubscribeWithChan() }
func (m *mockAudioSource) Unsubscribe(ch chan audio.CapturedBlock) { m.b.UnsubscribeWithChan(ch) }
func (m *mockAudioSource) Push(data []byte)                        { m.b.Broadcast(audio.CapturedBlock{Data: data}) }
func (m *mockAudioSource) HasSubscribers() bool                    { return m.b.HasSubscribers() }

func mustNewServer(t *testing.T, conn *net.UDPConn, src AudioSource, f Format, reg *server.ConnRegistry) *Server {
	t.Helper()
	srv, err := NewServer(conn, src, f, reg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return srv
}

func TestPacketCodecRoundTrip(t *testing.T) {
	var ctrl [ControlSize]byte
	n := EncodeControl(ctrl[:], TypeHello, 0x123456789abcdef0)
	if n != ControlSize {
		t.Fatalf("EncodeControl = %d, want %d", n, ControlSize)
	}
	pt, cid, ok := DecodeControl(ctrl[:])
	if !ok || pt != TypeHello || cid != 0x123456789abcdef0 {
		t.Fatalf("DecodeControl = (%d, %x, %v), want (%d, 123456789abcdef0, true)", pt, cid, ok, TypeHello)
	}
	if _, _, ok := DecodeControl([]byte("bad")); ok {
		t.Fatal("expected invalid control packet to be rejected")
	}

	f := Format{SampleRate: 48000, Channels: 2, BitsPerSample: 16}
	if got := f.ChunkBytes(); got != 960 {
		t.Fatalf("48kHz stereo 16-bit ChunkBytes = %d, want 960 (5 ms)", got)
	}
	fHigh := Format{SampleRate: 192000, Channels: 2, BitsPerSample: 16}
	if got := fHigh.ChunkBytes(); got > MaxPayloadBytes || got%fHigh.FrameBytes() != 0 {
		t.Fatalf("192kHz ChunkBytes = %d, must be <= %d and frame-aligned", got, MaxPayloadBytes)
	}

	buf := make([]byte, DataHeaderSize+8)
	EncodeDataHeader(buf, TypeAudio, 42, 0xBEEF, f)
	copy(buf[DataHeaderSize:], []byte("12345678"))
	dpt, seq, epoch, df, payload, ok := DecodeDataPacket(buf)
	if !ok || dpt != TypeAudio || seq != 42 || epoch != 0xBEEF || df != f || string(payload) != "12345678" {
		t.Fatalf("DecodeDataPacket = (%d, %d, %x, %+v, %q, %v)", dpt, seq, epoch, df, payload, ok)
	}

	// Invalid format (0 channels) must be rejected.
	buf[12] = 0
	if _, _, _, _, _, ok := DecodeDataPacket(buf); ok {
		t.Fatal("expected invalid format (0 channels) to be rejected")
	}
}

func TestServerRejectsInvalidFormat(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer conn.Close()

	if _, err := NewServer(conn, newMockAudioSource(), Format{SampleRate: 0, Channels: 2, BitsPerSample: 16}, nil); err == nil {
		t.Fatal("expected error for invalid format")
	}
}

func TestServerCloseBeforeServeDoesNotRace(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	srv := mustNewServer(t, conn, newMockAudioSource(), Format{SampleRate: 48000, Channels: 2, BitsPerSample: 16}, nil)
	srv.Close()
	if err := srv.Serve(); err != nil {
		t.Fatalf("Serve after Close returned %v, want nil", err)
	}
}

func TestServerClientEndToEndAndMTUChunking(t *testing.T) {
	srvConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	src := newMockAudioSource()
	reg := server.NewConnRegistry()
	fmtCfg := Format{SampleRate: 48000, Channels: 2, BitsPerSample: 16}

	srv := mustNewServer(t, srvConn, src, fmtCfg, reg)
	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		_ = srv.Serve()
	}()
	defer func() {
		srv.Close()
		<-srvDone
	}()

	cliConn, err := net.DialUDP("udp4", nil, srvConn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer cliConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sess, gotFmt, err := Handshake(ctx, cliConn, 2*time.Second)
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	if gotFmt != fmtCfg {
		t.Fatalf("Handshake format = %+v, want %+v", gotFmt, fmtCfg)
	}
	if reg.Count() != 1 {
		t.Fatalf("registry count = %d, want 1", reg.Count())
	}

	// Send a 10 ms (1920-byte) block; server must slice into two 960-byte single-MTU packets.
	wantBlock := make([]byte, 1920)
	for i := range wantBlock {
		wantBlock[i] = byte(i & 0xFF)
	}

	var (
		mu       sync.Mutex
		received []byte
		chunks   int
		gotAll   = make(chan struct{})
	)
	runDone := make(chan error, 1)
	go func() {
		runDone <- sess.Run(ctx, func(pcm []byte) error {
			if len(pcm) > MaxPayloadBytes {
				t.Errorf("received oversized UDP payload: %d > %d", len(pcm), MaxPayloadBytes)
			}
			mu.Lock()
			received = append(received, pcm...)
			chunks++
			done := len(received) >= len(wantBlock)
			mu.Unlock()
			if done {
				select {
				case <-gotAll:
				default:
					close(gotAll)
				}
			}
			return nil
		})
	}()

	// Wait briefly for server's sendLoop to subscribe before pushing.
	deadline := time.Now().Add(time.Second)
	for !src.HasSubscribers() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	src.Push(wantBlock)

	select {
	case <-gotAll:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for UDP audio packets")
	}

	mu.Lock()
	if chunks != 2 {
		t.Errorf("chunk count = %d, want 2 (two 960-byte packets)", chunks)
	}
	if !bytes.Equal(received, wantBlock) {
		t.Errorf("received PCM did not match sent 1920-byte block")
	}
	mu.Unlock()

	// Cancel client context -> must send BYE, remove from registry, and unsubscribe.
	cancel()
	<-runDone

	deadline = time.Now().Add(time.Second)
	for (reg.Count() != 0 || src.HasSubscribers()) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if reg.Count() != 0 {
		t.Fatalf("registry count after BYE = %d, want 0", reg.Count())
	}
	if src.HasSubscribers() {
		t.Fatal("expected server to unsubscribe from AudioSource after last client BYE")
	}
}

func TestClientDropsStaleAndTracksPacketLoss(t *testing.T) {
	srvConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer srvConn.Close()

	cliConn, err := net.DialUDP("udp4", nil, srvConn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer cliConn.Close()

	f := Format{SampleRate: 48000, Channels: 2, BitsPerSample: 16}

	go func() {
		buf := make([]byte, 64)
		_, raddr, err := srvConn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		var hdr [DataHeaderSize]byte
		EncodeDataHeader(hdr[:], TypeAck, 0, 7, f)
		_, _ = srvConn.WriteToUDP(hdr[:], raddr)

		sendPkt := func(seq uint32, tag byte) {
			p := make([]byte, DataHeaderSize+4)
			EncodeDataHeader(p, TypeAudio, seq, 7, f)
			p[DataHeaderSize] = tag
			_, _ = srvConn.WriteToUDP(p, raddr)
		}

		time.Sleep(20 * time.Millisecond)
		sendPkt(0xFFFFFFFE, 'A') // first
		sendPkt(0xFFFFFFFF, 'B') // +1
		sendPkt(0xFFFFFFFF, 'X') // duplicate -> drop (outOfOrder=1)
		sendPkt(0xFFFFFFFE, 'Y') // stale -> drop (outOfOrder=2)
		sendPkt(0x00000000, 'C') // wrap-around +1
		sendPkt(0x00000004, 'D') // gap of 3 (1,2,3 lost -> packetsLost=3)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sess, _, err := Handshake(ctx, cliConn, 2*time.Second)
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}

	var gotTags []byte
	err = sess.Run(ctx, func(pcm []byte) error {
		gotTags = append(gotTags, pcm[0])
		if len(gotTags) == 4 {
			cancel()
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	if string(gotTags) != "ABCD" {
		t.Fatalf("got tags %q, want %q", string(gotTags), "ABCD")
	}
	st := sess.TakeStats()
	if st.OutOfOrder != 2 || st.PacketsLost != 3 {
		t.Fatalf("stats = %+v, want OutOfOrder=2 PacketsLost=3", st)
	}
}

func TestClientResyncsOnServerRestartEpochChange(t *testing.T) {
	srvConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer srvConn.Close()

	cliConn, err := net.DialUDP("udp4", nil, srvConn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer cliConn.Close()

	f := Format{SampleRate: 48000, Channels: 2, BitsPerSample: 16}

	go func() {
		buf := make([]byte, 64)
		_, raddr, err := srvConn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		var hdr [DataHeaderSize]byte
		EncodeDataHeader(hdr[:], TypeAck, 0, 10, f)
		_, _ = srvConn.WriteToUDP(hdr[:], raddr)

		sendPkt := func(seq uint32, epoch uint16, tag byte) {
			p := make([]byte, DataHeaderSize+4)
			EncodeDataHeader(p, TypeAudio, seq, epoch, f)
			p[DataHeaderSize] = tag
			_, _ = srvConn.WriteToUDP(p, raddr)
		}

		time.Sleep(20 * time.Millisecond)
		// Server running at seq=120000 with epoch=10.
		sendPkt(120000, 10, 'A')
		// Server restarts with new epoch=11 and seq reset to 1, 2.
		sendPkt(1, 11, 'B')
		sendPkt(2, 11, 'C')
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sess, _, err := Handshake(ctx, cliConn, time.Second)
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	var gotTags []byte
	err = sess.Run(ctx, func(pcm []byte) error {
		gotTags = append(gotTags, pcm[0])
		if len(gotTags) == 3 {
			cancel()
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	if string(gotTags) != "ABC" {
		t.Fatalf("got tags %q, want ABC", string(gotTags))
	}
	if st := sess.TakeStats(); st.OutOfOrder != 0 {
		t.Fatalf("expected 0 out-of-order drops on epoch change, got %d", st.OutOfOrder)
	}
}

func TestClientResyncsOnSameEpochMonotonicReset(t *testing.T) {
	srvConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer srvConn.Close()

	cliConn, err := net.DialUDP("udp4", nil, srvConn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer cliConn.Close()

	f := Format{SampleRate: 48000, Channels: 2, BitsPerSample: 16}

	go func() {
		buf := make([]byte, 64)
		_, raddr, err := srvConn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		var hdr [DataHeaderSize]byte
		EncodeDataHeader(hdr[:], TypeAck, 0, 42, f)
		_, _ = srvConn.WriteToUDP(hdr[:], raddr)

		sendPkt := func(seq uint32, tag byte) {
			p := make([]byte, DataHeaderSize+4)
			EncodeDataHeader(p, TypeAudio, seq, 42, f)
			p[DataHeaderSize] = tag
			_, _ = srvConn.WriteToUDP(p, raddr)
		}

		time.Sleep(20 * time.Millisecond)
		sendPkt(120000, 'A')
		// Same epoch=42 but seq resets to 1..6; after 5 consecutive monotonic packets
		// the belt-and-suspenders resync must recover automatically.
		for i := uint32(1); i <= 6; i++ {
			sendPkt(i, byte('0'+i))
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sess, _, err := Handshake(ctx, cliConn, time.Second)
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	var gotTags []byte
	err = sess.Run(ctx, func(pcm []byte) error {
		gotTags = append(gotTags, pcm[0])
		if len(gotTags) == 3 {
			cancel()
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	if string(gotTags) != "A56" {
		t.Fatalf("got tags %q, want A56 (resync on 5th monotonic packet)", string(gotTags))
	}
}

func TestHandshakeRetriesPastICMPConnectionRefused(t *testing.T) {
	// Grab an ephemeral port and close it immediately so the first HELLO gets
	// ICMP Port Unreachable (ECONNREFUSED on Linux / WSAECONNRESET on Windows).
	probe, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP probe: %v", err)
	}
	addr := probe.LocalAddr().(*net.UDPAddr)
	probe.Close()

	cliConn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer cliConn.Close()

	// Start the server 150 ms later on the same port.
	srvReady := make(chan *Server, 1)
	go func() {
		time.Sleep(150 * time.Millisecond)
		srvConn, err := net.ListenUDP("udp4", addr)
		if err != nil {
			return
		}
		srv := mustNewServer(t, srvConn, newMockAudioSource(), Format{SampleRate: 48000, Channels: 2, BitsPerSample: 16}, nil)
		srvReady <- srv
		_ = srv.Serve()
	}()
	defer func() {
		select {
		case srv := <-srvReady:
			srv.Close()
		default:
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sess, f, err := Handshake(ctx, cliConn, 2*time.Second)
	if err != nil {
		t.Fatalf("Handshake failed instead of retrying past ICMP refused: %v", err)
	}
	sess.Close()
	if f.SampleRate != 48000 {
		t.Fatalf("format = %+v, want 48000 Hz", f)
	}
}

func TestClientDetectsFormatChange(t *testing.T) {
	srvConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer srvConn.Close()

	cliConn, err := net.DialUDP("udp4", nil, srvConn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer cliConn.Close()

	f1 := Format{SampleRate: 48000, Channels: 2, BitsPerSample: 16}
	f2 := Format{SampleRate: 44100, Channels: 2, BitsPerSample: 16}

	go func() {
		buf := make([]byte, 64)
		_, raddr, err := srvConn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		var pkt [DataHeaderSize + 4]byte
		EncodeDataHeader(pkt[:], TypeAck, 0, 1, f1)
		_, _ = srvConn.WriteToUDP(pkt[:DataHeaderSize], raddr)
		time.Sleep(20 * time.Millisecond)
		EncodeDataHeader(pkt[:], TypeAudio, 1, 1, f2)
		_, _ = srvConn.WriteToUDP(pkt[:], raddr)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sess, _, err := Handshake(ctx, cliConn, time.Second)
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	err = sess.Run(ctx, func(pcm []byte) error { return nil })
	if !errors.Is(err, ErrFormatChanged) {
		t.Fatalf("Run error = %v, want ErrFormatChanged", err)
	}
}

func TestServerReapsTimedOutPeer(t *testing.T) {
	srvConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	src := newMockAudioSource()
	reg := server.NewConnRegistry()
	srv := mustNewServer(t, srvConn, src, Format{SampleRate: 48000, Channels: 2, BitsPerSample: 16}, reg)
	srv.peerTimeout = 80 * time.Millisecond
	srv.reapInterval = 20 * time.Millisecond

	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		_ = srv.Serve()
	}()
	defer func() {
		srv.Close()
		<-srvDone
	}()

	cliConn, err := net.DialUDP("udp4", nil, srvConn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer cliConn.Close()

	// Send one HELLO and then go silent (no BYE, no keepalives).
	var hello [ControlSize]byte
	EncodeControl(hello[:], TypeHello, 99)
	if _, err := cliConn.Write(hello[:]); err != nil {
		t.Fatalf("Write HELLO: %v", err)
	}

	// Wait for registration.
	deadline := time.Now().Add(time.Second)
	for reg.Count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if reg.Count() != 1 {
		t.Fatalf("expected peer registered, count=%d", reg.Count())
	}

	// Wait for expiry reaper to remove peer and unsubscribe.
	deadline = time.Now().Add(time.Second)
	for (reg.Count() != 0 || src.HasSubscribers()) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if reg.Count() != 0 || src.HasSubscribers() {
		t.Fatalf("peer not reaped: reg=%d hasSubs=%v", reg.Count(), src.HasSubscribers())
	}
}

func TestClientIdleKeepaliveSurvivesSilence(t *testing.T) {
	srvConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	src := newMockAudioSource()
	srv := mustNewServer(t, srvConn, src, Format{SampleRate: 48000, Channels: 2, BitsPerSample: 16}, nil)
	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		_ = srv.Serve()
	}()
	defer func() {
		srv.Close()
		<-srvDone
	}()

	cliConn, err := net.DialUDP("udp4", nil, srvConn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer cliConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 220*time.Millisecond)
	defer cancel()

	sess, _, err := Handshake(ctx, cliConn, time.Second)
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	// Configure short read timeout (90 ms) with faster keepalive (30 ms).
	// No audio is ever pushed; ACK replies to HELLO keepalives must keep Run alive
	// until ctx timeout expires cleanly.
	sess.keepaliveInterval = 30 * time.Millisecond
	sess.readTimeout = 90 * time.Millisecond

	err = sess.Run(ctx, func(pcm []byte) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run exited early during silence with %v, want context.DeadlineExceeded", err)
	}
}

func TestServerByeWithWrongClientIDIgnored(t *testing.T) {
	srvConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	src := newMockAudioSource()
	reg := server.NewConnRegistry()
	srv := mustNewServer(t, srvConn, src, Format{SampleRate: 48000, Channels: 2, BitsPerSample: 16}, reg)
	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		_ = srv.Serve()
	}()
	defer func() {
		srv.Close()
		<-srvDone
	}()

	cliConn, err := net.DialUDP("udp4", nil, srvConn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer cliConn.Close()

	var pkt [ControlSize]byte
	EncodeControl(pkt[:], TypeHello, 200)
	_, _ = cliConn.Write(pkt[:])

	// Wait for ACK.
	var ack [DataHeaderSize]byte
	_ = cliConn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := cliConn.Read(ack[:]); err != nil {
		t.Fatalf("read ACK: %v", err)
	}

	// Send stale BYE with old clientID=100 -> must NOT remove peer 200.
	EncodeControl(pkt[:], TypeBye, 100)
	_, _ = cliConn.Write(pkt[:])
	time.Sleep(30 * time.Millisecond)
	if reg.Count() != 1 {
		t.Fatalf("stale BYE removed active peer: count=%d, want 1", reg.Count())
	}

	// Send matching BYE with clientID=200 -> removes peer.
	EncodeControl(pkt[:], TypeBye, 200)
	_, _ = cliConn.Write(pkt[:])
	deadline := time.Now().Add(time.Second)
	for reg.Count() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if reg.Count() != 0 {
		t.Fatalf("matching BYE did not remove peer: count=%d", reg.Count())
	}
}
