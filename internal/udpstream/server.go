package udpstream

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go-audio-sharer/internal/audio"
	"go-audio-sharer/internal/server"
)

const (
	defaultPeerTimeout  = 6 * time.Second
	defaultReapInterval = 1 * time.Second
	defaultMaxPeers     = 64
	socketBufferBytes   = 256 * 1024
)

// AudioSource is the capture subscription interface implemented by *audio.Recorder.
type AudioSource interface {
	Subscribe() chan audio.CapturedBlock
	Unsubscribe(chan audio.CapturedBlock)
}

type udpPeer struct {
	addr     *net.UDPAddr
	clientID uint64
	lastSeen time.Time
}

// Server streams captured PCM blocks over single-MTU UDP datagrams to all
// currently subscribed clients, using one shared subscription to AudioSource.
type Server struct {
	conn     *net.UDPConn
	src      AudioSource
	format   Format
	registry *server.ConnRegistry
	epoch    uint16

	peerTimeout  time.Duration
	reapInterval time.Duration
	maxPeers     int

	mu     sync.RWMutex
	peers  map[string]*udpPeer
	closed bool

	subMu sync.Mutex
	subCh chan audio.CapturedBlock

	seq    atomic.Uint32
	stopCh chan struct{}
	wg     sync.WaitGroup
	once   sync.Once
}

func randomEpoch() uint16 {
	var b [2]byte
	_, _ = rand.Read(b[:])
	e := binary.LittleEndian.Uint16(b[:])
	if e == 0 {
		e = 1
	}
	return e
}

// NewServer creates a UDP audio streaming server bound to conn.
func NewServer(conn *net.UDPConn, src AudioSource, format Format, registry *server.ConnRegistry) (*Server, error) {
	if !format.Valid() {
		return nil, fmt.Errorf("invalid audio format: %+v", format)
	}
	_ = conn.SetWriteBuffer(socketBufferBytes)
	_ = conn.SetReadBuffer(socketBufferBytes)
	return &Server{
		conn:         conn,
		src:          src,
		format:       format,
		registry:     registry,
		epoch:        randomEpoch(),
		peerTimeout:  defaultPeerTimeout,
		reapInterval: defaultReapInterval,
		maxPeers:     defaultMaxPeers,
		peers:        make(map[string]*udpPeer),
		stopCh:       make(chan struct{}),
	}, nil
}

// Serve handles incoming client HELLO/BYE packets and fans out audio until
// Close is called.
func (s *Server) Serve() error {
	// Register reapLoop under s.mu after checking s.closed so Close() calling
	// s.wg.Wait() can never race with s.wg.Add(1).
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.wg.Add(1)
	s.mu.Unlock()
	go s.reapLoop()

	buf := make([]byte, 64)
	var ackBuf [DataHeaderSize]byte
	for {
		n, raddr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-s.stopCh:
				return nil
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			// Brief backoff so a persistent OS socket error cannot spin CPU at 100%.
			select {
			case <-s.stopCh:
				return nil
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}

		pktType, clientID, ok := DecodeControl(buf[:n])
		if !ok {
			continue
		}

		switch pktType {
		case TypeHello:
			s.handleHello(raddr, clientID, &ackBuf)
		case TypeBye:
			s.handleBye(raddr, clientID)
		}
	}
}

func (s *Server) handleHello(raddr *net.UDPAddr, clientID uint64, ackBuf *[DataHeaderSize]byte) {
	key := raddr.String()
	regKey := "udp:" + key
	now := time.Now()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	existing, exists := s.peers[key]
	if !exists {
		if s.maxPeers > 0 && len(s.peers) >= s.maxPeers {
			s.mu.Unlock()
			return
		}
		addrCopy := *raddr
		if raddr.IP != nil {
			addrCopy.IP = append(net.IP(nil), raddr.IP...)
		}
		s.peers[key] = &udpPeer{
			addr:     &addrCopy,
			clientID: clientID,
			lastSeen: now,
		}
		if s.registry != nil {
			s.registry.Add(regKey)
		}
	} else {
		existing.clientID = clientID
		existing.lastSeen = now
	}
	s.mu.Unlock()

	if !exists {
		log.Printf("UDP client connected: %s", key)
		s.reconcileSub()
	}

	// Reply immediately with format ACK so clients connect even when audio is silent
	// and idle keepalives reset the client read deadline.
	EncodeDataHeader(ackBuf[:], TypeAck, 0, s.epoch, s.format)
	_, _ = s.conn.WriteToUDP(ackBuf[:], raddr)
}

func (s *Server) handleBye(raddr *net.UDPAddr, clientID uint64) {
	key := raddr.String()
	regKey := "udp:" + key

	s.mu.Lock()
	peer, exists := s.peers[key]
	if !exists || peer.clientID != clientID {
		s.mu.Unlock()
		return
	}
	delete(s.peers, key)
	if s.registry != nil {
		s.registry.Remove(regKey)
	}
	empty := len(s.peers) == 0
	s.mu.Unlock()

	log.Printf("UDP client disconnected: %s", key)
	if empty {
		s.reconcileSub()
	}
}

func (s *Server) reapLoop() {
	defer s.wg.Done()
	t := time.NewTicker(s.reapInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case now := <-t.C:
			var expired []string
			var empty bool

			s.mu.Lock()
			for key, peer := range s.peers {
				if now.Sub(peer.lastSeen) > s.peerTimeout {
					delete(s.peers, key)
					if s.registry != nil {
						s.registry.Remove("udp:" + key)
					}
					expired = append(expired, key)
				}
			}
			empty = len(s.peers) == 0
			s.mu.Unlock()

			for _, key := range expired {
				log.Printf("UDP client timed out: %s", key)
			}
			if len(expired) > 0 && empty {
				s.reconcileSub()
			}
		}
	}
}

// reconcileSub ensures the server holds a single AudioSource subscription if
// and only if it is open and has at least one connected UDP peer.
func (s *Server) reconcileSub() {
	s.subMu.Lock()
	defer s.subMu.Unlock()

	s.mu.RLock()
	wantActive := !s.closed && len(s.peers) > 0
	s.mu.RUnlock()

	if wantActive && s.subCh == nil {
		ch := s.src.Subscribe()
		s.subCh = ch
		s.wg.Add(1)
		go s.sendLoop(ch)
	} else if !wantActive && s.subCh != nil {
		ch := s.subCh
		s.subCh = nil
		s.src.Unsubscribe(ch)
	}
}

func (s *Server) sendLoop(ch chan audio.CapturedBlock) {
	defer s.wg.Done()

	chunkBytes := s.format.ChunkBytes()
	frameBytes := s.format.FrameBytes()
	pktBuf := make([]byte, DataHeaderSize+MaxPayloadBytes)
	var targets []*net.UDPAddr

	for block := range ch {
		data := block.Data
		// Align total block length down to whole frames.
		dataLen := (len(data) / frameBytes) * frameBytes
		if dataLen == 0 {
			block.Release()
			continue
		}

		// Snapshot active peer addresses into a reused slice (zero allocs).
		s.mu.RLock()
		targets = targets[:0]
		for _, peer := range s.peers {
			targets = append(targets, peer.addr)
		}
		s.mu.RUnlock()

		if len(targets) == 0 {
			block.Release()
			continue
		}

		for off := 0; off < dataLen; off += chunkBytes {
			end := off + chunkBytes
			if end > dataLen {
				end = dataLen
			}
			seq := s.seq.Add(1)
			EncodeDataHeader(pktBuf, TypeAudio, seq, s.epoch, s.format)
			n := copy(pktBuf[DataHeaderSize:], data[off:end])
			pkt := pktBuf[:DataHeaderSize+n]

			for _, addr := range targets {
				_, _ = s.conn.WriteToUDP(pkt, addr)
			}
		}

		block.Release()
	}
}

// Close stops the server, unsubscribes from the audio source, clears active
// clients from the registry, and waits for all background goroutines to exit.
func (s *Server) Close() {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		for k := range s.peers {
			delete(s.peers, k)
			if s.registry != nil {
				s.registry.Remove("udp:" + k)
			}
		}
		s.mu.Unlock()

		close(s.stopCh)
		_ = s.conn.Close()
		s.reconcileSub()
		s.wg.Wait()
	})
}
