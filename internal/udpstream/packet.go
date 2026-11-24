package udpstream

import (
	"encoding/binary"
)

const (
	Magic0          byte  = 'A'
	Magic1          byte  = 'S'
	ProtocolVersion uint8 = 1

	TypeHello uint8 = 1 // Client -> Server: subscribe / keepalive (12 bytes)
	TypeBye   uint8 = 2 // Client -> Server: clean disconnect (12 bytes)
	TypeAudio uint8 = 3 // Server -> Client: 16-byte header + raw PCM
	TypeAck   uint8 = 4 // Server -> Client: 16-byte format ack (no payload)

	ControlSize    = 12
	DataHeaderSize = 16

	// MaxPayloadBytes keeps every UDP datagram (DataHeaderSize + payload <= 1216)
	// comfortably below the 1500-byte Ethernet / IPv6 / PPPoE / VPN MTU so no
	// IP fragmentation occurs.
	MaxPayloadBytes = 1200

	// TargetChunkMs aims for 5 ms PCM packets (960 bytes at 48 kHz stereo 16-bit),
	// keeping every datagram single-MTU and low-latency.
	TargetChunkMs = 5
)

// Format describes the raw signed little-endian PCM stream parameters carried
// in every server packet header.
type Format struct {
	SampleRate    int
	Channels      int
	BitsPerSample int
}

// Valid reports whether f represents a sane PCM audio format.
func (f Format) Valid() bool {
	return f.SampleRate > 0 && f.SampleRate <= 384000 &&
		f.Channels > 0 && f.Channels <= 8 &&
		(f.BitsPerSample == 8 || f.BitsPerSample == 16 || f.BitsPerSample == 24 || f.BitsPerSample == 32)
}

// FrameBytes returns the byte size of one interleaved audio frame.
func (f Format) FrameBytes() int {
	fb := f.Channels * (f.BitsPerSample / 8)
	if fb <= 0 {
		return 2
	}
	return fb
}

// ChunkBytes returns the frame-aligned maximum PCM payload size per UDP packet,
// targeting TargetChunkMs (5 ms) and never exceeding MaxPayloadBytes.
func (f Format) ChunkBytes() int {
	fb := f.FrameBytes()
	maxAligned := (MaxPayloadBytes / fb) * fb
	if maxAligned < fb {
		return fb
	}
	target := int((int64(f.SampleRate) * int64(fb) * TargetChunkMs) / 1000)
	target = (target / fb) * fb
	if target < fb {
		target = fb
	}
	if target > maxAligned {
		target = maxAligned
	}
	return target
}

// EncodeControl writes a 12-byte HELLO or BYE packet into dst and returns ControlSize.
func EncodeControl(dst []byte, pktType uint8, clientID uint64) int {
	dst[0] = Magic0
	dst[1] = Magic1
	dst[2] = ProtocolVersion
	dst[3] = pktType
	binary.LittleEndian.PutUint64(dst[4:12], clientID)
	return ControlSize
}

// DecodeControl parses a 12-byte HELLO or BYE control packet.
func DecodeControl(b []byte) (pktType uint8, clientID uint64, ok bool) {
	if len(b) < ControlSize || b[0] != Magic0 || b[1] != Magic1 || b[2] != ProtocolVersion {
		return 0, 0, false
	}
	pktType = b[3]
	if pktType != TypeHello && pktType != TypeBye {
		return 0, 0, false
	}
	return pktType, binary.LittleEndian.Uint64(b[4:12]), true
}

// EncodeDataHeader writes a 16-byte AUDIO or ACK header into dst and returns DataHeaderSize.
func EncodeDataHeader(dst []byte, pktType uint8, seq uint32, f Format) int {
	dst[0] = Magic0
	dst[1] = Magic1
	dst[2] = ProtocolVersion
	dst[3] = pktType
	binary.LittleEndian.PutUint32(dst[4:8], seq)
	binary.LittleEndian.PutUint32(dst[8:12], uint32(f.SampleRate))
	dst[12] = uint8(f.Channels)
	dst[13] = uint8(f.BitsPerSample)
	dst[14] = 0
	dst[15] = 0
	return DataHeaderSize
}

// DecodeDataPacket parses a server AUDIO or ACK datagram without allocating.
// The returned payload slice aliases b.
func DecodeDataPacket(b []byte) (pktType uint8, seq uint32, f Format, payload []byte, ok bool) {
	if len(b) < DataHeaderSize || b[0] != Magic0 || b[1] != Magic1 || b[2] != ProtocolVersion {
		return 0, 0, Format{}, nil, false
	}
	pktType = b[3]
	if pktType != TypeAudio && pktType != TypeAck {
		return 0, 0, Format{}, nil, false
	}
	seq = binary.LittleEndian.Uint32(b[4:8])
	f = Format{
		SampleRate:    int(binary.LittleEndian.Uint32(b[8:12])),
		Channels:      int(b[12]),
		BitsPerSample: int(b[13]),
	}
	if !f.Valid() {
		return 0, 0, Format{}, nil, false
	}
	return pktType, seq, f, b[DataHeaderSize:], true
}
