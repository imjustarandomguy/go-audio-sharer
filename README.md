# Go Audio Sharer

A high-performance, low-latency gRPC service to stream system loopback audio to other computers on the LAN.

## Features

- **Low Latency**: Configurable buffer size (default 10ms).
- **On-Demand**: Only captures and streams audio when a client is connected.
- **Configurable**: Adjustable sample rate, channels, bit depth, and target device.
- **Cross-Platform**: Go client included.

## Build

```bash
# Build the proto files
./build_proto.sh

# Build binaries
go build -o audio_server cmd/server/main.go
go build -o audio_client cmd/client/main.go
```

## Server Usage

```bash
./audio_server [flags]

Flags:
  -port string
        Port to listen on (default: "50051")
  -device string
        Name or ID of the audio device to record (matches substring).
        If not provided, uses the default sink's monitor (loopback).
  -buffer-ms int
        Capture buffer size in milliseconds (default: 10).
        Lower = lower latency but higher CPU/network packet rate.
  -rate int
        Sample rate (default: 48000)
  -channels int
        Channels (default: 2)
  -bits int
        Bits per sample (default: 16)
```

Example: Select a specific device and use a 5ms buffer:

```bash
./audio_server -device "Headphones" -buffer-ms 5
```

## Client Usage

```bash
./audio_client [flags]

Flags:
  -server string
        Server address (default: "localhost:50051")
  -device string
        Name or ID of the audio device to play to.
  -latency int
        Playback latency in milliseconds (default: 100).
```

## Running as a Service (Linux)

1.  Copy service file:

    ```bash
    mkdir -p ~/.config/systemd/user/
    cp audio-sharer.service ~/.config/systemd/user/
    ```

2.  (Optional) Edit `~/.config/systemd/user/audio-sharer.service` to add custom flags to `ExecStart`.

3.  Enable and start:
    ```bash
    systemctl --user daemon-reload
    systemctl --user enable --now audio-sharer
    ```
