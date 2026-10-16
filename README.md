# Go Audio Sharer

A high-performance, low-latency gRPC and WebSocket service for streaming system loopback audio across a local network to desktop clients or web browsers.

## Features

- **Low Latency**: Configurable capture and playback buffer sizes (default 10ms).
- **On-Demand Capture**: Captures and streams audio only while at least one client is connected.
- **Built-in Web Player**: Zero-install browser player served over HTTP/WebSocket (`:8080` by default) with output device selection.
- **System Tray UI**: Both server and client run in the system tray (Linux, Windows, macOS) or headless via `-no-tray`.
- **Automatic Reconnect**: Reconnects automatically with exponential backoff and immediately upon network recovery (such as waking from sleep).
- **Network Interface Binding**: Optionally bind client connections to a specific LAN interface to bypass VPN tunnels or virtual adapters.
- **Cross-Platform Audio (`malgo`/miniaudio)**: Select a specific audio device or follow the OS default device automatically; captures system loopback on Linux via PulseAudio/PipeWire monitor sources.
- **Configurable**: Adjustable sample rate, channels, buffer size, target latency, and playback volume (16-bit PCM).

## Build

```bash
# Regenerate protobuf code if proto/audio.proto is modified
./build_proto.sh

# Build the server and client (native platform)
go build -o audio_server ./cmd/server
go build -o audio_client ./cmd/client

# Build the Windows client
./build_windows.sh
```

### Linux Build Dependencies

Both the system tray (`systray`) and audio backend (`malgo`) require `cgo` and native development headers:

```bash
# Debian / Ubuntu
sudo apt install build-essential libgtk-3-dev libayatana-appindicator3-dev libasound2-dev

# Arch Linux
sudo pacman -S --needed base-devel gtk3 libayatana-appindicator alsa-lib
```

### Building for Windows

Because `malgo` (miniaudio) uses `cgo`, building `audio_client.exe` requires a C toolchain targeting Windows—either natively on Windows or cross-compiled from Linux with `mingw-w64`:

```bash
# Native build on Windows:
go build -o audio_client.exe -ldflags "-H windowsgui" ./cmd/client

# Cross-compile from Linux with mingw-w64:
CGO_ENABLED=1 GOOS=windows CC=x86_64-w64-mingw32-gcc \
  go build -o audio_client.exe -ldflags "-H windowsgui" ./cmd/client
```

## Server Usage

```bash
./audio_server [flags]
```

| Flag         | Description                                          | Default    |
|--------------|------------------------------------------------------|------------|
| `-port`      | gRPC port to listen on                               | `5001`     |
| `-device`    | Audio device to loopback from (substring match)      | (default)  |
| `-buffer-ms` | Capture buffer size in milliseconds                  | `10`       |
| `-rate`      | Sample rate in Hz                                    | `48000`    |
| `-channels`  | Number of audio channels                             | `2`        |
| `-web-addr`  | Listen address for the HTTP/WebSocket web player     | `:8080`    |
| `-no-tray`   | Run headless without a system tray icon              | `false`    |

Audio is streamed as **16-bit signed little-endian PCM**.

### Web Player

The server hosts an embedded web player at `http://<server-ip>:8080` (configurable via `-web-addr`). Open the URL in any modern browser on the LAN and click **Connect** to listen without installing the native client.

### Server Tray Menu

The server tray menu displays the active gRPC and web player addresses and provides:
- **Connected clients** count (updated live across gRPC and WebSocket clients)
- **Open Web Player** (launches the web player in the default browser)
- **Audio Device**: select a specific output device to loop back, or *Default (follow)* to track system default changes
- **Restart**: re-initialize audio capture
- **Quit**

#### Desktop Autostart (Linux)

Because the system tray requires an active desktop session, you can start the tray server on login using an XDG autostart entry:

```bash
mkdir -p ~/.local/bin ~/.config/autostart
cp audio_server ~/.local/bin/
cp autostart/audio-sharer-server.desktop ~/.config/autostart/
# Update Exec= in ~/.config/autostart/audio-sharer-server.desktop if ~/.local/bin is not in your session PATH
```

#### Headless Systemd Service (Linux)

To run the server in the background without a tray icon, use `-no-tray` with a systemd user service:

```bash
mkdir -p ~/.local/bin ~/.config/systemd/user/
cp audio_server ~/.local/bin/
cp audio-sharer.service ~/.config/systemd/user/

systemctl --user daemon-reload
systemctl --user enable --now audio-sharer
```

## Client Usage

```bash
./audio_client [flags]
```

| Flag           | Description                                                | Default     |
|----------------|------------------------------------------------------------|-------------|
| `-server`      | Server address override (`host:port`)                      | (config)    |
| `-device`      | Audio output device override (substring match)             | (config)    |
| `-latency`     | Playback latency override in ms (`0` = use config)         | `0`         |
| `-autoconnect` | Connect automatically on launch                            | `true`      |
| `-no-tray`     | Run headless without a tray icon; print status to terminal | `false`     |

Example with CLI overrides:

```bash
./audio_client \
  -server 192.168.1.10:5001 \
  -device "Headphones" \
  -latency 20 \
  -no-tray
```

### Client Tray Menu

Right-click the client tray icon to access:
- **Start** / **Stop** / **Restart** / **Exit**
- **Server**: view the target server address, open the JSON config file in the default editor, or reload configuration from disk
- **Network Interface**: bind to a specific LAN interface (e.g. `Ethernet` or `Wi-Fi`) or use *Automatic (default)*
- **Audio Device**: pin playback to a specific output device or use *Default (follow)* to track the OS default output
- **Volume**: `Mute`, `25%`, `50%`, `75%`, `100%`
- **Latency**: target playback buffer presets (`5 ms` – `100 ms`)
- **Status & Live Latency**: current connection state and buffered playback latency readout

### Windows Client

`audio_client.exe` is built as a Windows GUI application (`-H windowsgui`) so it runs in the system tray without a console window. To launch it automatically on sign-in, place a shortcut to `audio_client.exe` in the Startup folder (`Win+R` → `shell:startup`).

### Configuration File

Client settings are persisted as JSON:

- **Linux**: `~/.config/audio-sharer/client.json`
- **Windows**: `%APPDATA%\audio-sharer\client.json`

```json
{
  "server": "192.168.1.10:5001",
  "interface": "",
  "device": "",
  "latency_ms": 10,
  "auto_connect": true,
  "volume": 1.0
}
```

- `interface`: Leave `""` to use OS default routing, or set to a network interface name (e.g. `"Ethernet"`, `"eth0"`) to bind outbound traffic to that interface's IPv4 address.
- `device`: Leave `""` to follow the system default output device, or specify a device name substring.

### Logs

The client logs events to:

- **Linux**: `~/.config/audio-sharer/client.log` (and `stderr` when launched from a terminal)
- **Windows**: `%APPDATA%\audio-sharer\client.log`

## Tests

```bash
go test -race ./...
```
