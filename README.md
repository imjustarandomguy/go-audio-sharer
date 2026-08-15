# Go Audio Sharer

A high-performance, low-latency gRPC service to stream system loopback audio to other computers on the LAN.

## Features

- **Low Latency**: Configurable buffer size (default 10ms).
- **On-Demand**: Only captures and streams audio when a client is connected.
- **Tray Icon**: Both server and client run in the system tray (KDE Plasma, Windows, macOS).
- **Client Auto-Reconnect**: The client reconnects automatically with backoff, and reconnects immediately when the network (or laptop wake) returns.
- **Network Interface Selection**: The client can bind to a specific LAN interface so it avoids corporate VPN interfaces (like Deskflow).
- **Cross-Platform Audio (malgo/miniaudio)**: Client selects an output device or automatically follows the OS default output device; server captures system loopback (Linux via the PulseAudio monitor).
- **Configurable**: Adjustable sample rate, channels, buffer, latency, playout volume, and target devices (16-bit PCM).

## Build

```bash
# Regenerate proto files if you change proto/audio.proto
./build_proto.sh

# Build the server and client (native platform)
go build -o audio_server cmd/server/main.go
go build -o audio_client cmd/client/main.go

# Windows client - see build_windows.sh
./build_windows.sh
```

### Build dependencies (Arch Linux)

The tray (`systray`) and audio (`malgo`) both need cgo + dev libraries. You also need a C compiler
(`base-devel`). On Arch:

```bash
sudo pacman -S --needed base-devel gtk3 libayatana-appindicator alsa-lib
```

(On Debian/Ubuntu the equivalents are `libgtk-3-dev libayatana-appindicator3-dev libasound2-dev build-essential`.)

### Windows build and cgo

The audio backend is `malgo` (miniaudio), which uses **cgo**, so the Windows
client is **not** a pure-Go cross-compile. Build it on the Windows machine, or
with a mingw-w64 cross toolchain:

```bash
# On Windows (recommended):
go build -o audio_client.exe -ldflags "-H windowsgui" ./cmd/client

# Or cross-compile from Arch with mingw-w64:
sudo pacman -S --needed mingw-w64-gcc
CGO_ENABLED=1 GOOS=windows CC=x86_64-w64-mingw32-gcc \
  go build -o audio_client.exe -ldflags "-H windowsgui" ./cmd/client
```

## Server Usage

```bash
./audio_server [flags]
```

| Flag        | Description                                          | Default  |
|-------------|------------------------------------------------------|----------|
| `-port`     | Port to listen on                                    | `50051`  |
| `-device`   | Audio device to loopback from (substring match)      | (default)|
| `-buffer-ms`| Capture buffer size in milliseconds                  | `10`     |
| `-rate`     | Sample rate                                          | `48000`  |
| `-channels` | Channels                                             | `2`      |
| `-web-addr` | Address for the HTTP/WebSocket web player            | `:8080`  |
| `-no-tray`  | Run headless without a system tray icon              | `false`  |

Audio is always **16-bit PCM** (bits are not configurable).

### Tray icon

The server shows a tray icon with the active gRPC port and web player address, and:
- **Connected clients** count and list (updates live),
- **Open Web Player** (opens the web client at the configured web address),
- **Audio Device**: choose which output device to loop back, or *Default (follow)* (the device list tracks system changes),
- **Restart** (re-detect/restart audio capturing),
- **Quit**.

Because a tray needs a running desktop session, run the server via **Plasma autostart** rather
than a background service:

```bash
mkdir -p ~/.config/autostart
cp autostart/audio-sharer-server.desktop ~/.config/autostart/
# Edit the Exec= path in ~/.config/autostart/audio-sharer-server.desktop if needed
```

Headless systemd mode is still supported with `-no-tray` (see `audio-sharer.service`).

## Client Usage

The client is a tray application. It connects automatically on launch (unless disabled in config).

It also accepts CLI flags which override the config for that run (useful for testing):

```bash
./audio_client \
  -server 10.0.0.220:5001 \  # override the server address
  -device "Arctis Nova 7" \   # override the output device
  -latency 20 \               # override latency in ms
  -no-tray                    # run headless: print status to the terminal (no tray icon)
```

Right-click the tray icon for:
- **Start** / **Stop** / **Restart** / **Exit**,
- **Network Interface**: pick the LAN interface (e.g. `Ethernet`/`Wi-Fi`) to route around your VPN,
- **Volume**: mute / 25% / 50% / 75% / 100% presets,
- **Latency**: preset playback latency targets (5–100 ms); defaults as low as possible (10ms),
- **Audio Device**: pick a specific output device, or *Default (follow)* to automatically switch when you change the system default output,
- **Server → Open config file…** and **Server → Reload config**: edit the server address in the config JSON from the tray,
- live **Status** (Connected / Reconnecting / Disconnected) and a **Latency** readout (current buffered playback latency, refreshed every few seconds).

### Windows

Copy `audio_client.exe` to your laptop and run it. No console window appears. It will:

- start on login automatically if you add it to Startup (shortcut in `shell:startup`),
- auto-connect to the server,
- reconnect automatically after waking from sleep.

### Configuration

Settings are kept in a JSON file and edited via the tray (interface) or config file directly:

- Linux: `~/.config/audio-sharer/client.json`
- Windows: `%APPDATA%\audio-sharer\client.json`

```json
{
  "server": "192.168.1.10:50051",
  "interface": "Ethernet",
  "device": "",
  "latency_ms": 10,
  "auto_connect": true,
  "volume": 1.0
}
```

`interface` is optional; leave it `""` (Automatic) to use the OS default routing. To route around a
VPN, set it to your LAN interface name. `server` should be your LAN address so it resolves over the
picked interface.

`device` is optional; leave it `""` (Default follow) to automatically use and follow the system default
output device, or set a device name substring to pin playback to a specific audio output.

### Client logs

The client writes a log file (helpful on Windows, which has no console):

- Linux: `~/.config/audio-sharer/client.log`
- Windows: `%APPDATA%\audio-sharer\client.log`

On Linux/macOS, output also still appears in the terminal when run from one.

## Tests

```bash
go test ./...
```

## Running as a Service (headless server, Linux)

1.  Copy the service file:

    ```bash
    mkdir -p ~/.config/systemd/user/
    cp audio-sharer.service ~/.config/systemd/user/
    ```

2.  (Optional) Edit `~/.config/systemd/user/audio-sharer.service` to add custom flags to `ExecStart`
    (keep `-no-tray` when running headless).

3.  Enable and start:

    ```bash
    systemctl --user daemon-reload
    systemctl --user enable --now audio-sharer
    ```
