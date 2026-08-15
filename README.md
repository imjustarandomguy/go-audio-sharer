# Go Audio Sharer

A high-performance, low-latency gRPC service to stream system loopback audio to other computers on the LAN.

## Features

- **Low Latency**: Configurable buffer size (default 10ms).
- **On-Demand**: Only captures and streams audio when a client is connected.
- **Tray Icon**: Both server and client run in the system tray (KDE Plasma, Windows, macOS).
- **Client Auto-Reconnect**: The client reconnects automatically with backoff, and reconnects immediately when the network (or laptop wake) returns.
- **Network Interface Selection**: The client can bind to a specific LAN interface so it avoids corporate VPN interfaces (like Deskflow).
- **Cross-Platform Client**: Plays audio on Windows, macOS, and Linux via Oto.
- **Configurable**: Adjustable sample rate, channels, bit depth, and target device.

## Build

```bash
# Regenerate proto files if you change proto/audio.proto
./build_proto.sh

# Build the server
go build -o audio_server cmd/server/main.go

# Build the client (Linux/macOS)
go build -o audio_client cmd/client/main.go

# Build the Windows client (from this machine)
./build_windows.sh   # or:
# GOOS=windows GOARCH=amd64 go build -o audio_client.exe -ldflags "-H windowsgui" ./cmd/client
```

### Build dependencies (Linux)

`systray` needs cgo + GTK on Linux:

```bash
sudo apt install libgtk-3-dev libayatana-appindicator3-dev
```

Oto on Linux needs ALSA:

```bash
sudo apt install libasound2-dev
```

The Windows build is pure-Go and can be cross-compiled from Linux with `CGO_ENABLED=0`.

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
| `-bits`     | Bits per sample                                      | `16`     |
| `-no-tray`  | Run headless without a system tray icon              | `false`  |

### Tray icon

The server shows a tray icon with:
- **Connected clients** count and list (updates live),
- **Open Web Player** (opens the web client on `:8080`),
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

Right-click the tray icon for:
- **Start** / **Stop** / **Restart** / **Exit**,
- **Network Interface**: pick the LAN interface (e.g. `Ethernet`/`Wi-Fi`) to route around your VPN,
- **Volume**: mute / 25% / 50% / 75% / 100% presets,
- **Server → Open config file…** and **Server → Reload config**: edit the server address in the config JSON from the tray,
- live **Status** (Connected / Reconnecting / Disconnected).

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
  "latency_ms": 100,
  "auto_connect": true,
  "volume": 1.0
}
```

`interface` is optional; leave it `""` (Automatic) to use the OS default routing. To route around a
VPN, set it to your LAN interface name. `server` should be your LAN address so it resolves over the
picked interface.

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
