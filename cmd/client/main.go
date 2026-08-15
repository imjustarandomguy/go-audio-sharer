package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"fyne.io/systray"

	"go-audio-sharer/internal/audio"
	"go-audio-sharer/internal/client"
	"go-audio-sharer/internal/config"
	"go-audio-sharer/internal/netutil"
	"go-audio-sharer/internal/tray"
)

// devState caches the latest playback device list (refreshed periodically).
var devState struct {
	mu   sync.Mutex
	devs []audio.DeviceDesc
}

var (
	rebuildMu sync.Mutex
	rebuild   func()
)

func devSnapshot() []audio.DeviceDesc {
	devState.mu.Lock()
	defer devState.mu.Unlock()
	out := make([]audio.DeviceDesc, len(devState.devs))
	copy(out, devState.devs)
	return out
}

func safeRebuild() {
	rebuildMu.Lock()
	defer rebuildMu.Unlock()
	if rebuild != nil {
		rebuild()
	}
}

func main() {
	if f, err := setupLog(); err == nil {
		defer f.Close()
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("could not load config: %v", err)
	}

	if devs, err := audio.ListPlaybackDeviceDescriptions(); err == nil {
		devState.devs = devs
	}

	app := client.New(cfg)
	if cfg.AutoConnect {
		app.Start()
	}

	onReady := func() {
		systray.SetIcon(tray.ClientIcon)
		systray.SetTooltip("Audio Sharer Client")

		var statusItem *systray.MenuItem
		var latencyItem *systray.MenuItem

		rebuild = func() {
			systray.ResetMenu()

			statusItem = systray.AddMenuItem("Status: "+app.Status(), "")
			statusItem.Disable()

			latencyItem = systray.AddMenuItem("Latency: —", "Current buffered playback latency")
			latencyItem.Disable()

			cfg := app.Config()

			serverItem := systray.AddMenuItem("Server", "gRPC server address")
			addrItem := serverItem.AddSubMenuItem(cfg.Server, "")
			addrItem.Disable()
			openCfgItem := serverItem.AddSubMenuItem("Open config file…", "Edit the server address in the config JSON")
			go func() {
				for range openCfgItem.ClickedCh {
					_ = openConfig()
				}
			}()
			reloadCfgItem := serverItem.AddSubMenuItem("Reload config", "Re-read config from disk and reconnect")
			go func() {
				for range reloadCfgItem.ClickedCh {
					app.Reload()
					app.Restart()
					safeRebuild()
				}
			}()

			ifaceMenu := systray.AddMenuItem("Network Interface", "Choose LAN interface to route around a VPN")
			autoItem := ifaceMenu.AddSubMenuItemCheckbox("Automatic (default)", "", cfg.Interface == "")
			for _, iface := range netutil.Interfaces() {
				item := ifaceMenu.AddSubMenuItemCheckbox(
					iface.Name+" ("+iface.IP.String()+")", "", cfg.Interface == iface.Name)
				it := item
				name := iface.Name
				go func() {
					for range it.ClickedCh {
						app.SetInterface(name)
						safeRebuild()
					}
				}()
			}
			go func() {
				for range autoItem.ClickedCh {
					app.SetInterface("")
					safeRebuild()
				}
			}()

			devMenu := systray.AddMenuItem("Audio Device", "Choose an output device, or follow the system default")
			autoDev := devMenu.AddSubMenuItemCheckbox("Default (follow)", "", cfg.Device == "")
			for _, dev := range devSnapshot() {
				label := dev.Name
				if dev.IsDefault {
					label += " (default)"
				}
				item := devMenu.AddSubMenuItemCheckbox(label, dev.ID, cfg.Device == dev.Name)
				it := item
				name := dev.Name
				go func() {
					for range it.ClickedCh {
						app.SetDevice(name)
						safeRebuild()
					}
				}()
			}
			go func() {
				for range autoDev.ClickedCh {
					app.SetDevice("")
					safeRebuild()
				}
			}()

			volMenu := systray.AddMenuItem("Volume", "Playback volume")
			for _, p := range []struct {
				label string
				v     float64
			}{
				{"Mute", 0}, {"25%", 0.25}, {"50%", 0.5}, {"75%", 0.75}, {"100%", 1},
			} {
				preset := volMenu.AddSubMenuItemCheckbox(p.label, "", cfg.Volume == p.v)
				pp := p
				go func() {
					for range preset.ClickedCh {
						app.SetVolume(pp.v)
						safeRebuild()
					}
				}()
			}

			latencyMenu := systray.AddMenuItem("Latency", "Set target playback latency")
			for _, p := range []struct {
				label string
				ms    int
			}{
				{"5 ms", 5}, {"10 ms", 10}, {"15 ms", 15}, {"20 ms", 20},
				{"30 ms", 30}, {"50 ms", 50}, {"100 ms", 100},
			} {
				preset := latencyMenu.AddSubMenuItemCheckbox(p.label, "", cfg.LatencyMs == p.ms)
				pp := p
				go func() {
					for range preset.ClickedCh {
						app.SetLatency(pp.ms)
						safeRebuild()
					}
				}()
			}

			systray.AddSeparator()
			startItem := systray.AddMenuItem("Start", "Connect to the server")
			stopItem := systray.AddMenuItem("Stop", "Disconnect from the server")
			restartItem := systray.AddMenuItem("Restart", "Reconnect now")
			systray.AddSeparator()
			quitItem := systray.AddMenuItem("Exit", "Quit")

			go func() {
				for range startItem.ClickedCh {
					app.Start()
					safeRebuild()
				}
			}()
			go func() {
				for range stopItem.ClickedCh {
					app.Stop()
					safeRebuild()
				}
			}()
			go func() {
				for range restartItem.ClickedCh {
					app.Restart()
					safeRebuild()
				}
			}()
			go func() {
				for range quitItem.ClickedCh {
					app.Stop()
					systray.Quit()
				}
			}()
		}

		app.SetStatusCallback(func() {
			if statusItem != nil {
				statusItem.SetTitle("Status: " + app.Status())
			}
		})

		rebuild()

		// Update the live latency display while connected. Only touch the menu
		// item when the displayed text actually changes, so the open popup isn't
		// re-rendered (which clears the hover highlight on KDE) on every tick.
		go func() {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			var last string
			for range ticker.C {
				rebuildMu.Lock()
				item := latencyItem
				rebuildMu.Unlock()
				if item == nil {
					last = ""
					continue
				}
				var s string
				if app.Status() == "Connected" {
					s = fmt.Sprintf("Latency: %.0f ms", app.CurrentLatencyMs())
				} else {
					s = "Latency: —"
				}
				if s == last {
					continue
				}
				item.SetTitle(s)
				last = s
			}
		}()

		// Refresh the device list and menu when it changes (e.g. the system
		// default output device changes).
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				devs, err := audio.ListPlaybackDeviceDescriptions()
				if err != nil {
					continue
				}
				changed := !sameDevices(devs, devSnapshot())
				devState.mu.Lock()
				devState.devs = devs
				devState.mu.Unlock()
				if changed {
					safeRebuild()
				}
			}
		}()
	}

	tray.Run(onReady, func() {
		app.Stop()
		os.Exit(0)
	})
}

func sameDevices(a, b []audio.DeviceDesc) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].IsDefault != b[i].IsDefault {
			return false
		}
	}
	return true
}

// setupLog mirrors output to a log file in the config directory. This matters
// on Windows where the GUI build has no console. On Linux/macOS, terminal output
// is preserved and struct-era log lines are also written to the file.
func setupLog() (*os.File, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return nil, err
	}
	p := filepath.Join(dir, "audio-sharer", "client.log")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	if runtime.GOOS == "windows" {
		// No console on the GUI build: capture everything to the file.
		os.Stdout = f
		os.Stderr = f
		log.SetOutput(f)
	} else {
		log.SetOutput(io.MultiWriter(os.Stderr, f))
	}
	log.Printf("========== audio client started ==========")
	return f, nil
}

// openConfig opens the client config JSON in the system default editor/text editor.
func openConfig() error {
	p, err := config.Path()
	if err != nil {
		return err
	}
	_ = os.MkdirAll(filepath.Dir(p), 0o755)
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("notepad", p)
	case "darwin":
		cmd = exec.Command("open", p)
	default:
		cmd = exec.Command("xdg-open", p)
	}
	return cmd.Start()
}
