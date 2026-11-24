package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"fyne.io/systray"
	"github.com/gorilla/websocket"

	"go-audio-sharer/internal/audio"
	"go-audio-sharer/internal/server"
	"go-audio-sharer/internal/tray"
	"go-audio-sharer/internal/udpstream"
	webpage "go-audio-sharer/internal/web"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// execCommand opens a URL in the user's default browser.
func execCommand(url string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	return cmd.Start()
}

// localURL converts a listen address like ":8080" or "0.0.0.0:8080" into a
// browser URL on localhost, e.g. "http://localhost:8080".
func localURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// fall back to treating the whole string as a port
		return "http://localhost:" + strings.TrimPrefix(addr, ":")
	}
	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		host = "localhost"
	}
	return "http://" + net.JoinHostPort(host, port)
}

// safeRebuild serializes menu rebuilds (which call systray.ResetMenu) against
// concurrent callers.
func safeRebuild(mu *sync.Mutex, fn func()) {
	mu.Lock()
	defer mu.Unlock()
	if fn != nil {
		fn()
	}
}

// onClick binds a menu item click handler that terminates cleanly when ctx is cancelled.
func onClick(ctx context.Context, item *systray.MenuItem, action func()) {
	if item == nil {
		return
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-item.ClickedCh:
				if !ok {
					return
				}
				action()
			}
		}
	}()
}

// sameDevices reports whether two device lists are identical by name+default.
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

func wsStreamAudio(recorder *audio.Recorder, registry *server.ConnRegistry, config audio.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("WebSocket upgrade error: %v", err)
			return
		}
		defer conn.Close()

		addr := r.RemoteAddr
		log.Printf("WebSocket client connected: %s", addr)
		registry.Add(addr)
		defer registry.Remove(addr)

		conn.SetCloseHandler(func(code int, text string) error {
			log.Printf("WebSocket closed by %s: %d %s", addr, code, text)
			return nil
		})

		if err := conn.WriteJSON(map[string]interface{}{
			"sampleRate":    config.SampleRate,
			"channels":      config.Channels,
			"bitsPerSample": config.BitsPerSample,
		}); err != nil {
			log.Printf("WebSocket write format error: %v", err)
			return
		}

		ch := recorder.Subscribe()
		defer recorder.Unsubscribe(ch)

		for block := range ch {
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err := conn.WriteMessage(websocket.BinaryMessage, block.Data)
			block.Release()
			if err != nil {
				log.Printf("WebSocket write error: %v", err)
				return
			}
		}
	}
}

func main() {
	port := flag.String("port", "5001", "UDP port to listen on")
	bufferMs := flag.Int("buffer-ms", 10, "Buffer size in milliseconds")
	sampleRate := flag.Int("rate", 48000, "Sample rate")
	channels := flag.Int("channels", 2, "Number of channels")
	device := flag.String("device", "", "Audio device name to loopback from (optional)")
	webAddr := flag.String("web-addr", ":8080", "Address for the HTTP/WebSocket web player")
	noTray := flag.Bool("no-tray", false, "Run headless without a system tray icon")
	flag.Parse()

	udpListenAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%s", *port))
	if err != nil {
		log.Fatalf("invalid UDP listen port: %v", err)
	}
	udpConn, err := net.ListenUDP("udp", udpListenAddr)
	if err != nil {
		log.Fatalf("failed to listen on UDP: %v", err)
	}

	udpAddrStr := fmt.Sprintf(":%s", *port)
	webURL := localURL(*webAddr)
	udpDisplay := udpAddrStr
	if strings.HasPrefix(udpDisplay, ":") {
		udpDisplay = "127.0.0.1" + udpDisplay
	}

	config := audio.Config{
		SampleRate:    *sampleRate,
		Channels:      *channels,
		BitsPerSample: 16,
		BufferMs:      *bufferMs,
		Device:        *device,
	}

	log.Printf("Starting server with config: %+v", config)

	recorder, err := audio.NewRecorder(config)
	if err != nil {
		log.Fatalf("failed to initialize recorder: %v", err)
	}
	defer recorder.Close()

	registry := server.NewConnRegistry()
	udpServer := udpstream.NewServer(udpConn, recorder, udpstream.Format{
		SampleRate:    config.SampleRate,
		Channels:      config.Channels,
		BitsPerSample: config.BitsPerSample,
	}, registry)

	httpServer := &http.Server{}
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/ws", wsStreamAudio(recorder, registry, config))
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(webpage.IndexHTML))
		})
		log.Printf("HTTP/WebSocket server listening on %s", *webAddr)
		httpServer.Addr = *webAddr
		httpServer.Handler = mux
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server failed: %v", err)
		}
	}()

	go func() {
		log.Printf("UDP audio server listening on %v", udpConn.LocalAddr())
		if err := udpServer.Serve(); err != nil {
			log.Fatalf("UDP server failed: %v", err)
		}
	}()

	// Periodically log capture/broadcast glitches (silent when healthy).
	gapThreshold := max(50*time.Millisecond, 2*time.Duration(config.BufferMs)*time.Millisecond)
	statsStop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-statsStop:
				return
			case <-ticker.C:
				st := recorder.TakeStats()
				if st.DroppedBlocks > 0 || st.MaxCaptureGap >= gapThreshold {
					log.Printf("Capture glitches (5s): dropped_blocks=%d max_capture_gap=%dms clients=%d",
						st.DroppedBlocks, st.MaxCaptureGap.Milliseconds(), registry.Count())
				}
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	shutdown := func() {
		select {
		case <-statsStop:
		default:
			close(statsStop)
		}
		udpServer.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(ctx)
	}

	if *noTray {
		select {
		case <-sigCh:
			shutdown()
		}
		return
	}

	onReady := func() {
		systray.SetIcon(tray.ServerIcon())
		systray.SetTooltip(fmt.Sprintf("Audio Sharer Server (%s/udp)", udpDisplay))

		var statusItem *systray.MenuItem
		var clientsItem *systray.MenuItem

		var rebuildMu sync.Mutex
		var rebuild func()
		var menuCancel context.CancelFunc

		rebuildClients := func() {
			if clientsItem == nil {
				return
			}
			clients := registry.List()
			if len(clients) == 0 {
				clientsItem.SetTitle("Connected clients: none")
			} else {
				clientsItem.SetTitle(fmt.Sprintf("Connected clients (%d)", len(clients)))
			}
		}

		devs := func() []audio.DeviceDesc {
			d, err := audio.ListPlaybackDeviceDescriptions()
			if err != nil {
				return nil
			}
			return d
		}

		rebuild = func() {
			if menuCancel != nil {
				menuCancel()
			}
			var menuCtx context.Context
			menuCtx, menuCancel = context.WithCancel(context.Background())

			systray.ResetMenu()
			statusItem = systray.AddMenuItem(fmt.Sprintf("Status: Running (%s/udp)", udpDisplay), "")
			statusItem.Disable()

			webItem := systray.AddMenuItem("Open Web Player", "Open the web client in a browser")
			webAddrItem := systray.AddMenuItem(fmt.Sprintf("Web player: %s", webURL), "")
			webAddrItem.Disable()

			clientsItem = systray.AddMenuItem("Connected clients: none", "")
			clientsItem.Disable()
			rebuildClients()

			devMenu := systray.AddMenuItem("Audio Device", "Choose which output device to loop back, or follow default")
			autoDev := devMenu.AddSubMenuItemCheckbox("Default (follow)", "", recorder.Device() == "")
			for _, dev := range devs() {
				label := dev.Name
				if dev.IsDefault {
					label += " (default)"
				}
				item := devMenu.AddSubMenuItemCheckbox(label, dev.ID, recorder.Device() == dev.Name)
				name := dev.Name
				onClick(menuCtx, item, func() {
					recorder.SetDevice(name)
					safeRebuild(&rebuildMu, rebuild)
				})
			}
			onClick(menuCtx, autoDev, func() {
				recorder.SetDevice("")
				safeRebuild(&rebuildMu, rebuild)
			})

			restartItem := systray.AddMenuItem("Restart", "Restart audio capturing (re-detect device)")
			systray.AddSeparator()
			quitItem := systray.AddMenuItem("Quit", "Stop the server")

			onClick(menuCtx, webItem, func() {
				_ = execCommand(webURL)
			})
			onClick(menuCtx, restartItem, func() {
				recorder.Restart()
				statusItem.SetTitle(fmt.Sprintf("Status: Running (%s/udp)", udpDisplay))
				rebuildClients()
			})
			onClick(menuCtx, quitItem, func() {
				if menuCancel != nil {
					menuCancel()
				}
				systray.Quit()
			})
		}

		rebuild()

		// Refresh the connected-clients count, but only touch the menu item when
		// the count actually changes (avoids re-rendering the open popup on KDE).
		go func() {
			lastCount := -1
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				cur := registry.Count()
				if cur != lastCount {
					rebuildClients()
					lastCount = cur
				}
			}
		}()

		// Refresh the loopback device list when it changes.
		go func() {
			var last []audio.DeviceDesc
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				cur := devs()
				if !sameDevices(last, cur) {
					last = cur
					safeRebuild(&rebuildMu, rebuild)
				}
			}
		}()
	}

	tray.Run(onReady, func() {
		shutdown()
		recorder.Close()
		os.Exit(0)
	})
}
