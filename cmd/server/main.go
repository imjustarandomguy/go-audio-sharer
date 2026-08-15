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
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/emptypb"

	"go-audio-sharer/internal/audio"
	"go-audio-sharer/internal/server"
	"go-audio-sharer/internal/tray"
	webpage "go-audio-sharer/internal/web"
	pb "go-audio-sharer/proto"
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

type streamServer struct {
	pb.UnimplementedAudioStreamServer
	recorder *audio.Recorder
	config   audio.Config
	registry *server.ConnRegistry
	restart  func()
}

func remoteAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return "unknown"
}

func (s *streamServer) GetStream(_ *emptypb.Empty, stream pb.AudioStream_GetStreamServer) error {
	addr := remoteAddr(stream.Context())
	log.Printf("New client connected: %s", addr)
	s.registry.Add(addr)
	defer func() {
		s.registry.Remove(addr)
		log.Printf("Client disconnected: %s", addr)
	}()

	ch := s.recorder.Subscribe()
	defer s.recorder.Unsubscribe(ch)

	for block := range ch {
		err := stream.Send(&pb.AudioSample{
			Timestamp: block.Time.Format(time.RFC3339Nano),
			Data:      block.Data,
		})
		if err != nil {
			log.Printf("Error sending stream to %s: %v", addr, err)
			return err
		}
	}
	return nil
}

func (s *streamServer) GetFormat(_ context.Context, _ *emptypb.Empty) (*pb.AudioFormat, error) {
	bytesPerSec := s.config.SampleRate * s.config.Channels * (s.config.BitsPerSample / 8)
	return &pb.AudioFormat{
		AverageBytesPerSecond: int32(bytesPerSec),
		BitsPerSample:         int32(s.config.BitsPerSample),
		BlockAlign:            int32(s.config.Channels * (s.config.BitsPerSample / 8)),
		Channels:              int32(s.config.Channels),
		SampleRate:            int32(s.config.SampleRate),
		Encoding:              "PCM",
		Codec:                 "PCM",
		Timestamp:             time.Now().UTC().Format(time.RFC3339Nano),
	}, nil
}

func wsStreamAudio(recorder *audio.Recorder, registry *server.ConnRegistry, config audio.Config, restart func()) http.HandlerFunc {
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
			if err := conn.WriteMessage(websocket.BinaryMessage, block.Data); err != nil {
				log.Printf("WebSocket write error: %v", err)
				return
			}
		}
	}
}

func main() {
	port := flag.String("port", "50051", "Port to listen on")
	bufferMs := flag.Int("buffer-ms", 10, "Buffer size in milliseconds")
	sampleRate := flag.Int("rate", 48000, "Sample rate")
	channels := flag.Int("channels", 2, "Number of channels")
	device := flag.String("device", "", "Audio device name to loopback from (optional)")
	webAddr := flag.String("web-addr", ":8080", "Address for the HTTP/WebSocket web player")
	noTray := flag.Bool("no-tray", false, "Run headless without a system tray icon")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	gRPCAddr := fmt.Sprintf(":%s", *port)
	webURL := localURL(*webAddr)
	grpcDisplay := gRPCAddr
	if strings.HasPrefix(grpcDisplay, ":") {
		grpcDisplay = "127.0.0.1" + grpcDisplay
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
	ss := &streamServer{
		recorder: recorder,
		config:   config,
		registry: registry,
		restart:  recorder.Restart,
	}

	grpcServer := grpc.NewServer()
	pb.RegisterAudioStreamServer(grpcServer, ss)

	httpServer := &http.Server{}
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/ws", wsStreamAudio(recorder, registry, config, recorder.Restart))
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
		log.Printf("gRPC server listening on %v", lis.Addr())
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	shutdown := func() {
		// Use Stop(), not GracefulStop(): GracefulStop waits for active streaming
		// RPCs (GetStream), which never end while a client stays connected.
		grpcServer.Stop()
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
		systray.SetIcon(tray.ServerIcon)
		systray.SetTooltip(fmt.Sprintf("Audio Sharer Server (%s)", grpcDisplay))

		var statusItem *systray.MenuItem
		var clientsItem *systray.MenuItem

		var rebuildMu sync.Mutex
		var rebuild func()

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
			systray.ResetMenu()
			statusItem = systray.AddMenuItem(fmt.Sprintf("Status: Running (%s)", grpcDisplay), "")
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
				it := item
				name := dev.Name
				go func() {
					for range it.ClickedCh {
						recorder.SetDevice(name)
						safeRebuild(&rebuildMu, rebuild)
					}
				}()
			}
			go func() {
				for range autoDev.ClickedCh {
					recorder.SetDevice("")
					safeRebuild(&rebuildMu, rebuild)
				}
			}()

			restartItem := systray.AddMenuItem("Restart", "Restart audio capturing (re-detect device)")
			systray.AddSeparator()
			quitItem := systray.AddMenuItem("Quit", "Stop the server")

			go func() {
				for range webItem.ClickedCh {
					_ = execCommand(webURL)
				}
			}()
			go func() {
				for range restartItem.ClickedCh {
					ss.restart()
					statusItem.SetTitle(fmt.Sprintf("Status: Running (%s)", grpcDisplay))
					rebuildClients()
				}
			}()
			go func() {
				for range quitItem.ClickedCh {
					systray.Quit()
				}
			}()
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
