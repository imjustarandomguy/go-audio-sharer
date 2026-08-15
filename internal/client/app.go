package client

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"go-audio-sharer/internal/audio"
	"go-audio-sharer/internal/config"
	"go-audio-sharer/internal/netutil"
	pb "go-audio-sharer/proto"
)

const (
	minBackoff = time.Second
	maxBackoff = 30 * time.Second
)

// App manages the client streaming session, reconnect logic, and state.
type App struct {
	cfg           config.Config
	mu            sync.Mutex
	currentPlayer *audio.Player
	running       bool
	cancel        context.CancelFunc
	wake          chan struct{}
	status        string
	onState       func()
}

func New(cfg config.Config) *App {
	return &App{cfg: cfg, status: "Stopped"}
}

// SetStatusCallback registers a handler fired whenever status changes.
func (a *App) SetStatusCallback(cb func()) {
	a.mu.Lock()
	a.onState = cb
	a.mu.Unlock()
}

func (a *App) updateStatus(s string) {
	a.mu.Lock()
	a.status = s
	cb := a.onState
	a.mu.Unlock()
	if cb != nil {
		cb()
	}
}

func (a *App) Status() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.status
}

func (a *App) Config() config.Config {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cfg
}

func (a *App) SetServer(addr string) {
	a.mu.Lock()
	a.cfg.Server = addr
	a.mu.Unlock()
	a.saveConfig()
}

// SetInterface updates the preferred network interface and restarts if running.
func (a *App) SetInterface(name string) {
	a.mu.Lock()
	changed := a.cfg.Interface != name
	a.cfg.Interface = name
	wasRunning := a.running
	a.mu.Unlock()
	a.saveConfig()
	if changed && wasRunning {
		a.Restart()
	}
}

// SetDevice updates the preferred audio output device (substring match) and
// restarts if running. Pass "" to follow the system default device.
func (a *App) SetDevice(name string) {
	a.mu.Lock()
	changed := a.cfg.Device != name
	a.cfg.Device = name
	wasRunning := a.running
	a.mu.Unlock()
	a.saveConfig()
	if changed && wasRunning {
		a.Restart()
	}
}

func (a *App) saveConfig() {
	a.mu.Lock()
	cfg := a.cfg
	a.mu.Unlock()
	_ = cfg.Save()
}

func (a *App) Start() {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return
	}
	a.running = true
	ctx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	a.wake = make(chan struct{}, 1)
	a.mu.Unlock()

	a.updateStatus("Connecting")
	go a.watchInterface(ctx)
	go a.streamLoop(ctx)
}

func (a *App) Stop() {
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return
	}
	a.running = false
	cancel := a.cancel
	a.cancel = nil
	a.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	a.updateStatus("Stopped")
}

func (a *App) Restart() {
	a.Stop()
	a.Start()
}

// Reload re-reads the config file from disk and applies it to the running app.
func (a *App) Reload() {
	nc, err := config.Load()
	if err != nil {
		return
	}
	a.mu.Lock()
	a.cfg = nc
	a.mu.Unlock()
	a.updateStatus(a.Status())
}

func (a *App) streamLoop(ctx context.Context) {
	backoff := minBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		attemptCtx, attemptCancel := context.WithCancel(ctx)
		errCh := make(chan error, 1)
		go func() { errCh <- a.connectAndStream(attemptCtx) }()

		select {
		case <-ctx.Done():
			attemptCancel()
			return
		case <-a.wake:
			attemptCancel()
			backoff = minBackoff
			continue
		case err := <-errCh:
			attemptCancel()
			if err != nil && ctx.Err() != nil {
				return
			}
			a.updateStatus("Reconnecting")
			select {
			case <-ctx.Done():
				return
			case <-a.wake:
				backoff = minBackoff
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (a *App) connectAndStream(ctx context.Context) error {
	a.mu.Lock()
	server := a.cfg.Server
	ifaceName := a.cfg.Interface
	a.mu.Unlock()

	var iface *netutil.Interface
	if ifaceName != "" {
		if f, ok := netutil.FindInterface(ifaceName); ok {
			iface = &f
		} else {
			iface = nil
		}
	}

	localIP, target, err := netutil.DialTarget(server, iface)
	if err != nil {
		return err
	}
	dialer := netutil.NewDialer(localIP)

	conn, err := grpc.NewClient(server,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", target)
		}),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	c := pb.NewAudioStreamClient(conn)
	fmtInfo, err := c.GetFormat(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}

	cfg := audio.Config{
		SampleRate:    int(fmtInfo.SampleRate),
		Channels:      int(fmtInfo.Channels),
		BitsPerSample: int(fmtInfo.BitsPerSample),
		BufferMs:      a.Config().LatencyMs,
		Device:        a.Config().Device,
	}

	p, err := audio.NewPlayer(cfg)
	if err != nil {
		return err
	}
	defer p.Close()
	if err := p.Start(); err != nil {
		return err
	}

	a.mu.Lock()
	a.currentPlayer = p
	vol := a.cfg.Volume
	a.mu.Unlock()
	p.SetVolume(vol)
	defer func() {
		a.mu.Lock()
		if a.currentPlayer == p {
			a.currentPlayer = nil
		}
		a.mu.Unlock()
	}()

	a.updateStatus("Connected")
	stream, err := c.GetStream(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}

	for {
		in, rerr := stream.Recv()
		if rerr != nil {
			if rerr == io.EOF || ctx.Err() != nil {
				return rerr
			}
			return rerr
		}
		if wErr := p.Write(in.Data); wErr != nil {
			return wErr
		}
	}
}

// Volume returns the configured playback volume.
func (a *App) Volume() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cfg.Volume
}

// SetVolume updates and persists the playback volume, applying it live.
func (a *App) SetVolume(v float64) {
	if v < 0 {
		v = 0
	}
	if v > 1 {
		v = 1
	}
	a.mu.Lock()
	a.cfg.Volume = v
	player := a.currentPlayer
	cfg := a.cfg
	a.mu.Unlock()
	if player != nil {
		player.SetVolume(v)
	}
	_ = cfg.Save()
}

// watchInterface triggers an immediate reconnect when the preferred interface
// (re)appears, e.g. when the laptop wakes and the LAN comes back up.
func (a *App) watchInterface(ctx context.Context) {
	a.mu.Lock()
	ifaceName := a.cfg.Interface
	a.mu.Unlock()
	if ifaceName == "" {
		return
	}

	prev := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
		_, present := netutil.FindInterface(ifaceName)
		if present && !prev {
			select {
			case a.wake <- struct{}{}:
			default:
			}
		}
		prev = present
	}
}
