package client

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"go-audio-sharer/internal/audio"
	"go-audio-sharer/internal/config"
	"go-audio-sharer/internal/netutil"
	"go-audio-sharer/internal/udpstream"
)

const (
	minBackoff  = time.Second
	maxBackoff  = 5 * time.Second
	stopTimeout = 2 * time.Second
)

// App manages the client streaming session, reconnect logic, and state.
type App struct {
	cfg           config.Config
	mu            sync.Mutex
	currentPlayer *audio.Player
	running       bool
	cancel        context.CancelFunc
	loopDone      chan struct{}
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
	wake := make(chan struct{}, 1)
	loopDone := make(chan struct{})
	a.cancel = cancel
	a.loopDone = loopDone
	a.mu.Unlock()

	a.updateStatus("Connecting")
	go a.watchInterface(ctx, wake)
	go func() {
		defer close(loopDone)
		a.streamLoop(ctx, wake)
	}()
}

func (a *App) Stop() {
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return
	}
	a.running = false
	cancel := a.cancel
	loopDone := a.loopDone
	a.cancel = nil
	a.loopDone = nil
	a.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if loopDone != nil {
		select {
		case <-loopDone:
		case <-time.After(stopTimeout):
			log.Printf("Timed out waiting for streaming loop to stop")
		}
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

func (a *App) streamLoop(ctx context.Context, wake <-chan struct{}) {
	backoff := minBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		attemptCtx, attemptCancel := context.WithCancel(ctx)
		errCh := make(chan error, 1)
		resetBackoffCh := make(chan struct{}, 1)

		go func() {
			errCh <- a.connectAndStream(attemptCtx, func() {
				select {
				case resetBackoffCh <- struct{}{}:
				default:
				}
			})
		}()

		for {
			select {
			case <-ctx.Done():
				attemptCancel()
				select {
				case <-errCh:
				case <-time.After(stopTimeout):
				}
				return
			case <-wake:
				attemptCancel()
				select {
				case <-errCh:
				case <-time.After(stopTimeout):
				}
				backoff = minBackoff
				goto nextAttempt
			case <-resetBackoffCh:
				// Reset backoff immediately upon streaming active audio
				backoff = minBackoff
			case <-errCh:
				attemptCancel()
				if ctx.Err() != nil {
					return
				}
				a.updateStatus("Reconnecting")
				select {
				case <-ctx.Done():
					return
				case <-wake:
					backoff = minBackoff
				case <-time.After(backoff):
				}
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				goto nextAttempt
			}
		}
	nextAttempt:
	}
}

func (a *App) connectAndStream(ctx context.Context, onConnected func()) error {
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

	dnsCtx, dnsCancel := context.WithTimeout(ctx, 5*time.Second)
	localIP, target, err := netutil.DialTargetContext(dnsCtx, server, iface)
	dnsCancel()
	if err != nil {
		return err
	}

	conn, err := netutil.DialUDP(localIP, target)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Handshake over UDP (HELLO -> ACK/AUDIO) with a 5-second timeout to
	// negotiate stream format without hanging on unreachable hosts.
	sess, fmtInfo, err := udpstream.Handshake(ctx, conn, 5*time.Second)
	if err != nil {
		return err
	}
	defer sess.Close()

	cfg := audio.Config{
		SampleRate:    fmtInfo.SampleRate,
		Channels:      fmtInfo.Channels,
		BitsPerSample: fmtInfo.BitsPerSample,
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
	if onConnected != nil {
		onConnected()
	}

	// Periodically log audio and UDP transport glitches (silent when healthy).
	gapThreshold := max(50*time.Millisecond, 2*time.Duration(cfg.BufferMs)*time.Millisecond)
	statsDone := make(chan struct{})
	defer close(statsDone)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-statsDone:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				st := p.TakeStats()
				ust := sess.TakeStats()
				if st.Underruns > 0 || st.OverflowTrims > 0 || st.RingDrops > 0 || st.DeviceResets > 0 ||
					st.MaxWriteGap >= gapThreshold || ust.PacketsLost > 0 || ust.OutOfOrder > 0 {
					log.Printf("Audio glitches (5s): underruns=%d overflow_trims=%d ring_drops=%d device_resets=%d max_packet_gap=%dms pkt_lost=%d ooo=%d buf=%.1fms",
						st.Underruns, st.OverflowTrims, st.RingDrops, st.DeviceResets,
						st.MaxWriteGap.Milliseconds(), ust.PacketsLost, ust.OutOfOrder, p.LatencyMs())
				}
			}
		}
	}()

	runErr := sess.Run(ctx, p.Write)
	if ctx.Err() == nil {
		// Reflect disconnect immediately before audio device teardown runs.
		a.updateStatus("Reconnecting")
	}
	if errors.Is(runErr, udpstream.ErrFormatChanged) {
		log.Printf("Server audio format changed, reconnecting...")
	}
	return runErr
}

// Volume returns the configured playback volume.
func (a *App) Volume() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cfg.Volume
}

// Latency returns the configured playback latency in milliseconds.
func (a *App) Latency() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cfg.LatencyMs
}

// SetLatency updates and persists the latency, restarting if running.
func (a *App) SetLatency(ms int) {
	if ms < 5 {
		ms = 5
	}
	if ms > 500 {
		ms = 500
	}
	a.mu.Lock()
	changed := a.cfg.LatencyMs != ms
	a.cfg.LatencyMs = ms
	wasRunning := a.running
	a.mu.Unlock()
	a.saveConfig()
	if changed && wasRunning {
		a.Restart()
	}
}

// CurrentLatencyMs returns the player's current buffered latency, or 0 when idle.
func (a *App) CurrentLatencyMs() float64 {
	a.mu.Lock()
	player := a.currentPlayer
	a.mu.Unlock()
	if player == nil {
		return 0
	}
	return player.LatencyMs()
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
func (a *App) watchInterface(ctx context.Context, wake chan<- struct{}) {
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
			case wake <- struct{}{}:
			default:
			}
		}
		prev = present
	}
}
