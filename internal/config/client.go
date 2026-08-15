package config

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Config holds persisted client settings.
type Config struct {
	Server      string  `json:"server"`
	Interface   string  `json:"interface"` // optional network interface (e.g. "Ethernet") to use
	LatencyMs   int     `json:"latency_ms"`
	AutoConnect bool    `json:"auto_connect"`
	Volume      float64 `json:"volume"` // playback volume in [0, 1]
}

func Default() Config {
	return Config{
		Server:      "localhost:50051",
		LatencyMs:   100,
		AutoConnect: true,
		Volume:      1.0,
	}
}

// Path returns the config file location, e.g. ~/.config/audio-sharer/client.json
// or %APPDATA%\audio-sharer\client.json on Windows.
func Path() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "audio-sharer", "client.json"), nil
}

func Load() (Config, error) {
	cfg := Default()
	p, err := Path()
	if err != nil {
		return cfg, err
	}
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	if cfg.Server == "" {
		cfg.Server = Default().Server
	}
	if cfg.LatencyMs == 0 {
		cfg.LatencyMs = Default().LatencyMs
	}
	return cfg, nil
}

func (c Config) Save() error {
	p, err := Path()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(p, data, 0o644)
}
