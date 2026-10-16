package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestDefault(t *testing.T) {
	c := Default()
	if c.Server != "localhost:5001" {
		t.Errorf("Server = %q, want localhost:5001", c.Server)
	}
	if c.LatencyMs != 10 {
		t.Errorf("LatencyMs = %d, want 10 (as low as possible)", c.LatencyMs)
	}
	if c.Volume != 1.0 {
		t.Errorf("Volume = %v, want 1.0", c.Volume)
	}
	if !c.AutoConnect {
		t.Error("AutoConnect should default to true")
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test relies on XDG_CONFIG_HOME used on Unix")
	}
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	orig := Default()
	orig.Server = "192.168.1.50:5001"
	orig.Interface = "Ethernet"
	orig.Device = "Arctis Nova 7"
	orig.LatencyMs = 20
	orig.Volume = 0.5
	orig.AutoConnect = false

	if err := orig.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Verify the file is where we expect it.
	expected := filepath.Join(dir, "audio-sharer", "client.json")
	if _, err := os.Stat(expected); err != nil {
		t.Fatalf("file not written to %s: %v", expected, err)
	}

	got, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Server != orig.Server {
		t.Errorf("Server = %q, want %q", got.Server, orig.Server)
	}
	if got.Interface != orig.Interface {
		t.Errorf("Interface = %q, want %q", got.Interface, orig.Interface)
	}
	if got.Device != orig.Device {
		t.Errorf("Device = %q, want %q", got.Device, orig.Device)
	}
	if got.LatencyMs != orig.LatencyMs {
		t.Errorf("LatencyMs = %d, want %d", got.LatencyMs, orig.LatencyMs)
	}
	if got.Volume != orig.Volume {
		t.Errorf("Volume = %v, want %v", got.Volume, orig.Volume)
	}
	if got.AutoConnect != orig.AutoConnect {
		t.Errorf("AutoConnect = %v, want %v", got.AutoConnect, orig.AutoConnect)
	}
}

func TestLoadMissingFileReturnsDefaults(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test relies on XDG_CONFIG_HOME used on Unix")
	}
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	got, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := Default()
	if got.Server != want.Server || got.LatencyMs != want.LatencyMs {
		t.Errorf("expected defaults, got %+v", got)
	}
}
