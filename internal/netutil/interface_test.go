package netutil

import (
	"net"
	"testing"
)

func TestDialTargetIPNoInterface(t *testing.T) {
	local, target, err := DialTarget("192.168.1.50:5001", nil)
	if err != nil {
		t.Fatalf("DialTarget: %v", err)
	}
	if local != nil {
		t.Errorf("local = %v, want nil", local)
	}
	if target != "192.168.1.50:5001" {
		t.Errorf("target = %q, want 192.168.1.50:5001", target)
	}
}

func TestDialTargetWithInterface(t *testing.T) {
	iface := &Interface{
		Name: "eth0",
		IP:   net.ParseIP("10.0.0.5"),
		Net:  &net.IPNet{IP: net.ParseIP("10.0.0.0"), Mask: net.CIDRMask(8, 32)},
	}
	local, target, err := DialTarget("10.1.2.3:5001", iface)
	if err != nil {
		t.Fatalf("DialTarget: %v", err)
	}
	if local == nil || !local.Equal(net.ParseIP("10.0.0.5")) {
		t.Errorf("local = %v, want 10.0.0.5", local)
	}
	if target != "10.1.2.3:5001" {
		t.Errorf("target = %q, want 10.1.2.3:5001", target)
	}
}

func TestDialTargetInvalid(t *testing.T) {
	if _, _, err := DialTarget("not-an-addr", nil); err == nil {
		t.Error("expected error for invalid address")
	}
}

func TestNewDialer(t *testing.T) {
	ip := net.ParseIP("10.0.0.5")
	d := NewDialer(ip)
	if d.LocalAddr == nil {
		t.Fatal("NewDialer with ip should set LocalAddr")
	}

	d2 := NewDialer(nil)
	if d2.LocalAddr != nil {
		t.Error("NewDialer without ip should leave LocalAddr nil")
	}
}

func TestFindInterface(t *testing.T) {
	ifaces := Interfaces()
	if len(ifaces) == 0 {
		t.Skip("no network interfaces available on host")
	}
	first := ifaces[0]

	// Exact match
	if found, ok := FindInterface(first.Name); !ok || found.Name != first.Name {
		t.Errorf("exact match failed: got %v, %v; want %s", found, ok, first.Name)
	}

	// Case-insensitive match
	upper := first.Name
	if len(upper) > 0 {
		upper = string(upper[0]-32) + upper[1:] // simple case shift if letter
	}
	if found, ok := FindInterface(upper); !ok || found.Name != first.Name {
		t.Errorf("case-insensitive match failed: got %v, %v; want %s", found, ok, first.Name)
	}

	// Substring match (first 2 chars if long enough)
	if len(first.Name) >= 3 {
		sub := first.Name[:2]
		if found, ok := FindInterface(sub); !ok {
			t.Errorf("substring match failed for %q", sub)
		} else if found.Name == "" {
			t.Errorf("expected non-empty interface name")
		}
	}

	// Non-existent interface
	if _, ok := FindInterface("non_existent_iface_9999"); ok {
		t.Errorf("expected false for nonexistent interface")
	}

	// Empty string
	if _, ok := FindInterface(""); ok {
		t.Errorf("expected false for empty string")
	}
}
