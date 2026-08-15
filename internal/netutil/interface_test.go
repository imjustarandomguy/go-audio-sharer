package netutil

import (
	"net"
	"testing"
)

func TestDialTargetIPNoInterface(t *testing.T) {
	local, target, err := DialTarget("192.168.1.50:50051", nil)
	if err != nil {
		t.Fatalf("DialTarget: %v", err)
	}
	if local != nil {
		t.Errorf("local = %v, want nil", local)
	}
	if target != "192.168.1.50:50051" {
		t.Errorf("target = %q, want 192.168.1.50:50051", target)
	}
}

func TestDialTargetWithInterface(t *testing.T) {
	iface := &Interface{
		Name: "eth0",
		IP:   net.ParseIP("10.0.0.5"),
		Net:  &net.IPNet{IP: net.ParseIP("10.0.0.0"), Mask: net.CIDRMask(8, 32)},
	}
	local, target, err := DialTarget("10.1.2.3:50051", iface)
	if err != nil {
		t.Fatalf("DialTarget: %v", err)
	}
	if local == nil || !local.Equal(net.ParseIP("10.0.0.5")) {
		t.Errorf("local = %v, want 10.0.0.5", local)
	}
	if target != "10.1.2.3:50051" {
		t.Errorf("target = %q, want 10.1.2.3:50051", target)
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
