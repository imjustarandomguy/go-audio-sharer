package netutil

import (
	"fmt"
	"net"
	"strings"
	"time"
)

// Interface describes a network interface with its IPv4 address.
type Interface struct {
	Name string
	IP   net.IP
	Net  *net.IPNet
}

// Interfaces returns all up, non-loopback interfaces with an IPv4 address.
func Interfaces() []Interface {
	var out []Interface
	ifaces, err := net.Interfaces()
	if err != nil {
		return out
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipn, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip4 := ipn.IP.To4()
			if ip4 != nil {
				mask := net.CIDRMask(ipn.Mask.Size())
				out = append(out, Interface{
					Name: iface.Name,
					IP:   ip4,
					Net:  &net.IPNet{IP: ip4, Mask: mask},
				})
				break
			}
		}
	}
	return out
}

// FindInterface returns the interface matching name (case-insensitive substring match).
// It prefers an exact case-insensitive match, and falls back to a substring match.
func FindInterface(name string) (Interface, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return Interface{}, false
	}
	lower := strings.ToLower(name)
	ifaces := Interfaces()

	// Pass 1: exact match (case-insensitive)
	for _, iface := range ifaces {
		if strings.ToLower(iface.Name) == lower {
			return iface, true
		}
	}

	// Pass 2: substring match (case-insensitive)
	for _, iface := range ifaces {
		if strings.Contains(strings.ToLower(iface.Name), lower) {
			return iface, true
		}
	}
	return Interface{}, false
}

// DialTarget resolves the server address and picks a target reachable via the
// given interface. It returns the local address to bind to, and the address to dial.
func DialTarget(serverAddr string, iface *Interface) (localIP net.IP, target string, err error) {
	host, port, err := net.SplitHostPort(serverAddr)
	if err != nil {
		return nil, "", fmt.Errorf("invalid server address %q: %w", serverAddr, err)
	}

	// Resolve the host to an IP.
	var ip net.IP
	if parsed := net.ParseIP(host); parsed != nil {
		ip = parsed
	} else {
		addrs, lerr := net.LookupIP(host)
		if lerr != nil {
			return nil, "", lerr
		}
		if iface != nil {
			for _, a := range addrs {
				if ip4 := a.To4(); ip4 != nil && iface.Net.Contains(ip4) {
					ip = ip4
					break
				}
			}
		}
		if ip == nil {
			for _, a := range addrs {
				if ip4 := a.To4(); ip4 != nil {
					ip = ip4
					break
				}
			}
		}
	}
	if ip == nil {
		return nil, "", fmt.Errorf("could not resolve server host %q", host)
	}

	if iface == nil {
		return nil, net.JoinHostPort(ip.String(), port), nil
	}
	return iface.IP, net.JoinHostPort(ip.String(), port), nil
}

// NewDialer returns a dialer bound to localIP, or a plain dialer when localIP is nil.
func NewDialer(localIP net.IP) *net.Dialer {
	d := &net.Dialer{Timeout: 5 * time.Second}
	if localIP != nil {
		d.LocalAddr = &net.TCPAddr{IP: localIP}
	}
	return d
}
