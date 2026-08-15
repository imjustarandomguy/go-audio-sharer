package server

import (
	"sort"
	"sync"
)

// ConnRegistry tracks currently connected audio streaming clients (gRPC and
// WebSocket) by their remote address, so the tray and logs can report them.
type ConnRegistry struct {
	mu    sync.Mutex
	conns map[string]struct{}
}

func NewConnRegistry() *ConnRegistry {
	return &ConnRegistry{conns: make(map[string]struct{})}
}

func (r *ConnRegistry) Add(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.conns[addr] = struct{}{}
}

func (r *ConnRegistry) Remove(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.conns, addr)
}

func (r *ConnRegistry) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.conns)
}

func (r *ConnRegistry) List() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.conns))
	for a := range r.conns {
		out = append(out, a)
	}
	sort.Strings(out)
	return out
}
