// Package wspush keeps track of the persistent WebSocket connections opened
// by Android devices and delivers push messages to them.
//
// iOS/watchOS devices are still delivered through APNs; only devices whose
// registered type is "android" register a connection here.
package wspush

import "sync"

// Conn is the minimal connection contract the hub needs.
// github.com/gofiber/websocket/v2.Conn satisfies this interface.
type Conn interface {
	WriteJSON(v interface{}) error
}

// Envelope is the wire format sent to Android clients.
type Envelope struct {
	// Type is "message" for pushes and "ping" for keepalive frames.
	Type string `json:"type"`
	// ID uniquely identifies a push and is echoed back in the client ack.
	ID string `json:"id,omitempty"`
	// Data carries the full push payload (the Bark parameters).
	Data interface{} `json:"data,omitempty"`
	// Timestamp is the Unix timestamp (seconds) when the envelope was created.
	Timestamp int64 `json:"timestamp"`
}

// Hub maintains the set of live connections keyed by device key. A single
// device may hold more than one connection (reconnect overlap, multiple
// processes), so each connection gets a unique registration id.
type Hub struct {
	mu      sync.RWMutex
	clients map[string]map[uint64]Conn
	nextID  uint64
}

// New creates an empty hub.
func New() *Hub {
	return &Hub{clients: make(map[string]map[uint64]Conn)}
}

// Add registers a connection for the given device key and returns its
// unique registration id, which must be passed to Remove on disconnect.
func (h *Hub) Add(deviceKey string, conn Conn) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.nextID++
	id := h.nextID
	set := h.clients[deviceKey]
	if set == nil {
		set = make(map[uint64]Conn)
		h.clients[deviceKey] = set
	}
	set[id] = conn
	return id
}

// Remove unregisters a connection. It is safe to call an unknown id.
func (h *Hub) Remove(deviceKey string, id uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	set := h.clients[deviceKey]
	if set == nil {
		return
	}
	delete(set, id)
	if len(set) == 0 {
		delete(h.clients, deviceKey)
	}
}

// Online reports whether the device currently has at least one connection.
func (h *Hub) Online(deviceKey string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients[deviceKey]) > 0
}

// Push writes payload to every live connection of the device. It returns true
// when at least one connection accepted the write, false when the device is
// offline or every write failed.
func (h *Hub) Push(deviceKey string, payload interface{}) bool {
	h.mu.RLock()
	conns := make([]Conn, 0, len(h.clients[deviceKey]))
	for _, conn := range h.clients[deviceKey] {
		conns = append(conns, conn)
	}
	h.mu.RUnlock()

	delivered := false
	for _, conn := range conns {
		if err := conn.WriteJSON(payload); err == nil {
			delivered = true
		}
	}
	return delivered
}

// Default is the hub shared by the HTTP routes.
var Default = New()
