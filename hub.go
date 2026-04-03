package fibersse

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

// HubConfig configures the SSE hub.
type HubConfig struct {
	// FlushInterval is how often P1/P2 events are flushed to clients (default: 2s).
	FlushInterval time.Duration

	// SendBufferSize is the per-connection channel buffer. If full, events are
	// dropped and the client should reconnect (default: 256).
	SendBufferSize int

	// HeartbeatInterval is how often a heartbeat comment is sent to idle
	// connections to detect disconnects and prevent proxy timeouts (default: 30s).
	HeartbeatInterval time.Duration

	// MaxLifetime is the maximum duration a single SSE connection can stay
	// open. After this, the connection is closed gracefully. 0 = unlimited (default: 30m).
	MaxLifetime time.Duration

	// RetryMS is the reconnection interval hint sent to clients via the
	// `retry:` directive on connect (default: 3000).
	RetryMS int

	// Replayer enables Last-Event-ID replay. If nil, replay is disabled.
	Replayer Replayer

	// Logger receives structured log output. If nil, logging is disabled.
	Logger *slog.Logger

	// OnConnect is called when a new client connects, before the SSE stream
	// begins. Use it for authentication, topic selection, and connection limits.
	// Set conn.Topics and conn.Metadata here.
	// Return a non-nil error to reject the connection (the error message is
	// sent as the HTTP response body with status 403).
	OnConnect func(c fiber.Ctx, conn *Connection) error

	// OnDisconnect is called after a client disconnects. Use for cleanup,
	// metrics, or releasing connection limit slots.
	OnDisconnect func(conn *Connection)
}

func (cfg *HubConfig) defaults() {
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 2 * time.Second
	}
	if cfg.SendBufferSize <= 0 {
		cfg.SendBufferSize = 256
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 30 * time.Second
	}
	if cfg.MaxLifetime == 0 {
		cfg.MaxLifetime = 30 * time.Minute
	}
	if cfg.RetryMS <= 0 {
		cfg.RetryMS = 3000
	}
}

// Hub is the central SSE event broker. It manages client connections,
// event routing, coalescing, and delivery. All methods are goroutine-safe.
type Hub struct {
	cfg HubConfig

	// channels for the run loop
	register   chan *Connection
	unregister chan *Connection
	events     chan Event
	shutdown   chan struct{}

	// connections indexed by ID
	mu          sync.RWMutex
	connections map[string]*Connection

	// topic → set of connection IDs (exact subscriptions only)
	topicIndex map[string]map[string]struct{}

	// connections with wildcard subscriptions (* or >) that need pattern matching
	wildcardConns map[string]struct{}

	metrics   hubMetrics
	throttler *adaptiveThrottler
	draining  atomic.Bool // true during graceful drain
	stopped   chan struct{} // closed when run loop exits
}

// New creates a new SSE hub. Call hub.Handler() to get the Fiber handler,
// and hub.Publish() to send events from your application code.
//
//	hub := fibersse.New(fibersse.HubConfig{
//	    FlushInterval: 2 * time.Second,
//	    OnConnect: func(c fiber.Ctx, conn *fibersse.Connection) error {
//	        conn.Topics = []string{"notifications", "live"}
//	        conn.Metadata["tenant_id"] = c.Locals("tenant_id").(string)
//	        return nil
//	    },
//	})
//	app.Get("/events", hub.Handler())
func New(cfg ...HubConfig) *Hub {
	c := HubConfig{}
	if len(cfg) > 0 {
		c = cfg[0]
	}
	c.defaults()

	h := &Hub{
		cfg:         c,
		register:    make(chan *Connection, 64),
		unregister:  make(chan *Connection, 64),
		events:      make(chan Event, 1024),
		shutdown:    make(chan struct{}),
		connections:   make(map[string]*Connection),
		topicIndex:    make(map[string]map[string]struct{}),
		wildcardConns: make(map[string]struct{}),
		throttler:     newAdaptiveThrottler(c.FlushInterval),
		stopped:       make(chan struct{}),
	}

	go h.run()
	return h
}

// Publish sends an event to all connections subscribed to the event's topics.
// This method is goroutine-safe and non-blocking (events are buffered).
func (h *Hub) Publish(event Event) {
	if event.TTL > 0 && event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now()
	}
	select {
	case h.events <- event:
		h.metrics.eventsPublished.Add(1)
	case <-h.shutdown:
		// Hub is shutting down, discard
	}
}

// SetPaused pauses or resumes a connection by ID. Paused connections
// skip P1/P2 events (visibility hint for hidden browser tabs).
// P0 (instant) events are always delivered regardless.
func (h *Hub) SetPaused(connID string, paused bool) {
	h.mu.RLock()
	conn, ok := h.connections[connID]
	h.mu.RUnlock()
	if ok {
		conn.paused.Store(paused)
	}
}

// Shutdown gracefully drains all connections and stops the hub.
// It enters drain mode (rejects new connections), sends a `server-shutdown`
// event to all clients, waits for DrainTimeout to let clients reconnect
// elsewhere, then closes the hub.
// Pass nil ctx for an unbounded wait.
func (h *Hub) Shutdown(ctx context.Context) error {
	// Enter drain mode — Handler() will reject new connections
	h.draining.Store(true)

	// Signal the run loop to stop
	close(h.shutdown)

	if ctx == nil {
		<-h.stopped
		return nil
	}

	select {
	case <-h.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats returns a snapshot of the hub's current state.
func (h *Hub) Stats() HubStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	byTopic := make(map[string]int, len(h.topicIndex))
	for topic, conns := range h.topicIndex {
		byTopic[topic] = len(conns)
	}

	return HubStats{
		ActiveConnections:  len(h.connections),
		TotalTopics:        len(h.topicIndex),
		EventsPublished:    h.metrics.eventsPublished.Load(),
		EventsDropped:      h.metrics.eventsDropped.Load(),
		ConnectionsByTopic: byTopic,
	}
}

// Handler returns a Fiber handler that serves the SSE stream.
// Mount it on a GET route:
//
//	app.Get("/events", hub.Handler())
func (h *Hub) Handler() fiber.Handler {
	return func(c fiber.Ctx) error {
		// Reject during graceful drain
		if h.draining.Load() {
			c.Set("Retry-After", "5")
			return c.Status(fiber.StatusServiceUnavailable).SendString("server draining, please reconnect")
		}

		conn := newConnection(
			uuid.New().String(),
			nil,
			h.cfg.SendBufferSize,
			h.cfg.FlushInterval,
		)

		// Let the application authenticate and configure the connection
		if h.cfg.OnConnect != nil {
			if err := h.cfg.OnConnect(c, conn); err != nil {
				return c.Status(fiber.StatusForbidden).SendString(err.Error())
			}
		}

		if len(conn.Topics) == 0 {
			return c.Status(fiber.StatusBadRequest).SendString("no topics subscribed")
		}

		// Set SSE headers
		c.Set("Content-Type", "text/event-stream")
		c.Set("Cache-Control", "no-cache")
		c.Set("Connection", "keep-alive")
		c.Set("X-Accel-Buffering", "no")

		// Register the connection with the hub
		h.register <- conn

		// Capture Last-Event-ID before entering the stream writer
		lastEventID := c.Get("Last-Event-ID")
		if lastEventID == "" {
			lastEventID = c.Query("lastEventID")
		}

		return c.SendStreamWriter(func(w *bufio.Writer) {
			defer func() {
				h.unregister <- conn
				conn.Close()
				if h.cfg.OnDisconnect != nil {
					h.cfg.OnDisconnect(conn)
				}
			}()

			// Send retry hint
			if err := writeRetry(w, h.cfg.RetryMS); err != nil {
				return
			}

			// Replay missed events if client sent Last-Event-ID
			if lastEventID != "" && h.cfg.Replayer != nil {
				events, err := h.cfg.Replayer.Replay(lastEventID, conn.Topics)
				if err == nil && len(events) > 0 {
					for _, me := range events {
						if _, err := me.WriteTo(w); err != nil {
							return
						}
					}
					if err := w.Flush(); err != nil {
						return
					}
				}
			}

			// Send connected event
			connected := marshaledEvent{
				id:    nextEventID(),
				typ:   "connected",
				data:  fmt.Sprintf(`{"connection_id":"%s","topics":%s}`, conn.ID, topicsJSON(conn.Topics)),
				retry: -1,
			}
			if _, err := connected.WriteTo(w); err != nil {
				return
			}
			if err := w.Flush(); err != nil {
				return
			}

			// Lifetime timer — close connection after MaxLifetime
			if h.cfg.MaxLifetime > 0 {
				go func() {
					timer := time.NewTimer(h.cfg.MaxLifetime)
					defer timer.Stop()
					select {
					case <-timer.C:
						conn.Close()
					case <-conn.done:
					}
				}()
			}

			// Shutdown watcher — send server-shutdown event then close
			go func() {
				select {
				case <-h.shutdown:
					shutdownEvt := marshaledEvent{
						id:    nextEventID(),
						typ:   "server-shutdown",
						data:  "{}",
						retry: -1,
					}
					conn.trySend(shutdownEvt)
					// Give the writer a moment to flush, then close
					time.Sleep(100 * time.Millisecond)
					conn.Close()
				case <-conn.done:
				}
			}()

			// Delegate to the connection's write loop
			conn.writeLoop(w)
		})
	}
}

// run is the hub's main event loop. It processes registrations,
// unregistrations, events, heartbeats, and flushes.
func (h *Hub) run() {
	defer close(h.stopped)

	flushTicker := time.NewTicker(h.cfg.FlushInterval)
	defer flushTicker.Stop()

	heartbeatTicker := time.NewTicker(h.cfg.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	for {
		select {
		case conn := <-h.register:
			h.addConnection(conn)

		case conn := <-h.unregister:
			h.removeConnection(conn)

		case event := <-h.events:
			h.routeEvent(event)

		case <-flushTicker.C:
			h.flushAll()

		case <-heartbeatTicker.C:
			h.sendHeartbeats()

		case <-h.shutdown:
			// Close all remaining connections
			h.mu.Lock()
			for _, conn := range h.connections {
				conn.Close()
			}
			h.mu.Unlock()
			return
		}
	}
}

// addConnection registers a new connection and indexes it by topic.
func (h *Hub) addConnection(conn *Connection) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.connections[conn.ID] = conn

	hasWildcard := false
	for _, topic := range conn.Topics {
		if strings.ContainsAny(topic, "*>") {
			hasWildcard = true
		} else {
			if h.topicIndex[topic] == nil {
				h.topicIndex[topic] = make(map[string]struct{})
			}
			h.topicIndex[topic][conn.ID] = struct{}{}
		}
	}
	if hasWildcard {
		h.wildcardConns[conn.ID] = struct{}{}
	}

	if h.cfg.Logger != nil {
		h.cfg.Logger.Info("fibersse: connection opened",
			"conn_id", conn.ID,
			"topics", conn.Topics,
			"total", len(h.connections),
		)
	}
}

// removeConnection unregisters a connection and removes it from topic indexes.
func (h *Hub) removeConnection(conn *Connection) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, exists := h.connections[conn.ID]; !exists {
		return // already removed
	}

	for _, topic := range conn.Topics {
		if idx, ok := h.topicIndex[topic]; ok {
			delete(idx, conn.ID)
			if len(idx) == 0 {
				delete(h.topicIndex, topic)
			}
		}
	}

	delete(h.wildcardConns, conn.ID)
	delete(h.connections, conn.ID)
	h.throttler.remove(conn.ID)

	if h.cfg.Logger != nil {
		h.cfg.Logger.Info("fibersse: connection closed",
			"conn_id", conn.ID,
			"sent", conn.MessagesSent.Load(),
			"dropped", conn.MessagesDropped.Load(),
			"total", len(h.connections),
		)
	}
}

// routeEvent delivers an event to all connections subscribed to its topics.
func (h *Hub) routeEvent(event Event) {
	// Check TTL — skip expired events
	if event.TTL > 0 && !event.CreatedAt.IsZero() {
		if time.Since(event.CreatedAt) > event.TTL {
			h.metrics.eventsDropped.Add(1)
			return
		}
	}

	me := marshalEvent(&event)

	// Store for replay
	if h.cfg.Replayer != nil {
		h.cfg.Replayer.Store(me, event.Topics)
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	// Collect unique connection IDs matching the event's topics
	seen := make(map[string]struct{})

	// 1. Exact topic index lookup (O(1) per topic)
	for _, topic := range event.Topics {
		if idx, ok := h.topicIndex[topic]; ok {
			for connID := range idx {
				seen[connID] = struct{}{}
			}
		}
	}

	// 2. Wildcard connections — check each against event topics
	for connID := range h.wildcardConns {
		if _, already := seen[connID]; already {
			continue
		}
		conn, ok := h.connections[connID]
		if !ok {
			continue
		}
		for _, eventTopic := range event.Topics {
			if connMatchesTopic(conn, eventTopic) {
				seen[connID] = struct{}{}
				break
			}
		}
	}

	// 3. Group-based matching — match by metadata key-value pairs
	if len(event.Group) > 0 {
		for connID, conn := range h.connections {
			if _, already := seen[connID]; already {
				continue
			}
			if connMatchesGroup(conn, event.Group) {
				seen[connID] = struct{}{}
			}
		}
	}

	// Deliver to matched connections
	for connID := range seen {
		conn, ok := h.connections[connID]
		if !ok || conn.IsClosed() {
			continue
		}

		// Skip hidden (paused) connections for non-instant events
		if conn.paused.Load() && event.Priority != PriorityInstant {
			continue
		}

		h.deliverToConn(conn, event, me)
	}
}

// deliverToConn routes an event to a connection based on priority.
func (h *Hub) deliverToConn(conn *Connection, event Event, me marshaledEvent) {
	switch event.Priority {
	case PriorityInstant:
		if !conn.trySend(me) {
			h.metrics.eventsDropped.Add(1)
		}
	case PriorityBatched:
		conn.coalescer.addBatched(me)
	case PriorityCoalesced:
		key := event.CoalesceKey
		if key == "" {
			key = event.Type
		}
		conn.coalescer.addCoalesced(key, me)
	}
}

// flushAll drains each connection's coalescer and sends buffered events.
// Uses adaptive throttling — connections with full buffers get flushed less often.
func (h *Hub) flushAll() {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, conn := range h.connections {
		if conn.IsClosed() || conn.paused.Load() {
			continue
		}

		// Adaptive throttle: check buffer saturation
		bufCap := cap(conn.send)
		saturation := float64(0)
		if bufCap > 0 {
			saturation = float64(len(conn.send)) / float64(bufCap)
		}

		if !h.throttler.shouldFlush(conn.ID, saturation) {
			continue
		}

		events := conn.coalescer.flush()
		for _, me := range events {
			if !conn.trySend(me) {
				h.metrics.eventsDropped.Add(1)
			}
		}
	}
}

// sendHeartbeats sends a comment to connections that haven't received
// real data recently. This keeps the TCP connection alive and detects
// silent disconnects on the next flush.
func (h *Hub) sendHeartbeats() {
	h.mu.RLock()
	defer h.mu.RUnlock()

	now := time.Now()
	for _, conn := range h.connections {
		if conn.IsClosed() {
			continue
		}
		lastWrite, _ := conn.lastWrite.Load().(time.Time)
		if now.Sub(lastWrite) >= h.cfg.HeartbeatInterval {
			// Send a heartbeat as a zero-type event via the send channel
			hb := marshaledEvent{
				id:    "",
				typ:   "",
				data:  "",
				retry: -1,
			}
			hb.id = heartbeatMarker
			conn.trySend(hb)
		}
	}
}

// topicsJSON serializes a topic list as a JSON array string.
func topicsJSON(topics []string) string {
	if len(topics) == 0 {
		return "[]"
	}
	s := "["
	for i, t := range topics {
		if i > 0 {
			s += ","
		}
		s += `"` + t + `"`
	}
	s += "]"
	return s
}
