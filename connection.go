package fibersse

import (
	"bufio"
	"sync"
	"sync/atomic"
	"time"
)

// Connection represents a single SSE client connection managed by the hub.
type Connection struct {
	// ID is a unique identifier for this connection (UUID).
	ID string

	// Topics this connection is subscribed to.
	Topics []string

	// Metadata stores arbitrary key-value pairs (tenant_id, user_id, etc.).
	// Set during OnConnect callback. Read-only after connection is established.
	Metadata map[string]string

	// CreatedAt is when the connection was established.
	CreatedAt time.Time

	// MessagesSent is the total number of events successfully written.
	MessagesSent atomic.Int64

	// MessagesDropped is the count of events dropped due to backpressure.
	MessagesDropped atomic.Int64

	// LastEventID tracks the last event ID sent to this connection.
	LastEventID atomic.Value // stores string

	// send is the buffered channel for delivering events to the writer goroutine.
	// If the channel is full, events are dropped (backpressure).
	send chan marshaledEvent

	// done is closed when the connection is terminated (client disconnect or shutdown).
	done chan struct{}
	once sync.Once

	// lastWrite tracks when the last real event (not heartbeat) was sent.
	// Used by the hub to skip heartbeats for active connections.
	lastWrite atomic.Value // stores time.Time

	// paused is true when the client has indicated its tab is hidden.
	// P1/P2 events are skipped for paused connections; P0 still delivers.
	paused atomic.Bool

	// coalescer holds per-connection buffering state for P1/P2 events.
	coalescer *coalescer
}

// newConnection creates a Connection with the given buffer size.
func newConnection(id string, topics []string, bufferSize int, flushInterval time.Duration) *Connection {
	c := &Connection{
		ID:        id,
		Topics:    topics,
		Metadata:  make(map[string]string),
		CreatedAt: time.Now(),
		send:      make(chan marshaledEvent, bufferSize),
		done:      make(chan struct{}),
	}
	c.lastWrite.Store(time.Now())
	c.LastEventID.Store("")
	c.coalescer = newCoalescer(flushInterval)
	return c
}

// Close terminates the connection. Safe to call multiple times.
func (c *Connection) Close() {
	c.once.Do(func() {
		close(c.done)
	})
}

// IsClosed returns true if the connection has been terminated.
func (c *Connection) IsClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// trySend attempts to deliver an event to the connection's send channel.
// Returns false if the buffer is full (backpressure).
func (c *Connection) trySend(me marshaledEvent) bool {
	select {
	case c.send <- me:
		return true
	default:
		c.MessagesDropped.Add(1)
		return false
	}
}

// heartbeatMarker is a sentinel ID used internally to signal that a
// send-channel message is a heartbeat comment, not a real event.
const heartbeatMarker = "__heartbeat__"

// writeLoop runs inside Fiber's SendStreamWriter. It reads from the send
// channel and writes SSE-formatted events to the bufio.Writer. It returns
// when the connection is closed or a write/flush error occurs (client disconnect).
func (c *Connection) writeLoop(w *bufio.Writer) {
	for {
		select {
		case <-c.done:
			return
		case me, ok := <-c.send:
			if !ok {
				return
			}
			// Heartbeat: write as a comment, not a data event
			if me.id == heartbeatMarker {
				if err := writeComment(w, "heartbeat"); err != nil {
					c.Close()
					return
				}
				if err := w.Flush(); err != nil {
					c.Close()
					return
				}
				continue
			}
			if _, err := me.WriteTo(w); err != nil {
				c.Close()
				return
			}
			if err := w.Flush(); err != nil {
				c.Close()
				return
			}
			c.MessagesSent.Add(1)
			c.lastWrite.Store(time.Now())
			if me.id != "" {
				c.LastEventID.Store(me.id)
			}
		}
	}
}

// topicSet returns the connection's topics as a set for fast lookup.
func (c *Connection) topicSet() map[string]struct{} {
	s := make(map[string]struct{}, len(c.Topics))
	for _, t := range c.Topics {
		s[t] = struct{}{}
	}
	return s
}

// connMatchesGroup returns true if ALL key-value pairs in the group
// match the connection's metadata.
func connMatchesGroup(conn *Connection, group map[string]string) bool {
	for k, v := range group {
		if conn.Metadata[k] != v {
			return false
		}
	}
	return true
}
