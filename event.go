package fibersse

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"
)

// Priority controls how an event is delivered to clients.
type Priority int

const (
	// PriorityInstant bypasses all buffering — the event is written to the
	// client connection immediately. Use for errors, auth revocations,
	// force-refresh commands, and chat messages.
	PriorityInstant Priority = 0

	// PriorityBatched collects events in a time window (FlushInterval) and
	// sends them all at once. Use for status changes, media updates.
	PriorityBatched Priority = 1

	// PriorityCoalesced uses last-writer-wins per CoalesceKey. Multiple
	// events with the same key within a flush window are merged — only the
	// latest is sent. Use for progress bars, live counters, typing indicators.
	PriorityCoalesced Priority = 2
)

// Event represents a single SSE event to be published through the hub.
type Event struct {
	// Type maps to the SSE `event:` field. If empty, the client receives
	// a generic "message" event.
	Type string

	// Data is the payload. If it implements json.Marshaler or is a string/[]byte,
	// it is used directly. Otherwise it is JSON-encoded.
	Data any

	// ID is the SSE event ID (`id:` field). Used for Last-Event-ID replay.
	// If empty, an auto-incrementing ID is assigned by the hub.
	ID string

	// Topics lists which topic channels should receive this event.
	// A connection subscribed to any matching topic will receive it.
	Topics []string

	// Priority controls delivery timing. See Priority constants.
	Priority Priority

	// CoalesceKey is used when Priority == PriorityCoalesced. Events with
	// the same CoalesceKey within a flush window are merged (last wins).
	// Ignored for other priority levels.
	CoalesceKey string

	// TTL is the maximum age of this event. If the event is older than TTL
	// when delivery is attempted, it is dropped. Zero means no expiration.
	TTL time.Duration

	// CreatedAt is when the event was created. Used with TTL for staleness
	// checks. If zero and TTL is set, it defaults to time.Now() on Publish().
	CreatedAt time.Time

	// Group targets connections by metadata key-value instead of (or in
	// addition to) topics. For example, Group{"tenant_id": "t_123"} delivers
	// to all connections whose Metadata["tenant_id"] == "t_123".
	// If both Topics and Group are set, the event is delivered to the union.
	Group map[string]string
}

// globalEventID is an auto-incrementing counter for event IDs.
var globalEventID atomic.Uint64

// nextEventID returns a monotonically increasing event ID string.
func nextEventID() string {
	return fmt.Sprintf("evt_%d", globalEventID.Add(1))
}

// MarshaledEvent is the wire-ready representation of an SSE event,
// produced by marshaling an Event's Data field. External Replayer
// implementations receive and return this type.
type MarshaledEvent struct {
	ID    string
	Type  string
	Data  string
	Retry int // -1 means omit
}

// marshalEvent converts an Event into wire-ready format.
func marshalEvent(e *Event) MarshaledEvent {
	me := MarshaledEvent{
		ID:    e.ID,
		Type:  e.Type,
		Retry: -1,
	}

	if me.ID == "" {
		me.ID = nextEventID()
	}

	switch v := e.Data.(type) {
	case nil:
		me.Data = ""
	case string:
		me.Data = v
	case []byte:
		me.Data = string(v)
	case json.Marshaler:
		b, err := v.MarshalJSON()
		if err != nil {
			me.Data = fmt.Sprintf(`{"error":"marshal failed: %s"}`, err)
		} else {
			me.Data = string(b)
		}
	default:
		b, err := json.Marshal(v)
		if err != nil {
			me.Data = fmt.Sprintf(`{"error":"marshal failed: %s"}`, err)
		} else {
			me.Data = string(b)
		}
	}

	return me
}

// WriteTo writes the SSE-formatted event to w. The format follows the
// Server-Sent Events specification:
//
//	id: <id>
//	event: <type>
//	data: <line1>
//	data: <line2>
//	<blank line>
func (me *MarshaledEvent) WriteTo(w io.Writer) (int64, error) {
	var total int64

	// id: field
	if me.ID != "" {
		n, err := fmt.Fprintf(w, "id: %s\n", me.ID)
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	// event: field
	if me.Type != "" {
		n, err := fmt.Fprintf(w, "event: %s\n", me.Type)
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	// retry: field
	if me.Retry >= 0 {
		n, err := fmt.Fprintf(w, "retry: %d\n", me.Retry)
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	// data: field — handle multiline payloads
	if me.Data != "" {
		lines := strings.Split(me.Data, "\n")
		for _, line := range lines {
			n, err := fmt.Fprintf(w, "data: %s\n", line)
			total += int64(n)
			if err != nil {
				return total, err
			}
		}
	} else {
		n, err := fmt.Fprint(w, "data: \n")
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	// Blank line terminates the event
	n, err := fmt.Fprint(w, "\n")
	total += int64(n)
	return total, err
}

// writeComment writes an SSE comment line (`: <text>\n`).
// Comments are used for heartbeats and are ignored by EventSource clients.
func writeComment(w io.Writer, text string) error {
	_, err := fmt.Fprintf(w, ": %s\n\n", text)
	return err
}

// writeRetry writes the retry directive to set the client reconnection interval.
func writeRetry(w io.Writer, ms int) error {
	_, err := fmt.Fprintf(w, "retry: %d\n\n", ms)
	return err
}
