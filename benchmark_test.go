package fibersse

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

// ──────────────────────────────────────────────────────────────────────────────
// Hub Publish Benchmarks
// ──────────────────────────────────────────────────────────────────────────────

// BenchmarkPublish measures the overhead of publishing events to a hub with
// no subscribers. This is the "no-op" path — events enter the channel and
// get routed to zero connections.
func BenchmarkPublish(b *testing.B) {
	hub := New(HubConfig{
		FlushInterval:     time.Minute, // effectively disabled
		HeartbeatInterval: time.Minute,
	})
	defer hub.Shutdown(context.TODO())

	evt := Event{
		Type:     "notification",
		Data:     `{"title":"hello"}`,
		Topics:   []string{"notifications"},
		Priority: PriorityInstant,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hub.Publish(evt)
	}
}

// BenchmarkPublish_1000Conns measures publishing to a hub with 1000 registered
// connections all subscribed to the same topic.
func BenchmarkPublish_1000Conns(b *testing.B) {
	hub := New(HubConfig{
		FlushInterval:     time.Minute,
		HeartbeatInterval: time.Minute,
		SendBufferSize:    1024,
	})
	defer hub.Shutdown(context.TODO())

	// Register 1000 fake connections
	for i := 0; i < 1000; i++ {
		conn := newConnection(
			fmt.Sprintf("conn_%d", i),
			[]string{"notifications"},
			1024,
			time.Minute,
		)
		hub.register <- conn
	}

	// Wait for all registrations to be processed
	time.Sleep(200 * time.Millisecond)

	evt := Event{
		Type:     "notification",
		Data:     `{"title":"hello"}`,
		Topics:   []string{"notifications"},
		Priority: PriorityInstant,
	}

	// Drain connections in background to prevent backpressure
	done := make(chan struct{})
	go func() {
		defer close(done)
		hub.mu.RLock()
		conns := make([]*Connection, 0, len(hub.connections))
		for _, c := range hub.connections {
			conns = append(conns, c)
		}
		hub.mu.RUnlock()

		for {
			select {
			case <-done:
				return
			default:
			}
			for _, c := range conns {
				select {
				case <-c.send:
				default:
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hub.Publish(evt)
	}
	b.StopTimer()
}

// ──────────────────────────────────────────────────────────────────────────────
// Coalescer Benchmarks
// ──────────────────────────────────────────────────────────────────────────────

// BenchmarkCoalescer_Add measures the cost of adding events to a coalescer.
func BenchmarkCoalescer_Add(b *testing.B) {
	c := newCoalescer(2 * time.Second)
	me := MarshaledEvent{ID: "evt_1", Type: "progress", Data: `{"pct":50}`}

	b.Run("Batched", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c.addBatched(me)
		}
		// Drain to prevent unbounded growth
		c.flush()
	})

	b.Run("Coalesced_SameKey", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c.addCoalesced("task:123", me)
		}
		c.flush()
	})

	b.Run("Coalesced_UniqueKeys", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c.addCoalesced(fmt.Sprintf("task:%d", i), me)
		}
		c.flush()
	})
}

// BenchmarkCoalescer_Flush measures flushing a coalescer with varying numbers
// of buffered events.
func BenchmarkCoalescer_Flush(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("Batched_%d", n), func(b *testing.B) {
			me := MarshaledEvent{ID: "evt_1", Data: "x"}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c := newCoalescer(2 * time.Second)
				for j := 0; j < n; j++ {
					c.addBatched(me)
				}
				_ = c.flush()
			}
		})

		b.Run(fmt.Sprintf("Coalesced_%d", n), func(b *testing.B) {
			me := MarshaledEvent{ID: "evt_1", Data: "x"}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c := newCoalescer(2 * time.Second)
				for j := 0; j < n; j++ {
					c.addCoalesced(fmt.Sprintf("key:%d", j), me)
				}
				_ = c.flush()
			}
		})
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Topic Match Benchmarks
// ──────────────────────────────────────────────────────────────────────────────

// BenchmarkTopicMatch_Exact measures exact (no wildcard) topic matching.
func BenchmarkTopicMatch_Exact(b *testing.B) {
	b.Run("Short", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			topicMatch("notifications", "notifications")
		}
	})

	b.Run("Long", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			topicMatch("app.tenant.orders.items.details", "app.tenant.orders.items.details")
		}
	})

	b.Run("Mismatch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			topicMatch("notifications", "analytics")
		}
	})
}

// BenchmarkTopicMatch_Wildcard measures wildcard topic matching.
func BenchmarkTopicMatch_Wildcard(b *testing.B) {
	b.Run("Star_Match", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			topicMatch("notifications.*", "notifications.orders")
		}
	})

	b.Run("Star_NoMatch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			topicMatch("notifications.*", "analytics.orders")
		}
	})

	b.Run("GreaterThan_Match", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			topicMatch("analytics.>", "analytics.live.visitors.count")
		}
	})

	b.Run("GreaterThan_NoMatch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			topicMatch("analytics.>", "notifications.live")
		}
	})

	b.Run("Complex_Pattern", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			topicMatch("app.*.orders.>", "app.tenant1.orders.items.detail")
		}
	})
}

// ──────────────────────────────────────────────────────────────────────────────
// Marshal Benchmarks
// ──────────────────────────────────────────────────────────────────────────────

// BenchmarkMarshalEvent measures marshaling an Event to its wire-ready
// MarshaledEvent representation.
func BenchmarkMarshalEvent(b *testing.B) {
	b.Run("StringData", func(b *testing.B) {
		e := &Event{
			Type:   "notification",
			Data:   `{"title":"Hello","body":"World"}`,
			ID:     "evt_1",
			Topics: []string{"notifications"},
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			marshalEvent(e)
		}
	})

	b.Run("MapData", func(b *testing.B) {
		e := &Event{
			Type:   "notification",
			Data:   map[string]string{"title": "Hello", "body": "World"},
			ID:     "evt_1",
			Topics: []string{"notifications"},
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			marshalEvent(e)
		}
	})

	b.Run("StructData", func(b *testing.B) {
		type payload struct {
			Title string `json:"title"`
			Body  string `json:"body"`
			Count int    `json:"count"`
		}
		e := &Event{
			Type:   "notification",
			Data:   payload{Title: "Hello", Body: "World", Count: 42},
			ID:     "evt_1",
			Topics: []string{"notifications"},
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			marshalEvent(e)
		}
	})

	b.Run("AutoID", func(b *testing.B) {
		e := &Event{
			Type: "notification",
			Data: "hello",
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			marshalEvent(e)
		}
	})
}

// BenchmarkMarshaledEvent_WriteTo measures writing a MarshaledEvent to the
// SSE wire format.
func BenchmarkMarshaledEvent_WriteTo(b *testing.B) {
	b.Run("Simple", func(b *testing.B) {
		me := MarshaledEvent{
			ID:    "evt_42",
			Type:  "notification",
			Data:  `{"title":"Hello"}`,
			Retry: -1,
		}
		var buf bytes.Buffer
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			me.WriteTo(&buf)
		}
	})

	b.Run("Multiline", func(b *testing.B) {
		me := MarshaledEvent{
			ID:    "evt_42",
			Type:  "notification",
			Data:  "line1\nline2\nline3\nline4\nline5",
			Retry: -1,
		}
		var buf bytes.Buffer
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			me.WriteTo(&buf)
		}
	})

	b.Run("WithRetry", func(b *testing.B) {
		me := MarshaledEvent{
			ID:    "evt_42",
			Type:  "notification",
			Data:  `{"title":"Hello"}`,
			Retry: 3000,
		}
		var buf bytes.Buffer
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			me.WriteTo(&buf)
		}
	})

	b.Run("LargePayload", func(b *testing.B) {
		// ~1KB payload
		data := `{"title":"Hello World","description":"This is a much longer description that simulates a real-world payload with more content than a simple test case. It includes various fields and data that would be typical in a production SSE event.","items":[{"id":1,"name":"Item 1","price":19.99},{"id":2,"name":"Item 2","price":29.99},{"id":3,"name":"Item 3","price":39.99}],"metadata":{"source":"api","version":"v2","timestamp":"2025-01-15T10:30:00Z"}}`
		me := MarshaledEvent{
			ID:    "evt_42",
			Type:  "notification",
			Data:  data,
			Retry: -1,
		}
		var buf bytes.Buffer
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			me.WriteTo(&buf)
		}
	})
}

// ──────────────────────────────────────────────────────────────────────────────
// Connection Benchmarks
// ──────────────────────────────────────────────────────────────────────────────

// BenchmarkConnection_TrySend measures the cost of sending events through
// a connection's channel.
func BenchmarkConnection_TrySend(b *testing.B) {
	conn := newConnection("bench", []string{"t"}, 4096, time.Second)

	me := MarshaledEvent{ID: "evt_1", Data: "hello"}

	// Drain the channel in background
	go func() {
		for range conn.send {
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.trySend(me)
	}
	b.StopTimer()
	conn.Close()
}

// BenchmarkConnection_TrySend_Backpressure measures trySend when the buffer
// is full (drop path).
func BenchmarkConnection_TrySend_Backpressure(b *testing.B) {
	conn := newConnection("bench", []string{"t"}, 1, time.Second) // buffer of 1

	// Fill the buffer
	conn.trySend(MarshaledEvent{ID: "fill"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.trySend(MarshaledEvent{ID: "drop"})
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Throttler Benchmarks
// ──────────────────────────────────────────────────────────────────────────────

// BenchmarkAdaptiveThrottler_ShouldFlush measures the shouldFlush decision.
func BenchmarkAdaptiveThrottler_ShouldFlush(b *testing.B) {
	at := newAdaptiveThrottler(2 * time.Second)

	b.Run("SingleConn", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			at.shouldFlush("conn_0", 0.3)
		}
	})

	b.Run("ManyConns", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			at.shouldFlush(fmt.Sprintf("conn_%d", i%1000), 0.3)
		}
	})
}

// BenchmarkAdaptiveThrottler_EffectiveInterval measures the interval calculation.
func BenchmarkAdaptiveThrottler_EffectiveInterval(b *testing.B) {
	at := newAdaptiveThrottler(2 * time.Second)

	saturations := []float64{0.05, 0.3, 0.6, 0.9}
	for _, s := range saturations {
		b.Run(fmt.Sprintf("Saturation_%.0f%%", s*100), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				at.effectiveInterval(s)
			}
		})
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Memory Replayer Benchmarks
// ──────────────────────────────────────────────────────────────────────────────

// BenchmarkMemoryReplayer_Store measures storing events in the replayer.
func BenchmarkMemoryReplayer_Store(b *testing.B) {
	r := NewMemoryReplayer(MemoryReplayerConfig{MaxEvents: 10000, TTL: time.Minute})
	me := MarshaledEvent{ID: "evt_1", Type: "test", Data: "hello"}
	topics := []string{"notifications"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		me.ID = fmt.Sprintf("evt_%d", i)
		r.Store(me, topics)
	}
}

// BenchmarkMemoryReplayer_Replay measures replaying events.
func BenchmarkMemoryReplayer_Replay(b *testing.B) {
	r := NewMemoryReplayer(MemoryReplayerConfig{MaxEvents: 10000, TTL: time.Minute})

	// Pre-fill with 1000 events
	for i := 0; i < 1000; i++ {
		r.Store(MarshaledEvent{
			ID:   fmt.Sprintf("evt_%d", i),
			Type: "test",
			Data: "hello",
		}, []string{"notifications"})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Replay from the 500th event — should return ~500 events
		r.Replay("evt_500", []string{"notifications"})
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Group Matching Benchmarks
// ──────────────────────────────────────────────────────────────────────────────

// BenchmarkConnMatchesGroup measures metadata-based group matching.
func BenchmarkConnMatchesGroup(b *testing.B) {
	conn := newConnection("test", []string{"t"}, 10, time.Second)
	conn.Metadata["tenant_id"] = "t_123"
	conn.Metadata["plan"] = "pro"
	conn.Metadata["region"] = "us-east-1"

	b.Run("SingleKey_Match", func(b *testing.B) {
		group := map[string]string{"tenant_id": "t_123"}
		for i := 0; i < b.N; i++ {
			connMatchesGroup(conn, group)
		}
	})

	b.Run("MultiKey_Match", func(b *testing.B) {
		group := map[string]string{"tenant_id": "t_123", "plan": "pro"}
		for i := 0; i < b.N; i++ {
			connMatchesGroup(conn, group)
		}
	})

	b.Run("SingleKey_NoMatch", func(b *testing.B) {
		group := map[string]string{"tenant_id": "t_999"}
		for i := 0; i < b.N; i++ {
			connMatchesGroup(conn, group)
		}
	})
}
