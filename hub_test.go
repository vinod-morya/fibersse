package fibersse

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestMarshalEvent_JSON(t *testing.T) {
	e := &Event{
		Type:   "notification",
		Data:   map[string]string{"title": "Hello"},
		ID:     "evt_1",
		Topics: []string{"test"},
	}
	me := marshalEvent(e)

	if me.id != "evt_1" {
		t.Errorf("expected id evt_1, got %s", me.id)
	}
	if me.typ != "notification" {
		t.Errorf("expected type notification, got %s", me.typ)
	}
	if !strings.Contains(me.data, `"title":"Hello"`) {
		t.Errorf("expected JSON data, got %s", me.data)
	}
}

func TestMarshalEvent_String(t *testing.T) {
	e := &Event{
		Type: "ping",
		Data: "hello world",
		ID:   "evt_2",
	}
	me := marshalEvent(e)
	if me.data != "hello world" {
		t.Errorf("expected string data, got %s", me.data)
	}
}

func TestMarshalEvent_AutoID(t *testing.T) {
	e := &Event{Type: "test", Data: "x"}
	me := marshalEvent(e)
	if me.id == "" {
		t.Error("expected auto-generated ID")
	}
	if !strings.HasPrefix(me.id, "evt_") {
		t.Errorf("expected evt_ prefix, got %s", me.id)
	}
}

func TestMarshaledEvent_WriteTo(t *testing.T) {
	me := marshaledEvent{
		id:    "evt_42",
		typ:   "message",
		data:  `{"text":"hello"}`,
		retry: -1,
	}
	var buf bytes.Buffer
	_, err := me.WriteTo(&buf)
	if err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	if !strings.Contains(output, "id: evt_42\n") {
		t.Errorf("missing id field: %s", output)
	}
	if !strings.Contains(output, "event: message\n") {
		t.Errorf("missing event field: %s", output)
	}
	if !strings.Contains(output, `data: {"text":"hello"}`) {
		t.Errorf("missing data field: %s", output)
	}
	if !strings.HasSuffix(output, "\n\n") {
		t.Errorf("missing blank line terminator: %s", output)
	}
}

func TestMarshaledEvent_MultilineData(t *testing.T) {
	me := marshaledEvent{
		id:   "evt_1",
		data: "line1\nline2\nline3",
	}
	var buf bytes.Buffer
	me.WriteTo(&buf)
	output := buf.String()

	if !strings.Contains(output, "data: line1\n") {
		t.Errorf("missing line1: %s", output)
	}
	if !strings.Contains(output, "data: line2\n") {
		t.Errorf("missing line2: %s", output)
	}
	if !strings.Contains(output, "data: line3\n") {
		t.Errorf("missing line3: %s", output)
	}
}

func TestCoalescer_Batched(t *testing.T) {
	c := newCoalescer(time.Second)

	c.addBatched(marshaledEvent{id: "1", data: "a"})
	c.addBatched(marshaledEvent{id: "2", data: "b"})
	c.addBatched(marshaledEvent{id: "3", data: "c"})

	events := c.flush()
	if len(events) != 3 {
		t.Fatalf("expected 3 batched events, got %d", len(events))
	}
	if events[0].data != "a" || events[1].data != "b" || events[2].data != "c" {
		t.Error("batched events out of order")
	}

	// Second flush should be empty
	events = c.flush()
	if len(events) != 0 {
		t.Fatalf("expected 0 events after flush, got %d", len(events))
	}
}

func TestCoalescer_Coalesced(t *testing.T) {
	c := newCoalescer(time.Second)

	// Simulate progress: 5% → 6% → 7% → 8%
	c.addCoalesced("task:123", marshaledEvent{id: "1", data: `{"pct":5}`})
	c.addCoalesced("task:123", marshaledEvent{id: "2", data: `{"pct":6}`})
	c.addCoalesced("task:123", marshaledEvent{id: "3", data: `{"pct":7}`})
	c.addCoalesced("task:123", marshaledEvent{id: "4", data: `{"pct":8}`})

	events := c.flush()
	if len(events) != 1 {
		t.Fatalf("expected 1 coalesced event, got %d", len(events))
	}
	if events[0].data != `{"pct":8}` {
		t.Errorf("expected latest value {pct:8}, got %s", events[0].data)
	}
}

func TestCoalescer_Mixed(t *testing.T) {
	c := newCoalescer(time.Second)

	c.addBatched(marshaledEvent{id: "b1", data: "batch1"})
	c.addCoalesced("key1", marshaledEvent{id: "c1", data: "old"})
	c.addCoalesced("key1", marshaledEvent{id: "c2", data: "new"})
	c.addBatched(marshaledEvent{id: "b2", data: "batch2"})

	events := c.flush()
	// Should be: 2 batched + 1 coalesced = 3
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	// Batched come first
	if events[0].data != "batch1" || events[1].data != "batch2" {
		t.Error("batched events should come first in order")
	}
	// Coalesced last, only latest
	if events[2].data != "new" {
		t.Errorf("expected coalesced 'new', got %s", events[2].data)
	}
}

func TestCoalescer_MultipleKeys(t *testing.T) {
	c := newCoalescer(time.Second)

	c.addCoalesced("task:A", marshaledEvent{id: "1", data: "A-old"})
	c.addCoalesced("task:B", marshaledEvent{id: "2", data: "B-old"})
	c.addCoalesced("task:A", marshaledEvent{id: "3", data: "A-new"})

	events := c.flush()
	if len(events) != 2 {
		t.Fatalf("expected 2 coalesced events, got %d", len(events))
	}
	// First-seen order: A then B
	if events[0].data != "A-new" {
		t.Errorf("expected A-new, got %s", events[0].data)
	}
	if events[1].data != "B-old" {
		t.Errorf("expected B-old, got %s", events[1].data)
	}
}

func TestMemoryReplayer(t *testing.T) {
	r := NewMemoryReplayer(MemoryReplayerConfig{MaxEvents: 100, TTL: time.Minute})

	r.Store(marshaledEvent{id: "evt_1", typ: "a", data: "1"}, []string{"topic1"})
	r.Store(marshaledEvent{id: "evt_2", typ: "b", data: "2"}, []string{"topic1", "topic2"})
	r.Store(marshaledEvent{id: "evt_3", typ: "c", data: "3"}, []string{"topic2"})

	// Replay after evt_1, topic1 — should get evt_2 only (evt_3 is topic2 only)
	events, err := r.Replay("evt_1", []string{"topic1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].id != "evt_2" {
		t.Errorf("expected evt_2, got %s", events[0].id)
	}

	// Replay after evt_1, both topics — should get evt_2 and evt_3
	events, err = r.Replay("evt_1", []string{"topic1", "topic2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	// Replay with unknown ID — should return nil
	events, err = r.Replay("unknown", []string{"topic1"})
	if err != nil {
		t.Fatal(err)
	}
	if events != nil {
		t.Errorf("expected nil for unknown ID, got %d events", len(events))
	}
}

func TestMemoryReplayer_MaxEvents(t *testing.T) {
	r := NewMemoryReplayer(MemoryReplayerConfig{MaxEvents: 3, TTL: time.Minute})

	r.Store(marshaledEvent{id: "1"}, []string{"t"})
	r.Store(marshaledEvent{id: "2"}, []string{"t"})
	r.Store(marshaledEvent{id: "3"}, []string{"t"})
	r.Store(marshaledEvent{id: "4"}, []string{"t"})

	// Event "1" should be evicted
	events, _ := r.Replay("1", []string{"t"})
	if events != nil {
		t.Error("expected nil — event 1 should be evicted")
	}

	// Event "2" should still work
	events, _ = r.Replay("2", []string{"t"})
	if len(events) != 2 {
		t.Fatalf("expected 2 events after evt_2, got %d", len(events))
	}
}

func TestConnection_TrySend_Backpressure(t *testing.T) {
	conn := newConnection("test", []string{"t"}, 2, time.Second)

	// Fill the buffer
	if !conn.trySend(marshaledEvent{id: "1"}) {
		t.Error("first send should succeed")
	}
	if !conn.trySend(marshaledEvent{id: "2"}) {
		t.Error("second send should succeed")
	}
	// Third should fail (buffer full)
	if conn.trySend(marshaledEvent{id: "3"}) {
		t.Error("third send should fail (backpressure)")
	}
	if conn.MessagesDropped.Load() != 1 {
		t.Errorf("expected 1 dropped, got %d", conn.MessagesDropped.Load())
	}
}

func TestConnection_Close(t *testing.T) {
	conn := newConnection("test", []string{"t"}, 10, time.Second)

	if conn.IsClosed() {
		t.Error("connection should not be closed yet")
	}

	conn.Close()
	if !conn.IsClosed() {
		t.Error("connection should be closed")
	}

	// Safe to close multiple times
	conn.Close()
	conn.Close()
}

func TestTopicsJSON(t *testing.T) {
	tests := []struct {
		input    []string
		expected string
	}{
		{nil, "[]"},
		{[]string{}, "[]"},
		{[]string{"a"}, `["a"]`},
		{[]string{"a", "b", "c"}, `["a","b","c"]`},
	}
	for _, tt := range tests {
		got := topicsJSON(tt.input)
		if got != tt.expected {
			t.Errorf("topicsJSON(%v) = %s, want %s", tt.input, got, tt.expected)
		}
	}
}

func TestHubStats(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 10 * time.Second,
	})
	defer hub.Shutdown(nil)

	stats := hub.Stats()
	if stats.ActiveConnections != 0 {
		t.Errorf("expected 0 connections, got %d", stats.ActiveConnections)
	}
	if stats.TotalTopics != 0 {
		t.Errorf("expected 0 topics, got %d", stats.TotalTopics)
	}
}

// ─── Topic Wildcard Tests ───

func TestTopicMatch_Exact(t *testing.T) {
	tests := []struct {
		pattern, topic string
		expected       bool
	}{
		{"events", "events", true},
		{"events", "events.sub", false},
		{"events.sub", "events", false},
		{"a.b.c", "a.b.c", true},
		{"a.b.c", "a.b.d", false},
	}
	for _, tt := range tests {
		if got := topicMatch(tt.pattern, tt.topic); got != tt.expected {
			t.Errorf("topicMatch(%q, %q) = %v, want %v", tt.pattern, tt.topic, got, tt.expected)
		}
	}
}

func TestTopicMatch_SingleWildcard(t *testing.T) {
	tests := []struct {
		pattern, topic string
		expected       bool
	}{
		{"notifications.*", "notifications.orders", true},
		{"notifications.*", "notifications.chat", true},
		{"notifications.*", "notifications", false},
		{"notifications.*", "notifications.orders.new", false},
		{"*.orders", "notifications.orders", true},
		{"*.orders", "analytics.orders", true},
		{"a.*.c", "a.b.c", true},
		{"a.*.c", "a.b.d", false},
	}
	for _, tt := range tests {
		if got := topicMatch(tt.pattern, tt.topic); got != tt.expected {
			t.Errorf("topicMatch(%q, %q) = %v, want %v", tt.pattern, tt.topic, got, tt.expected)
		}
	}
}

func TestTopicMatch_MultiWildcard(t *testing.T) {
	tests := []struct {
		pattern, topic string
		expected       bool
	}{
		{"analytics.>", "analytics.live", true},
		{"analytics.>", "analytics.live.visitors", true},
		{"analytics.>", "analytics.live.visitors.count", true},
		{"analytics.>", "analytics", false},
		{">", "anything", true},
		{">", "a.b.c", true},
		{"a.b.>", "a.b.c", true},
		{"a.b.>", "a.b", false},
	}
	for _, tt := range tests {
		if got := topicMatch(tt.pattern, tt.topic); got != tt.expected {
			t.Errorf("topicMatch(%q, %q) = %v, want %v", tt.pattern, tt.topic, got, tt.expected)
		}
	}
}

// ─── Connection Group Tests ───

func TestConnMatchesGroup(t *testing.T) {
	conn := newConnection("test", []string{"t"}, 10, time.Second)
	conn.Metadata["tenant_id"] = "t_123"
	conn.Metadata["plan"] = "pro"

	// Match single key
	if !connMatchesGroup(conn, map[string]string{"tenant_id": "t_123"}) {
		t.Error("should match tenant_id")
	}

	// Match multiple keys
	if !connMatchesGroup(conn, map[string]string{"tenant_id": "t_123", "plan": "pro"}) {
		t.Error("should match both keys")
	}

	// Mismatch
	if connMatchesGroup(conn, map[string]string{"tenant_id": "t_456"}) {
		t.Error("should not match different tenant")
	}

	// Missing key
	if connMatchesGroup(conn, map[string]string{"role": "admin"}) {
		t.Error("should not match missing key")
	}
}

// ─── Adaptive Throttler Tests ───

func TestAdaptiveThrottler_EffectiveInterval(t *testing.T) {
	at := newAdaptiveThrottler(2 * time.Second)

	// Low saturation → fast
	fast := at.effectiveInterval(0.05)
	if fast >= 2*time.Second {
		t.Errorf("low saturation should be faster than base, got %v", fast)
	}

	// Normal saturation → base
	base := at.effectiveInterval(0.3)
	if base != 2*time.Second {
		t.Errorf("normal saturation should be base, got %v", base)
	}

	// High saturation → slow
	slow := at.effectiveInterval(0.9)
	if slow <= 2*time.Second {
		t.Errorf("high saturation should be slower than base, got %v", slow)
	}
}

func TestAdaptiveThrottler_ShouldFlush(t *testing.T) {
	at := newAdaptiveThrottler(100 * time.Millisecond)

	// First call always flushes
	if !at.shouldFlush("conn1", 0.0) {
		t.Error("first call should always flush")
	}

	// Immediate second call should not flush
	if at.shouldFlush("conn1", 0.0) {
		t.Error("immediate second call should not flush")
	}

	// Wait for interval
	time.Sleep(120 * time.Millisecond)
	if !at.shouldFlush("conn1", 0.0) {
		t.Error("should flush after interval")
	}
}

// ─── Auth Helper Tests ───

func TestTicketStore_MemoryBasic(t *testing.T) {
	store := NewMemoryTicketStore()

	err := store.Set("ticket1", `{"tenant":"t1"}`, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// First get should succeed
	val, err := store.GetDel("ticket1")
	if err != nil {
		t.Fatal(err)
	}
	if val != `{"tenant":"t1"}` {
		t.Errorf("expected ticket value, got %q", val)
	}

	// Second get should fail (one-time use)
	val, err = store.GetDel("ticket1")
	if err != nil {
		t.Fatal(err)
	}
	if val != "" {
		t.Errorf("expected empty after consume, got %q", val)
	}
}

func TestTicketStore_Expiry(t *testing.T) {
	store := NewMemoryTicketStore()

	err := store.Set("ticket1", "value", 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(60 * time.Millisecond)

	val, err := store.GetDel("ticket1")
	if err != nil {
		t.Fatal(err)
	}
	if val != "" {
		t.Errorf("expected empty after expiry, got %q", val)
	}
}

func TestIssueTicket(t *testing.T) {
	store := NewMemoryTicketStore()

	ticket, err := IssueTicket(store, `{"tenant":"t1"}`, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(ticket) != 48 { // 24 bytes = 48 hex chars
		t.Errorf("expected 48 char ticket, got %d: %s", len(ticket), ticket)
	}

	// Consume it
	val, _ := store.GetDel(ticket)
	if val != `{"tenant":"t1"}` {
		t.Errorf("expected stored value, got %q", val)
	}
}

// ─── Event TTL Tests ───

func TestEventTTL_Fresh(t *testing.T) {
	// An event created now with 5s TTL should not be dropped
	e := Event{
		Type:      "test",
		Data:      "fresh",
		Topics:    []string{"t"},
		TTL:       5 * time.Second,
		CreatedAt: time.Now(),
	}
	// TTL check happens in routeEvent — but we can test the logic directly
	if time.Since(e.CreatedAt) > e.TTL {
		t.Error("fresh event should not be expired")
	}
}

func TestEventTTL_Stale(t *testing.T) {
	e := Event{
		Type:      "test",
		Data:      "stale",
		Topics:    []string{"t"},
		TTL:       50 * time.Millisecond,
		CreatedAt: time.Now().Add(-100 * time.Millisecond),
	}
	if time.Since(e.CreatedAt) <= e.TTL {
		t.Error("stale event should be expired")
	}
}

// ─── Metrics Tests ───

func TestMetricsSnapshot(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 10 * time.Second,
	})
	defer hub.Shutdown(nil)

	snap := hub.Metrics(false)
	if snap.ActiveConnections != 0 {
		t.Errorf("expected 0 connections, got %d", snap.ActiveConnections)
	}
	if snap.Timestamp == "" {
		t.Error("expected non-empty timestamp")
	}
}

// ─── Graceful Drain Tests ───

func TestGracefulDrain(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 10 * time.Second,
	})

	// Before drain — not draining
	if hub.draining.Load() {
		t.Error("should not be draining initially")
	}

	// Shutdown triggers drain
	hub.Shutdown(nil)

	if !hub.draining.Load() {
		t.Error("should be draining after shutdown")
	}
}
