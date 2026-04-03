package fibersse

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
)

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

// sseEvent is a parsed SSE event read from the wire.
type sseEvent struct {
	ID    string
	Type  string
	Data  string
	Retry string
}

// readSSEEvents reads SSE events from an http.Response body until ctx expires
// or the body is closed. It sends parsed events to the returned channel.
func readSSEEvents(ctx context.Context, resp *http.Response) <-chan sseEvent {
	ch := make(chan sseEvent, 64)
	go func() {
		defer close(ch)
		scanner := bufio.NewScanner(resp.Body)
		var current sseEvent
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			line := scanner.Text()
			switch {
			case strings.HasPrefix(line, "id: "):
				current.ID = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "event: "):
				current.Type = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				if current.Data != "" {
					current.Data += "\n"
				}
				current.Data += strings.TrimPrefix(line, "data: ")
			case strings.HasPrefix(line, "retry: "):
				current.Retry = strings.TrimPrefix(line, "retry: ")
			case line == "":
				// Blank line = event boundary. Emit if we have content.
				if current.ID != "" || current.Type != "" || current.Data != "" || current.Retry != "" {
					ch <- current
					current = sseEvent{}
				}
			}
		}
	}()
	return ch
}

// waitForEvent reads from the channel until an event matching the predicate
// arrives, or the timeout fires. Returns the event and true, or zero value
// and false on timeout.
func waitForEvent(ch <-chan sseEvent, timeout time.Duration, match func(sseEvent) bool) (sseEvent, bool) {
	deadline := time.After(timeout)
	for {
		select {
		case evt, ok := <-ch:
			if !ok {
				return sseEvent{}, false
			}
			if match(evt) {
				return evt, true
			}
		case <-deadline:
			return sseEvent{}, false
		}
	}
}

// collectEvents drains the channel for `duration`, returning all received events.
func collectEvents(ch <-chan sseEvent, duration time.Duration) []sseEvent {
	var events []sseEvent
	deadline := time.After(duration)
	for {
		select {
		case evt, ok := <-ch:
			if !ok {
				return events
			}
			events = append(events, evt)
		case <-deadline:
			return events
		}
	}
}

// testServer starts a Fiber app on a random TCP port and returns the base URL
// and a cleanup function. The caller must defer cleanup().
func testServer(t *testing.T, app *fiber.App) (baseURL string, cleanup func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = app.Listener(ln)
	}()

	addr := ln.Addr().String()
	baseURL = "http://" + addr

	cleanup = func() {
		_ = app.Shutdown()
		<-done
	}
	return baseURL, cleanup
}

// sseClient is a helper that connects to an SSE endpoint and returns
// the parsed event channel, the raw response, and a cancel function.
func sseClient(t *testing.T, url string) (events <-chan sseEvent, cancel func()) {
	t.Helper()
	ctx, cancelFn := context.WithCancel(context.Background())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cancelFn()
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	client := &http.Client{Timeout: 0} // no timeout — SSE streams indefinitely
	resp, err := client.Do(req)
	if err != nil {
		cancelFn()
		t.Fatalf("SSE connection failed: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		cancelFn()
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	events = readSSEEvents(ctx, resp)
	cancel = func() {
		cancelFn()
		resp.Body.Close()
	}
	return events, cancel
}

// ──────────────────────────────────────────────────────────────────────────────
// Integration Tests
// ──────────────────────────────────────────────────────────────────────────────

func TestIntegration_Connect(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"test"}
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(nil)

	events, cancel := sseClient(t, baseURL+"/events")
	defer cancel()

	// The first real event should be "connected"
	evt, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected' event")
	}

	if !strings.Contains(evt.Data, `"connection_id"`) {
		t.Errorf("connected event missing connection_id: %s", evt.Data)
	}
	if !strings.Contains(evt.Data, `"topics":["test"]`) {
		t.Errorf("connected event missing topics: %s", evt.Data)
	}
}

func TestIntegration_PublishReceive(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"notifications"}
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(nil)

	events, cancel := sseClient(t, baseURL+"/events")
	defer cancel()

	// Wait for connected event first
	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected' event")
	}

	// Publish an event
	hub.Publish(Event{
		Type:     "notification",
		Data:     `{"title":"Hello"}`,
		Topics:   []string{"notifications"},
		Priority: PriorityInstant,
	})

	// Wait for the published event
	evt, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "notification"
	})
	if !ok {
		t.Fatal("timed out waiting for 'notification' event")
	}

	if !strings.Contains(evt.Data, `"title":"Hello"`) {
		t.Errorf("unexpected data: %s", evt.Data)
	}
}

func TestIntegration_TopicFiltering(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(c fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{c.Query("topic", "default")}
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(nil)

	// Client subscribed to topic "a"
	eventsA, cancelA := sseClient(t, baseURL+"/events?topic=a")
	defer cancelA()

	// Wait for connected
	_, ok := waitForEvent(eventsA, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected' event on client A")
	}

	// Small delay to ensure hub has registered the connection
	time.Sleep(50 * time.Millisecond)

	// Publish to topic "b" — client A should NOT receive this
	hub.Publish(Event{
		Type:     "msg",
		Data:     "for-b",
		Topics:   []string{"b"},
		Priority: PriorityInstant,
	})

	// Publish to topic "a" — client A SHOULD receive this
	hub.Publish(Event{
		Type:     "msg",
		Data:     "for-a",
		Topics:   []string{"a"},
		Priority: PriorityInstant,
	})

	// Collect events for a reasonable window
	collected := collectEvents(eventsA, 1*time.Second)

	// Filter out heartbeats and connected events — look for "msg" events
	var msgEvents []sseEvent
	for _, e := range collected {
		if e.Type == "msg" {
			msgEvents = append(msgEvents, e)
		}
	}

	if len(msgEvents) != 1 {
		t.Fatalf("expected 1 msg event, got %d: %+v", len(msgEvents), msgEvents)
	}
	if msgEvents[0].Data != "for-a" {
		t.Errorf("expected data 'for-a', got '%s'", msgEvents[0].Data)
	}
}

func TestIntegration_Coalescing(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     500 * time.Millisecond, // flush every 500ms
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"progress"}
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(nil)

	events, cancel := sseClient(t, baseURL+"/events")
	defer cancel()

	// Wait for connected
	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected' event")
	}

	// Small delay so the hub registers the connection before we publish
	time.Sleep(100 * time.Millisecond)

	// Publish 5 coalesced events rapidly — only the last should survive
	for i := 1; i <= 5; i++ {
		hub.Publish(Event{
			Type:        "progress",
			Data:        fmt.Sprintf(`{"pct":%d}`, i*20),
			Topics:      []string{"progress"},
			Priority:    PriorityCoalesced,
			CoalesceKey: "progress:import:1",
		})
	}

	// Wait for the flush cycle to deliver the coalesced event
	// We collect for slightly longer than FlushInterval
	collected := collectEvents(events, 2*time.Second)

	var progressEvents []sseEvent
	for _, e := range collected {
		if e.Type == "progress" {
			progressEvents = append(progressEvents, e)
		}
	}

	// Coalescing should produce exactly 1 event (the last one)
	if len(progressEvents) != 1 {
		t.Fatalf("expected 1 coalesced progress event, got %d: %+v", len(progressEvents), progressEvents)
	}
	if !strings.Contains(progressEvents[0].Data, `"pct":100`) {
		t.Errorf("expected last coalesced value (pct:100), got: %s", progressEvents[0].Data)
	}
}

func TestIntegration_GroupFiltering(t *testing.T) {
	// Each tenant subscribes to a unique topic so that topic-based routing
	// does NOT match both tenants. Group matching is then used to deliver
	// events that have no overlapping topics but match by metadata.
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(c fiber.Ctx, conn *Connection) error {
			tenant := c.Query("tenant")
			conn.Topics = []string{"tenant:" + tenant}
			conn.Metadata["tenant_id"] = tenant
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(nil)

	// Tenant A client
	eventsA, cancelA := sseClient(t, baseURL+"/events?tenant=t_A")
	defer cancelA()
	_, ok := waitForEvent(eventsA, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected' on tenant A")
	}

	// Tenant B client
	eventsB, cancelB := sseClient(t, baseURL+"/events?tenant=t_B")
	defer cancelB()
	_, ok = waitForEvent(eventsB, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected' on tenant B")
	}

	time.Sleep(100 * time.Millisecond)

	// Publish event scoped to tenant A only via Group. The topic "orders"
	// is NOT in either client's subscription, so only the Group match
	// routes the event to tenant A's connection.
	hub.Publish(Event{
		Type:     "order-update",
		Data:     `{"order":"ord_1"}`,
		Topics:   []string{"orders"}, // neither client subscribes to "orders"
		Group:    map[string]string{"tenant_id": "t_A"},
		Priority: PriorityInstant,
	})

	// Tenant A should receive it (matched by Group)
	evtA, ok := waitForEvent(eventsA, 2*time.Second, func(e sseEvent) bool {
		return e.Type == "order-update"
	})
	if !ok {
		t.Fatal("tenant A should have received the order-update event")
	}
	if !strings.Contains(evtA.Data, `ord_1`) {
		t.Errorf("unexpected data for tenant A: %s", evtA.Data)
	}

	// Tenant B should NOT receive it — collect briefly and verify
	collectedB := collectEvents(eventsB, 500*time.Millisecond)
	for _, e := range collectedB {
		if e.Type == "order-update" {
			t.Errorf("tenant B should NOT have received order-update, got: %+v", e)
		}
	}
}

func TestIntegration_Invalidate(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"orders"}
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(nil)

	events, cancel := sseClient(t, baseURL+"/events")
	defer cancel()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected' event")
	}

	time.Sleep(100 * time.Millisecond)

	// Call Invalidate
	hub.Invalidate("orders", "ord_123", "created")

	// Should receive an "invalidate" event
	evt, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "invalidate"
	})
	if !ok {
		t.Fatal("timed out waiting for 'invalidate' event")
	}

	if !strings.Contains(evt.Data, `"resource":"orders"`) {
		t.Errorf("invalidate event missing resource: %s", evt.Data)
	}
	if !strings.Contains(evt.Data, `"action":"created"`) {
		t.Errorf("invalidate event missing action: %s", evt.Data)
	}
	if !strings.Contains(evt.Data, `"resource_id":"ord_123"`) {
		t.Errorf("invalidate event missing resource_id: %s", evt.Data)
	}
}

func TestIntegration_BatchDomainEvents(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"orders", "inventory"}
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(nil)

	events, cancel := sseClient(t, baseURL+"/events")
	defer cancel()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected' event")
	}

	time.Sleep(100 * time.Millisecond)

	// Publish a batch
	hub.BatchDomainEvents("", []DomainEventSpec{
		{Resource: "orders", Action: "created", ResourceID: "ord_1"},
		{Resource: "inventory", Action: "updated", ResourceID: "sku_1"},
	})

	// Should receive a "batch" event
	evt, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "batch"
	})
	if !ok {
		t.Fatal("timed out waiting for 'batch' event")
	}

	if !strings.Contains(evt.Data, `"resource":"orders"`) {
		t.Errorf("batch event missing orders resource: %s", evt.Data)
	}
	if !strings.Contains(evt.Data, `"resource":"inventory"`) {
		t.Errorf("batch event missing inventory resource: %s", evt.Data)
	}
}

func TestIntegration_GracefulDrain(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"test"}
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()

	// Shutdown the hub in a goroutine — this sets draining=true and closes
	// the run loop. We do NOT have an active SSE connection, so the shutdown
	// completes quickly.
	ctx, cancelCtx := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCtx()
	if err := hub.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	// The Fiber app is still listening, but the handler now checks
	// hub.draining and returns 503.
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(baseURL + "/events")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 during drain, got %d", resp.StatusCode)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Additional Integration Tests
// ──────────────────────────────────────────────────────────────────────────────

func TestIntegration_OnConnectReject(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			return fmt.Errorf("access denied")
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(nil)

	resp, err := http.Get(baseURL + "/events")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 for rejected connection, got %d", resp.StatusCode)
	}
}

func TestIntegration_NoTopics_Returns400(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			// Deliberately do not set any topics
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(nil)

	resp, err := http.Get(baseURL + "/events")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for no topics, got %d", resp.StatusCode)
	}
}

func TestIntegration_MultipleClients(t *testing.T) {
	var connMu sync.Mutex
	connCount := 0

	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"broadcast"}
			connMu.Lock()
			connCount++
			connMu.Unlock()
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(nil)

	const numClients = 5
	channels := make([]<-chan sseEvent, numClients)
	cancels := make([]func(), numClients)

	for i := 0; i < numClients; i++ {
		ch, cancelFn := sseClient(t, baseURL+"/events")
		channels[i] = ch
		cancels[i] = cancelFn
		defer cancels[i]()

		_, ok := waitForEvent(channels[i], 3*time.Second, func(e sseEvent) bool {
			return e.Type == "connected"
		})
		if !ok {
			t.Fatalf("client %d: timed out waiting for 'connected'", i)
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Publish one event — all clients should receive it
	hub.Publish(Event{
		Type:     "broadcast",
		Data:     "hello-all",
		Topics:   []string{"broadcast"},
		Priority: PriorityInstant,
	})

	for i := 0; i < numClients; i++ {
		evt, ok := waitForEvent(channels[i], 3*time.Second, func(e sseEvent) bool {
			return e.Type == "broadcast"
		})
		if !ok {
			t.Errorf("client %d: timed out waiting for broadcast event", i)
			continue
		}
		if evt.Data != "hello-all" {
			t.Errorf("client %d: expected 'hello-all', got '%s'", i, evt.Data)
		}
	}

	connMu.Lock()
	if connCount != numClients {
		t.Errorf("expected %d connections, OnConnect called %d times", numClients, connCount)
	}
	connMu.Unlock()
}

func TestIntegration_StatsAfterConnect(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"stats-test"}
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(nil)

	events, cancel := sseClient(t, baseURL+"/events")
	defer cancel()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected' event")
	}

	// Allow the hub run loop to process the registration
	time.Sleep(100 * time.Millisecond)

	stats := hub.Stats()
	if stats.ActiveConnections != 1 {
		t.Errorf("expected 1 active connection, got %d", stats.ActiveConnections)
	}
	if stats.ConnectionsByTopic["stats-test"] != 1 {
		t.Errorf("expected 1 connection on topic 'stats-test', got %d", stats.ConnectionsByTopic["stats-test"])
	}
}
