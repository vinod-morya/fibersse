package fibersse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
)

// ──────────────────────────────────────────────────────────────────────────────
// Auth Tests (JWTAuth, TicketAuth)
// ──────────────────────────────────────────────────────────────────────────────

func TestJWTAuth_BearerHeader(t *testing.T) {
	authHandler := JWTAuth(func(token string) (map[string]string, error) {
		if token == "valid-token" {
			return map[string]string{
				"tenant_id": "t_1",
				"user_id":   "u_1",
			}, nil
		}
		return nil, fmt.Errorf("invalid token")
	})

	// Use a plain Fiber handler (not SSE) to test auth logic without streaming
	app := fiber.New()
	app.Get("/events", func(c fiber.Ctx) error {
		conn := &Connection{
			Metadata: make(map[string]string),
			Topics:   []string{"test"},
		}
		if err := authHandler(c, conn); err != nil {
			return c.Status(fiber.StatusForbidden).SendString(err.Error())
		}
		return c.SendString("ok")
	})

	baseURL, cleanup := testServer(t, app)
	defer cleanup()

	client := &http.Client{Timeout: 5 * time.Second}

	// Valid Bearer token
	req, _ := http.NewRequest("GET", baseURL+"/events", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("expected 200 for valid token, got %d", resp.StatusCode)
	}

	// Lowercase bearer
	req2, _ := http.NewRequest("GET", baseURL+"/events", nil)
	req2.Header.Set("Authorization", "bearer valid-token")
	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Errorf("expected 200 for lowercase bearer, got %d", resp2.StatusCode)
	}

	// Invalid token
	req3, _ := http.NewRequest("GET", baseURL+"/events", nil)
	req3.Header.Set("Authorization", "Bearer bad-token")
	resp3, err := client.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != 403 {
		t.Errorf("expected 403 for invalid token, got %d", resp3.StatusCode)
	}

	// No token
	resp4, err := client.Get(baseURL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	resp4.Body.Close()
	if resp4.StatusCode != 403 {
		t.Errorf("expected 403 for missing token, got %d", resp4.StatusCode)
	}
}

func TestJWTAuth_QueryParam(t *testing.T) {
	handler := JWTAuth(func(token string) (map[string]string, error) {
		if token == "qp-token" {
			return map[string]string{"user_id": "u_2"}, nil
		}
		return nil, fmt.Errorf("bad")
	})

	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(c fiber.Ctx, conn *Connection) error {
			if err := handler(c, conn); err != nil {
				return err
			}
			conn.Topics = []string{"test"}
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(context.TODO())

	// Token via query param — use sseClient so the SSE connection is managed
	events, cancel := sseClient(t, baseURL+"/events?token=qp-token")
	defer cancel()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("expected connected event for valid query param token")
	}
}

func TestTicketAuth_Full(t *testing.T) {
	store := NewMemoryTicketStore()

	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: TicketAuth(store, func(value string) (map[string]string, []string, error) {
			if value == "bad-format" {
				return nil, nil, fmt.Errorf("parse error")
			}
			return map[string]string{"tenant_id": "t_1"}, []string{"orders"}, nil
		}),
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(context.TODO())

	// Issue a valid ticket and connect as SSE client
	ticket, err := IssueTicket(store, `{"tenant":"t_1"}`, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	events, cancelSSE := sseClient(t, baseURL+"/events?ticket="+ticket)
	defer cancelSSE()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("valid ticket should establish SSE connection")
	}

	// Reuse same ticket — should fail (one-time use)
	client := &http.Client{Timeout: 5 * time.Second}
	resp2, err := client.Get(baseURL + "/events?ticket=" + ticket)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != 403 {
		t.Errorf("expected 403 for reused ticket, got %d", resp2.StatusCode)
	}

	// Missing ticket
	resp3, err := client.Get(baseURL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != 403 {
		t.Errorf("expected 403 for missing ticket, got %d", resp3.StatusCode)
	}

	// Ticket with parse error
	store.Set("bad-ticket", "bad-format", 30*time.Second)
	resp4, err := client.Get(baseURL + "/events?ticket=bad-ticket")
	if err != nil {
		t.Fatal(err)
	}
	resp4.Body.Close()
	if resp4.StatusCode != 403 {
		t.Errorf("expected 403 for parse error ticket, got %d", resp4.StatusCode)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Domain Event Tests
// ──────────────────────────────────────────────────────────────────────────────

func TestIntegration_DomainEvent(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(c fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"orders"}
			conn.Metadata["tenant_id"] = "t_1"
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(context.TODO())

	events, cancel := sseClient(t, baseURL+"/events")
	defer cancel()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected'")
	}
	time.Sleep(100 * time.Millisecond)

	// DomainEvent with tenant scoping
	hub.DomainEvent("orders", "created", "ord_1", "t_1", map[string]any{
		"total": 99.99,
	})

	evt, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "invalidate"
	})
	if !ok {
		t.Fatal("timed out waiting for domain event")
	}
	if !strings.Contains(evt.Data, `"resource":"orders"`) {
		t.Errorf("missing resource: %s", evt.Data)
	}
	if !strings.Contains(evt.Data, `"action":"created"`) {
		t.Errorf("missing action: %s", evt.Data)
	}
}

func TestIntegration_DomainEvent_NoTenant(t *testing.T) {
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
	defer hub.Shutdown(context.TODO())

	events, cancel := sseClient(t, baseURL+"/events")
	defer cancel()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected'")
	}
	time.Sleep(100 * time.Millisecond)

	// DomainEvent without tenant (global)
	hub.DomainEvent("orders", "updated", "ord_2", "", nil)

	evt, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "invalidate"
	})
	if !ok {
		t.Fatal("timed out waiting for global domain event")
	}
	if !strings.Contains(evt.Data, `"resource_id":"ord_2"`) {
		t.Errorf("missing resource_id: %s", evt.Data)
	}
}

func TestIntegration_Progress(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     200 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"import"}
			conn.Metadata["tenant_id"] = "t_1"
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(context.TODO())

	events, cancel := sseClient(t, baseURL+"/events")
	defer cancel()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected'")
	}
	time.Sleep(100 * time.Millisecond)

	// Send multiple progress events (coalesced)
	for i := 1; i <= 10; i++ {
		hub.Progress("import", "imp_1", "t_1", i*10, 100)
	}

	// With hint
	hub.Progress("import", "imp_2", "t_1", 50, 100, map[string]any{"file": "data.csv"})

	// Without tenant
	hub.Progress("import", "imp_3", "", 25, 100)

	collected := collectEvents(events, 1*time.Second)
	var progressEvents []sseEvent
	for _, e := range collected {
		if e.Type == "progress" {
			progressEvents = append(progressEvents, e)
		}
	}
	if len(progressEvents) == 0 {
		t.Fatal("expected at least 1 progress event")
	}
}

func TestIntegration_Complete(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"import"}
			conn.Metadata["tenant_id"] = "t_1"
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(context.TODO())

	events, cancel := sseClient(t, baseURL+"/events")
	defer cancel()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected'")
	}
	time.Sleep(100 * time.Millisecond)

	// Success completion
	hub.Complete("import", "imp_1", "t_1", true, nil)

	evt, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "complete"
	})
	if !ok {
		t.Fatal("timed out waiting for complete event")
	}
	if !strings.Contains(evt.Data, `"status":"completed"`) {
		t.Errorf("expected completed status: %s", evt.Data)
	}

	// Failure completion with hint
	hub.Complete("import", "imp_2", "t_1", false, map[string]any{
		"error": "CSV parse failed",
	})

	evt2, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "complete" && strings.Contains(e.Data, "failed")
	})
	if !ok {
		t.Fatal("timed out waiting for failed complete event")
	}
	if !strings.Contains(evt2.Data, `"status":"failed"`) {
		t.Errorf("expected failed status: %s", evt2.Data)
	}

	// Complete without tenant
	hub.Complete("import", "imp_3", "", true, nil)
	// Just ensure no panic — it routes globally
	time.Sleep(100 * time.Millisecond)
}

// ──────────────────────────────────────────────────────────────────────────────
// Invalidation Variants Tests
// ──────────────────────────────────────────────────────────────────────────────

func TestIntegration_InvalidateVariants(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"orders", "dashboard"}
			conn.Metadata["tenant_id"] = "t_1"
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(context.TODO())

	events, cancel := sseClient(t, baseURL+"/events")
	defer cancel()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected'")
	}
	time.Sleep(100 * time.Millisecond)

	// InvalidateForTenant
	hub.InvalidateForTenant("t_1", "orders", "ord_1", "created")
	evt, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "invalidate" && strings.Contains(e.Data, "ord_1")
	})
	if !ok {
		t.Fatal("timed out waiting for InvalidateForTenant event")
	}
	if !strings.Contains(evt.Data, `"action":"created"`) {
		t.Errorf("missing action: %s", evt.Data)
	}

	// InvalidateWithHint
	hub.InvalidateWithHint("orders", "ord_2", "updated", map[string]any{
		"total": 149.99,
	})
	evt2, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "invalidate" && strings.Contains(e.Data, "ord_2")
	})
	if !ok {
		t.Fatal("timed out waiting for InvalidateWithHint event")
	}
	if !strings.Contains(evt2.Data, "149.99") {
		t.Errorf("missing hint: %s", evt2.Data)
	}

	// InvalidateForTenantWithHint
	hub.InvalidateForTenantWithHint("t_1", "orders", "ord_3", "deleted", map[string]any{
		"reason": "cancelled",
	})
	evt3, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "invalidate" && strings.Contains(e.Data, "ord_3")
	})
	if !ok {
		t.Fatal("timed out waiting for InvalidateForTenantWithHint event")
	}
	if !strings.Contains(evt3.Data, "cancelled") {
		t.Errorf("missing hint: %s", evt3.Data)
	}

	// Signal
	hub.Signal("dashboard")
	evt4, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "signal"
	})
	if !ok {
		t.Fatal("timed out waiting for Signal event")
	}
	if !strings.Contains(evt4.Data, `"signal":"refresh"`) {
		t.Errorf("missing signal data: %s", evt4.Data)
	}

	// SignalForTenant
	hub.SignalForTenant("t_1", "dashboard")
	evt5, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "signal"
	})
	if !ok {
		t.Fatal("timed out waiting for SignalForTenant event")
	}
	_ = evt5

	// SignalThrottled
	hub.SignalThrottled("dashboard", 5*time.Second)
	evt6, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "signal"
	})
	if !ok {
		t.Fatal("timed out waiting for SignalThrottled event")
	}
	_ = evt6
}

// ──────────────────────────────────────────────────────────────────────────────
// FanOut Tests
// ──────────────────────────────────────────────────────────────────────────────

type mockSubscriber struct {
	messages chan string
}

func (m *mockSubscriber) Subscribe(ctx context.Context, channel string, onMessage func(string)) error {
	for {
		select {
		case msg, ok := <-m.messages:
			if !ok {
				return nil
			}
			onMessage(msg)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func TestFanOut_Basic(t *testing.T) {
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
	defer hub.Shutdown(context.TODO())

	events, cancelSSE := sseClient(t, baseURL+"/events")
	defer cancelSSE()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected'")
	}
	time.Sleep(100 * time.Millisecond)

	// Set up fan-out
	sub := &mockSubscriber{messages: make(chan string, 10)}
	cancelFanOut := hub.FanOut(FanOutConfig{
		Subscriber: sub,
		Channel:    "notifications",
		EventType:  "notification",
		Priority:   PriorityInstant,
	})
	defer cancelFanOut()

	// Send a message through the mock pub/sub
	sub.messages <- `{"title":"Hello from Redis"}`

	evt, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "notification"
	})
	if !ok {
		t.Fatal("timed out waiting for fan-out event")
	}
	if !strings.Contains(evt.Data, "Hello from Redis") {
		t.Errorf("unexpected data: %s", evt.Data)
	}
}

func TestFanOut_WithTransform(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"custom"}
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(context.TODO())

	events, cancelSSE := sseClient(t, baseURL+"/events")
	defer cancelSSE()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected'")
	}
	time.Sleep(100 * time.Millisecond)

	sub := &mockSubscriber{messages: make(chan string, 10)}
	cancelFanOut := hub.FanOut(FanOutConfig{
		Subscriber: sub,
		Channel:    "raw-channel",
		EventType:  "default-type",
		Priority:   PriorityInstant,
		Transform: func(payload string) *Event {
			if payload == "skip" {
				return nil
			}
			return &Event{
				Type:   "custom-event",
				Data:   "transformed:" + payload,
				Topics: []string{"custom"},
			}
		},
	})
	defer cancelFanOut()

	// Send a transformable message
	sub.messages <- "hello"
	evt, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "custom-event"
	})
	if !ok {
		t.Fatal("timed out waiting for transformed event")
	}
	if evt.Data != "transformed:hello" {
		t.Errorf("unexpected data: %s", evt.Data)
	}

	// Send a message that should be skipped
	sub.messages <- "skip"
	time.Sleep(200 * time.Millisecond) // no event expected
}

type errorSubscriber struct {
	callCount int
	mu        sync.Mutex
}

func (e *errorSubscriber) Subscribe(ctx context.Context, _ string, _ func(string)) error {
	e.mu.Lock()
	e.callCount++
	e.mu.Unlock()
	return fmt.Errorf("connection refused")
}

func TestFanOut_RetryOnError(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
	})
	defer hub.Shutdown(context.TODO())

	sub := &errorSubscriber{}
	cancel := hub.FanOut(FanOutConfig{
		Subscriber: sub,
		Channel:    "test",
		EventType:  "test",
	})

	// Wait enough for at least 1 retry (3s between retries)
	time.Sleep(4 * time.Second)
	cancel()

	sub.mu.Lock()
	count := sub.callCount
	sub.mu.Unlock()

	if count < 2 {
		t.Errorf("expected at least 2 subscribe attempts (retry), got %d", count)
	}
}

func TestFanOutMulti(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"ch1", "ch2"}
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(context.TODO())

	events, cancelSSE := sseClient(t, baseURL+"/events")
	defer cancelSSE()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out")
	}
	time.Sleep(100 * time.Millisecond)

	sub1 := &mockSubscriber{messages: make(chan string, 10)}
	sub2 := &mockSubscriber{messages: make(chan string, 10)}

	cancel := hub.FanOutMulti(
		FanOutConfig{
			Subscriber: sub1,
			Channel:    "ch1",
			EventType:  "from-ch1",
			Priority:   PriorityInstant,
		},
		FanOutConfig{
			Subscriber: sub2,
			Channel:    "ch2",
			EventType:  "from-ch2",
			Priority:   PriorityInstant,
		},
	)
	defer cancel()

	// Give fan-out goroutines time to start subscribing
	time.Sleep(100 * time.Millisecond)

	sub1.messages <- "msg1"
	sub2.messages <- "msg2"

	// Collect events and verify both arrive (order may vary)
	collected := collectEvents(events, 5*time.Second)
	gotCh1, gotCh2 := false, false
	for _, e := range collected {
		if e.Type == "from-ch1" {
			gotCh1 = true
		}
		if e.Type == "from-ch2" {
			gotCh2 = true
		}
	}
	if !gotCh1 {
		t.Error("missing from-ch1 event")
	}
	if !gotCh2 {
		t.Error("missing from-ch2 event")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Metrics Tests
// ──────────────────────────────────────────────────────────────────────────────

func TestMetricsHandler(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
	})
	defer hub.Shutdown(context.TODO())

	app := fiber.New()
	app.Get("/metrics", hub.MetricsHandler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()

	// Without connections query param
	resp, err := http.Get(baseURL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("expected application/json, got %s", ct)
	}

	var snap MetricsSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	if snap.Timestamp == "" {
		t.Error("expected non-empty timestamp")
	}

	// With connections=true
	resp2, err := http.Get(baseURL + "/metrics?connections=true")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp2.StatusCode)
	}
}

func TestPrometheusHandler(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
	})
	defer hub.Shutdown(context.TODO())

	// Publish some events to populate metrics
	hub.Publish(Event{Type: "test", Data: "hello", Topics: []string{"t"}, Priority: PriorityInstant})
	time.Sleep(100 * time.Millisecond) // let the run loop process

	app := fiber.New()
	app.Get("/metrics", hub.PrometheusHandler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()

	resp, err := http.Get(baseURL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	output := string(body)

	if !strings.Contains(output, "fibersse_connections_active") {
		t.Errorf("missing connections_active metric: %s", output)
	}
	if !strings.Contains(output, "fibersse_events_published_total") {
		t.Errorf("missing events_published metric: %s", output)
	}
}

func TestFormatFloat(t *testing.T) {
	tests := []struct {
		input    float64
		expected string
	}{
		{0, "0"},
		{42, "42"},
		{3.14, "3.140000"},
		{100, "100"},
	}
	for _, tt := range tests {
		got := formatFloat(tt.input)
		if got != tt.expected {
			t.Errorf("formatFloat(%v) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestAppendProm(t *testing.T) {
	// Without labels
	result := appendProm(nil, "metric_name", "", 42)
	if string(result) != "metric_name 42\n" {
		t.Errorf("unexpected: %q", string(result))
	}

	// With labels
	result = appendProm(nil, "metric_name", `topic="orders"`, 5)
	if string(result) != `metric_name{topic="orders"} 5`+"\n" {
		t.Errorf("unexpected: %q", string(result))
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// SetPaused Tests
// ──────────────────────────────────────────────────────────────────────────────

func TestSetPaused(t *testing.T) {
	pauseCalled := false
	resumeCalled := false

	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnPause: func(conn *Connection) {
			pauseCalled = true
		},
		OnResume: func(conn *Connection) {
			resumeCalled = true
		},
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"test"}
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(context.TODO())

	events, cancel := sseClient(t, baseURL+"/events")
	defer cancel()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected'")
	}
	time.Sleep(100 * time.Millisecond)

	// Get the connection ID from stats
	stats := hub.Stats()
	if stats.ActiveConnections != 1 {
		t.Fatalf("expected 1 connection, got %d", stats.ActiveConnections)
	}

	// Find the connection ID
	hub.mu.RLock()
	var connID string
	for id := range hub.connections {
		connID = id
	}
	hub.mu.RUnlock()

	// Pause
	hub.SetPaused(connID, true)
	time.Sleep(50 * time.Millisecond)
	if !pauseCalled {
		t.Error("OnPause should have been called")
	}

	// Resume
	hub.SetPaused(connID, false)
	time.Sleep(50 * time.Millisecond)
	if !resumeCalled {
		t.Error("OnResume should have been called")
	}

	// Pause non-existent connection — should not panic
	hub.SetPaused("nonexistent", true)
}

// ──────────────────────────────────────────────────────────────────────────────
// Wildcard Topic Integration Tests
// ──────────────────────────────────────────────────────────────────────────────

func TestIntegration_WildcardTopics(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(c fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{c.Query("topic", "test")}
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(context.TODO())

	// Client with wildcard subscription
	events, cancel := sseClient(t, baseURL+"/events?topic=notifications.*")
	defer cancel()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected'")
	}
	time.Sleep(100 * time.Millisecond)

	// Publish to matching topic
	hub.Publish(Event{
		Type:     "msg",
		Data:     "wildcard-match",
		Topics:   []string{"notifications.orders"},
		Priority: PriorityInstant,
	})

	evt, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "msg"
	})
	if !ok {
		t.Fatal("timed out waiting for wildcard-matched event")
	}
	if evt.Data != "wildcard-match" {
		t.Errorf("unexpected data: %s", evt.Data)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Throttle & Coalescer Coverage
// ─────��────────────────────────────────────────────────────────────────────────

func TestAdaptiveThrottler_Remove(t *testing.T) {
	at := newAdaptiveThrottler(100 * time.Millisecond)
	at.shouldFlush("conn1", 0.0)

	at.mu.Lock()
	_, exists := at.lastFlush["conn1"]
	at.mu.Unlock()
	if !exists {
		t.Error("conn1 should be tracked")
	}

	at.remove("conn1")

	at.mu.Lock()
	_, exists = at.lastFlush["conn1"]
	at.mu.Unlock()
	if exists {
		t.Error("conn1 should be removed")
	}
}

func TestAdaptiveThrottler_Cleanup(t *testing.T) {
	at := newAdaptiveThrottler(100 * time.Millisecond)
	at.shouldFlush("old_conn", 0.0)
	at.shouldFlush("new_conn", 0.0)

	// Set old_conn's last flush to a time well in the past
	at.mu.Lock()
	at.lastFlush["old_conn"] = time.Now().Add(-20 * time.Minute)
	at.mu.Unlock()

	at.cleanup(time.Now().Add(-10 * time.Minute))

	at.mu.Lock()
	_, oldExists := at.lastFlush["old_conn"]
	_, newExists := at.lastFlush["new_conn"]
	at.mu.Unlock()

	if oldExists {
		t.Error("old_conn should have been cleaned up")
	}
	if !newExists {
		t.Error("new_conn should still exist")
	}
}

func TestCoalescer_Pending(t *testing.T) {
	c := newCoalescer(time.Second)

	if c.pending() != 0 {
		t.Errorf("expected 0 pending, got %d", c.pending())
	}

	c.addBatched(MarshaledEvent{ID: "1", Data: "a"})
	c.addCoalesced("key1", MarshaledEvent{ID: "2", Data: "b"})

	if c.pending() != 2 {
		t.Errorf("expected 2 pending, got %d", c.pending())
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Event Marshaling Coverage
// ──────────────────────────────────────────────────────────────────────────────

func TestMarshalEvent_NilData(t *testing.T) {
	e := &Event{Type: "test", Data: nil, ID: "evt_nil"}
	me := marshalEvent(e)
	if me.Data != "" {
		t.Errorf("expected empty data for nil, got %q", me.Data)
	}
}

func TestMarshalEvent_BytesData(t *testing.T) {
	e := &Event{Type: "test", Data: []byte(`{"raw":"bytes"}`), ID: "evt_bytes"}
	me := marshalEvent(e)
	if me.Data != `{"raw":"bytes"}` {
		t.Errorf("expected byte data, got %q", me.Data)
	}
}

type customMarshaler struct{}

func (c customMarshaler) MarshalJSON() ([]byte, error) {
	return []byte(`{"custom":true}`), nil
}

func TestMarshalEvent_JSONMarshaler(t *testing.T) {
	e := &Event{Type: "test", Data: customMarshaler{}, ID: "evt_custom"}
	me := marshalEvent(e)
	if me.Data != `{"custom":true}` {
		t.Errorf("expected custom marshaler data, got %q", me.Data)
	}
}

type badMarshaler struct{}

func (b badMarshaler) MarshalJSON() ([]byte, error) {
	return nil, fmt.Errorf("marshal error")
}

func TestMarshalEvent_MarshalerError(t *testing.T) {
	e := &Event{Type: "test", Data: badMarshaler{}, ID: "evt_err"}
	me := marshalEvent(e)
	if !strings.Contains(me.Data, "marshal failed") {
		t.Errorf("expected error data, got %q", me.Data)
	}
}

func TestWriteComment(t *testing.T) {
	var buf bytes.Buffer
	err := writeComment(&buf, "heartbeat")
	if err != nil {
		t.Fatal(err)
	}
	if buf.String() != ": heartbeat\n\n" {
		t.Errorf("unexpected comment output: %q", buf.String())
	}
}

func TestWriteTo_EmptyData(t *testing.T) {
	me := MarshaledEvent{ID: "evt_1", Type: "test", Data: "", Retry: -1}
	var buf bytes.Buffer
	_, err := me.WriteTo(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "data: \n") {
		t.Errorf("expected empty data line: %q", buf.String())
	}
}

func TestWriteTo_WithRetry(t *testing.T) {
	me := MarshaledEvent{ID: "evt_1", Type: "test", Data: "x", Retry: 5000}
	var buf bytes.Buffer
	_, err := me.WriteTo(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "retry: 5000\n") {
		t.Errorf("expected retry field: %q", buf.String())
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// TopicMatchesAny / ConnMatchesTopic
// ──────────────────────────────────────────────────────────────────────────────

func TestTopicMatchesAny(t *testing.T) {
	patterns := []string{"orders", "analytics.*", "chat.>"}

	if !topicMatchesAny(patterns, "orders") {
		t.Error("should match exact")
	}
	if !topicMatchesAny(patterns, "analytics.live") {
		t.Error("should match wildcard *")
	}
	if !topicMatchesAny(patterns, "chat.room1.msg") {
		t.Error("should match wildcard >")
	}
	if topicMatchesAny(patterns, "unknown") {
		t.Error("should not match unknown")
	}
}

func TestConnMatchesTopic(t *testing.T) {
	conn := newConnection("test", []string{"orders", "analytics.*"}, 10, time.Second)

	if !connMatchesTopic(conn, "orders") {
		t.Error("should match exact topic")
	}
	if !connMatchesTopic(conn, "analytics.live") {
		t.Error("should match wildcard topic")
	}
	if connMatchesTopic(conn, "unknown") {
		t.Error("should not match unknown topic")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Metrics with connections + events
// ──────────────────────────────────────────────────────────────────────────────

func TestMetrics_WithConnections(t *testing.T) {
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
	defer hub.Shutdown(context.TODO())

	events, cancel := sseClient(t, baseURL+"/events")
	defer cancel()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for 'connected'")
	}
	time.Sleep(100 * time.Millisecond)

	snap := hub.Metrics(true)
	if snap.ActiveConnections != 1 {
		t.Errorf("expected 1 connection, got %d", snap.ActiveConnections)
	}
	if len(snap.Connections) != 1 {
		t.Errorf("expected 1 connection detail, got %d", len(snap.Connections))
	}
	if snap.Connections[0].Topics[0] != "test" {
		t.Errorf("unexpected topic: %v", snap.Connections[0].Topics)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Hub defaults coverage
// ──────────────────────────────────────────────────────────────────────────────

func TestHubConfig_Defaults(t *testing.T) {
	cfg := HubConfig{}
	cfg.defaults()

	if cfg.FlushInterval != 2*time.Second {
		t.Errorf("FlushInterval default: %v", cfg.FlushInterval)
	}
	if cfg.SendBufferSize != 256 {
		t.Errorf("SendBufferSize default: %d", cfg.SendBufferSize)
	}
	if cfg.HeartbeatInterval != 30*time.Second {
		t.Errorf("HeartbeatInterval default: %v", cfg.HeartbeatInterval)
	}
	if cfg.MaxLifetime != 30*time.Minute {
		t.Errorf("MaxLifetime default: %v", cfg.MaxLifetime)
	}
	if cfg.RetryMS != 3000 {
		t.Errorf("RetryMS default: %d", cfg.RetryMS)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Heartbeat test
// ──────────────────────────────────────────────────────────────────────────────

func TestIntegration_Heartbeat(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 500 * time.Millisecond, // fast heartbeats for test
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"test"}
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(context.TODO())

	// Connect and wait for connected event
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	req, _ := http.NewRequestWithContext(ctx, "GET", baseURL+"/events", nil)
	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Read raw SSE — heartbeats appear as ": heartbeat" comment lines
	scanner := make([]byte, 4096)
	deadline := time.After(3 * time.Second)
	foundHeartbeat := false

	for !foundHeartbeat {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for heartbeat")
			return
		default:
		}
		n, err := resp.Body.Read(scanner)
		if err != nil {
			break
		}
		if strings.Contains(string(scanner[:n]), ": heartbeat") {
			foundHeartbeat = true
		}
	}

	if !foundHeartbeat {
		t.Error("expected heartbeat comment")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Replay integration
// ──────────────────────────────────────────────────────────────────────────────

func TestIntegration_LastEventID_Replay(t *testing.T) {
	replayer := NewMemoryReplayer(MemoryReplayerConfig{MaxEvents: 100, TTL: time.Minute})

	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		Replayer:          replayer,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"orders"}
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(context.TODO())

	// First client — connect and receive some events
	events1, cancel1 := sseClient(t, baseURL+"/events")
	_, ok := waitForEvent(events1, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for first connected")
	}
	time.Sleep(100 * time.Millisecond)

	// Publish events
	hub.Publish(Event{
		Type:     "order",
		Data:     "order-1",
		Topics:   []string{"orders"},
		Priority: PriorityInstant,
	})

	evt1, ok := waitForEvent(events1, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "order"
	})
	if !ok {
		t.Fatal("first client didn't receive event")
	}
	lastID := evt1.ID

	// Publish another event
	hub.Publish(Event{
		Type:     "order",
		Data:     "order-2",
		Topics:   []string{"orders"},
		Priority: PriorityInstant,
	})
	time.Sleep(200 * time.Millisecond)
	cancel1()

	// Second client reconnects with Last-Event-ID
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	req, _ := http.NewRequestWithContext(ctx, "GET", baseURL+"/events", nil)
	req.Header.Set("Last-Event-ID", lastID)

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	eventsCh := readSSEEvents(ctx, resp)

	// Should receive the missed event (order-2) via replay before the connected event
	var replayedEvents []sseEvent
	collected := collectEvents(eventsCh, 2*time.Second)
	for _, e := range collected {
		if e.Type == "order" {
			replayedEvents = append(replayedEvents, e)
		}
	}

	if len(replayedEvents) == 0 {
		t.Error("expected at least 1 replayed event")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// buildFanOutEvent unit test
// ──────────────────────────────────────────────────────────────────────────────

func TestBuildFanOutEvent(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
	})
	defer hub.Shutdown(context.TODO())

	// Basic — no transform
	cfg := FanOutConfig{
		EventType:   "notification",
		Priority:    PriorityBatched,
		CoalesceKey: "ck",
		TTL:         5 * time.Second,
	}
	evt := hub.buildFanOutEvent(cfg, "topic1", "payload")
	if evt == nil {
		t.Fatal("expected event")
	}
	if evt.Type != "notification" {
		t.Errorf("expected type notification, got %s", evt.Type)
	}
	if evt.Topics[0] != "topic1" {
		t.Errorf("expected topic1, got %v", evt.Topics)
	}
	if evt.Priority != PriorityBatched {
		t.Errorf("expected PriorityBatched")
	}
	if evt.CoalesceKey != "ck" {
		t.Errorf("expected coalesce key 'ck', got %s", evt.CoalesceKey)
	}
	if evt.TTL != 5*time.Second {
		t.Errorf("expected 5s TTL, got %v", evt.TTL)
	}

	// Transform returns nil — should skip
	cfg2 := FanOutConfig{
		EventType: "test",
		Transform: func(payload string) *Event { return nil },
	}
	evt2 := hub.buildFanOutEvent(cfg2, "topic", "payload")
	if evt2 != nil {
		t.Error("expected nil for skipped transform")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Batched priority delivery
// ──────────────────────────────────────────────────────────────────────────────

func TestIntegration_BatchedPriority(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     300 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"updates"}
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())

	baseURL, cleanup := testServer(t, app)
	defer cleanup()
	defer hub.Shutdown(context.TODO())

	events, cancel := sseClient(t, baseURL+"/events")
	defer cancel()

	_, ok := waitForEvent(events, 3*time.Second, func(e sseEvent) bool {
		return e.Type == "connected"
	})
	if !ok {
		t.Fatal("timed out waiting for connected")
	}
	time.Sleep(100 * time.Millisecond)

	// Publish batched events
	hub.Publish(Event{
		Type:     "update",
		Data:     "batch-1",
		Topics:   []string{"updates"},
		Priority: PriorityBatched,
	})
	hub.Publish(Event{
		Type:     "update",
		Data:     "batch-2",
		Topics:   []string{"updates"},
		Priority: PriorityBatched,
	})

	// Collect events — batched should arrive after flush interval
	collected := collectEvents(events, 2*time.Second)
	var updateEvents []sseEvent
	for _, e := range collected {
		if e.Type == "update" {
			updateEvents = append(updateEvents, e)
		}
	}

	if len(updateEvents) != 2 {
		t.Errorf("expected 2 batched events, got %d", len(updateEvents))
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Shutdown with context timeout
// ──────────────────────────────────────────────────────────────────────────────

func TestShutdown_WithContextTimeout(t *testing.T) {
	hub := New(HubConfig{
		FlushInterval:     100 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := hub.Shutdown(ctx)
	if err != nil {
		t.Errorf("shutdown should succeed, got: %v", err)
	}
}
