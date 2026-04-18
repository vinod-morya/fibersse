package fibersse

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
)

func TestTenantFromChannelSegment(t *testing.T) {
	t.Parallel()

	ext := TenantFromChannelSegment(1)

	cases := []struct {
		channel string
		wantID  string
		wantOK  bool
	}{
		{"orders:t_123", "t_123", true},
		{"app:region:t_xyz", "region", true}, // segment 1 of 3
		{"nocodon", "", false},               // only 1 segment, index 1 out of range
		{"orders:", "", false},               // empty segment
		{"", "", false},                      // empty channel
	}

	for _, tc := range cases {
		tid, ok := ext(tc.channel, "")
		if ok != tc.wantOK || tid != tc.wantID {
			t.Errorf("channel=%q: got (%q, %v), want (%q, %v)", tc.channel, tid, ok, tc.wantID, tc.wantOK)
		}
	}
}

func TestTenantFromPayloadJSON(t *testing.T) {
	t.Parallel()

	ext := TenantFromPayloadJSON("tenant_id")

	cases := []struct {
		payload string
		wantID  string
		wantOK  bool
	}{
		{`{"tenant_id":"t_123","event":"order"}`, "t_123", true},
		{`{"event":"order","tenant_id":"t_abc"}`, "t_abc", true},
		{`{"event":"order"}`, "", false},          // field missing
		{`{"tenant_id":""}`, "", false},            // empty value
		{`not json at all`, "", false},             // garbage
		{`{"tenant_id":"t_1"`, "t_1", true},       // truncated JSON but value is complete
	}

	for _, tc := range cases {
		tid, ok := ext("", tc.payload)
		if ok != tc.wantOK || tid != tc.wantID {
			t.Errorf("payload=%q: got (%q, %v), want (%q, %v)", tc.payload, tid, ok, tc.wantID, tc.wantOK)
		}
	}
}

func TestTenantFanOut_BuildEventScoping(t *testing.T) {
	t.Parallel()

	// Verify buildTenantEvent correctly scopes Group by extracted tenant.
	cfg := TenantFanOutConfig{
		EventType:       "order",
		Topic:           "orders",
		Priority:        PriorityInstant,
		TenantExtractor: TenantFromChannelSegment(1),
		FailClosed:      true,
	}

	// Normal case: tenant extracted from channel
	evt := buildTenantEvent(cfg, "orders", "orders:t_123", `{"data":"x"}`)
	if evt == nil {
		t.Fatal("expected event, got nil")
	}
	if evt.Group["tenant_id"] != "t_123" {
		t.Errorf("expected tenant_id=t_123, got %q", evt.Group["tenant_id"])
	}

	// FailClosed: extractor returns false → nil
	badCfg := TenantFanOutConfig{
		EventType:       "order",
		TenantExtractor: func(_, _ string) (string, bool) { return "", false },
		FailClosed:      true,
	}
	if buildTenantEvent(badCfg, "orders", "no-colon", "payload") != nil {
		t.Error("FailClosed=true should return nil when extractor fails")
	}

	// FailClosed=false with failed extraction → still delivers (no Group set)
	openCfg := TenantFanOutConfig{
		EventType:       "order",
		Topic:           "orders",
		TenantExtractor: func(_, _ string) (string, bool) { return "", false },
		FailClosed:      false,
	}
	evt2 := buildTenantEvent(openCfg, "orders", "no-colon", "payload")
	if evt2 == nil {
		t.Fatal("FailClosed=false should still return event")
	}
	if len(evt2.Group) != 0 {
		t.Errorf("no tenant extracted → Group should be empty, got %v", evt2.Group)
	}
}

func TestTenantFanOut_FailClosed_DropsOnMissingTenant(t *testing.T) {
	t.Parallel()

	// Test buildTenantEvent directly: FailClosed=true + extractor fails → nil,
	// even when no Transform is set (so no side-effects before the check).
	cfg := TenantFanOutConfig{
		EventType:       "order",
		Topic:           "orders",
		TenantExtractor: func(_, _ string) (string, bool) { return "", false },
		FailClosed:      true,
	}
	if buildTenantEvent(cfg, "orders", "no-colon", "payload") != nil {
		t.Error("FailClosed=true should return nil when extractor fails")
	}

	// FailClosed=false → event still published, just without Group
	openCfg := TenantFanOutConfig{
		EventType:       "order",
		Topic:           "orders",
		TenantExtractor: func(_, _ string) (string, bool) { return "", false },
		FailClosed:      false,
	}
	evt := buildTenantEvent(openCfg, "orders", "no-colon", "payload")
	if evt == nil {
		t.Fatal("FailClosed=false should still produce an event")
	}
	if len(evt.Group) != 0 {
		t.Errorf("no tenant → Group should be empty, got %v", evt.Group)
	}
}

func TestDrainTenant(t *testing.T) {
	t.Parallel()

	hub := New(HubConfig{
		FlushInterval:     50 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"test"}
			conn.Metadata["tenant_id"] = "t_abc"
			return nil
		},
	})
	defer hub.Shutdown(context.TODO())

	app := fiber.New()
	app.Get("/events", hub.Handler())
	baseURL, cleanup := testServer(t, app)
	defer cleanup()

	client := &http.Client{Timeout: 5 * time.Second}
	for range 3 {
		go client.Get(baseURL + "/events") //nolint:errcheck
	}

	deadline := time.Now().Add(2 * time.Second)
	for hub.Stats().ActiveConnections < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if hub.Stats().ActiveConnections < 3 {
		t.Fatal("3 connections did not register in time")
	}

	closed := hub.DrainTenant("t_abc")
	if closed != 3 {
		t.Errorf("DrainTenant: expected 3 closed, got %d", closed)
	}

	// Wrong tenant should close nothing
	closed2 := hub.DrainTenant("t_other")
	if closed2 != 0 {
		t.Errorf("DrainTenant wrong tenant: expected 0, got %d", closed2)
	}
}

func TestActiveTenantIDs(t *testing.T) {
	t.Parallel()

	hub := New(HubConfig{
		FlushInterval:     50 * time.Millisecond,
		HeartbeatInterval: 30 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *Connection) error {
			conn.Topics = []string{"intelligence"}
			conn.Metadata["tenant_id"] = "t_shared"
			return nil
		},
	})

	app := fiber.New()
	app.Get("/events", hub.Handler())
	baseURL, cleanup := testServer(t, app)
	defer cleanup()

	// No connections yet → empty
	if ids := hub.ActiveTenantIDs("intelligence"); len(ids) != 0 {
		t.Errorf("expected 0 active tenants, got %v", ids)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client := &http.Client{Timeout: 5 * time.Second}
	for range 3 {
		req, _ := http.NewRequestWithContext(ctx, "GET", baseURL+"/events", nil)
		go client.Do(req) //nolint:errcheck
	}

	deadline := time.Now().Add(2 * time.Second)
	for hub.Stats().ActiveConnections < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	ids := hub.ActiveTenantIDs("intelligence")
	if len(ids) != 1 || ids[0] != "t_shared" {
		t.Errorf("expected [t_shared], got %v", ids)
	}

	// Different topic → 0
	if ids2 := hub.ActiveTenantIDs("other-topic"); len(ids2) != 0 {
		t.Errorf("expected 0 for unrelated topic, got %v", ids2)
	}

	// Cancel context to close HTTP connections, then shutdown hub
	cancel()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutCancel()
	hub.Shutdown(shutCtx) //nolint:errcheck
}
