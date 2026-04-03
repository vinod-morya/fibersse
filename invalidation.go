package fibersse

import "time"

// ──────────────────────────────────────────────────────────────────────────────
// Cache Invalidation Signals
//
// The primary pattern for replacing polling: instead of clients polling
// every 30 seconds, the server publishes a lightweight "invalidate" signal
// when data changes. The client then refetches only the specific API endpoint.
//
// Before (polling):
//
//	setInterval(() => fetch("/api/orders"), 30_000)
//
// After (SSE invalidation):
//
//	hub.Invalidate("orders", "order_123", "created")
//	// Client receives → refetches /api/orders only when data actually changed
//
// This reduces API calls by 80-90% while making the UI feel real-time.
// ──────────────────────────────────────────────────────────────────────────────

// InvalidationEvent is a lightweight signal telling the client to refetch
// a specific resource. It carries no data payload — just the signal.
type InvalidationEvent struct {
	// Resource is what changed (e.g., "orders", "products", "customers").
	Resource string `json:"resource"`

	// Action is what happened (e.g., "created", "updated", "deleted", "refresh").
	Action string `json:"action"`

	// ResourceID is the specific item that changed (optional).
	// Empty means "the whole collection changed".
	ResourceID string `json:"resource_id,omitempty"`

	// Hint is optional extra data for the client (e.g., order total, status).
	// Keep it small — the client will refetch the full data anyway.
	Hint map[string]any `json:"hint,omitempty"`
}

// Invalidate publishes a cache invalidation signal to all connections
// subscribed to the given topic. Clients should refetch the corresponding
// API endpoint when they receive this event.
//
// This is the primary API for replacing polling patterns:
//
//	// In your order handler, after creating an order:
//	hub.Invalidate("orders", "ord_123", "created")
//
//	// In your product handler, after bulk update:
//	hub.Invalidate("products", "", "refresh")
//
//	// With tenant scoping (multi-tenant SaaS):
//	hub.InvalidateForTenant("t_123", "orders", "ord_456", "created")
//
// The event type sent to clients is "invalidate" with a JSON payload
// containing the resource, action, and optional resource_id.
func (h *Hub) Invalidate(topic, resourceID, action string) {
	h.Publish(Event{
		Type:   "invalidate",
		Topics: []string{topic},
		Data: InvalidationEvent{
			Resource:   topic,
			Action:     action,
			ResourceID: resourceID,
		},
		Priority:    PriorityInstant,
		CoalesceKey: "invalidate:" + topic + ":" + resourceID,
	})
}

// InvalidateForTenant publishes a tenant-scoped cache invalidation signal.
// Only connections with the matching tenant_id in their metadata receive it.
//
//	// After creating an order for tenant t_123:
//	hub.InvalidateForTenant("t_123", "orders", "ord_456", "created")
func (h *Hub) InvalidateForTenant(tenantID, topic, resourceID, action string) {
	h.Publish(Event{
		Type:   "invalidate",
		Topics: []string{topic},
		Group:  map[string]string{"tenant_id": tenantID},
		Data: InvalidationEvent{
			Resource:   topic,
			Action:     action,
			ResourceID: resourceID,
		},
		Priority:    PriorityInstant,
		CoalesceKey: "invalidate:" + topic + ":" + resourceID,
	})
}

// InvalidateWithHint publishes an invalidation signal with extra data hints.
// Use when the client needs a small piece of info without refetching.
//
//	hub.InvalidateWithHint("orders", "ord_456", "created", map[string]any{
//	    "total": 149.99,
//	    "status": "pending",
//	})
func (h *Hub) InvalidateWithHint(topic, resourceID, action string, hint map[string]any) {
	h.Publish(Event{
		Type:   "invalidate",
		Topics: []string{topic},
		Data: InvalidationEvent{
			Resource:   topic,
			Action:     action,
			ResourceID: resourceID,
			Hint:       hint,
		},
		Priority:    PriorityInstant,
		CoalesceKey: "invalidate:" + topic + ":" + resourceID,
	})
}

// Signal publishes a simple refresh signal with no resource details.
// Use for dashboard-level "something changed, refetch everything" signals.
//
//	// After any mutation that affects the dashboard:
//	hub.Signal("dashboard")
//
//	// Throttled signal (60s TTL — if client is slow, stale signals are dropped):
//	hub.SignalThrottled("analytics", 60*time.Second)
func (h *Hub) Signal(topic string) {
	h.Publish(Event{
		Type:        "signal",
		Topics:      []string{topic},
		Data:        map[string]string{"signal": "refresh"},
		Priority:    PriorityCoalesced,
		CoalesceKey: "signal:" + topic,
	})
}

// SignalForTenant publishes a tenant-scoped refresh signal.
func (h *Hub) SignalForTenant(tenantID, topic string) {
	h.Publish(Event{
		Type:        "signal",
		Topics:      []string{topic},
		Group:       map[string]string{"tenant_id": tenantID},
		Data:        map[string]string{"signal": "refresh"},
		Priority:    PriorityCoalesced,
		CoalesceKey: "signal:" + topic + ":" + tenantID,
	})
}

// SignalThrottled publishes a signal with a TTL — if the event can't be
// delivered before the TTL expires, it's dropped. Useful for high-frequency
// signals (analytics, live counters) where stale data is worse than no data.
func (h *Hub) SignalThrottled(topic string, ttl time.Duration) {
	h.Publish(Event{
		Type:        "signal",
		Topics:      []string{topic},
		Data:        map[string]string{"signal": "refresh"},
		Priority:    PriorityCoalesced,
		CoalesceKey: "signal:" + topic,
		TTL:         ttl,
	})
}
