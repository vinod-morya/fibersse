package fibersse

import (
	"context"
	"strings"
	"time"
)

// ──────────────────────────────────────────────────────────────────────────────
// Multi-Tenant Helpers
//
// Thin wrappers over the core hub primitives (FanOut, CloseWhere) that add
// tenant-aware routing for multi-tenant SaaS deployments. No new hub state,
// no new goroutines beyond what FanOut already starts.
// ──────────────────────────────────────────────────────────────────────────────

// TenantExtractor extracts a tenant ID from a pub/sub message. It receives
// both the channel name and the raw payload so it can use either or both.
// Return ("", false) to drop the message — fail-closed means no broadcast-leak.
type TenantExtractor func(channel, payload string) (tenantID string, ok bool)

// TenantFromChannelSegment extracts the tenant ID from a colon-delimited
// channel name at the given zero-based segment index.
//
//	// "orders:t_123" → segment 1 → "t_123"
//	extractor := fibersse.TenantFromChannelSegment(1)
func TenantFromChannelSegment(index int) TenantExtractor {
	return func(channel, _ string) (string, bool) {
		parts := strings.Split(channel, ":")
		if index < 0 || index >= len(parts) || parts[index] == "" {
			return "", false
		}
		return parts[index], true
	}
}

// TenantFromPayloadJSON extracts the tenant ID from a top-level JSON string
// field in the payload without importing encoding/json.
//
//	// payload: {"tenant_id":"t_123","event":"order.created"}
//	extractor := fibersse.TenantFromPayloadJSON("tenant_id")
func TenantFromPayloadJSON(field string) TenantExtractor {
	needle := `"` + field + `":"`
	return func(_, payload string) (string, bool) {
		idx := strings.Index(payload, needle)
		if idx == -1 {
			return "", false
		}
		start := idx + len(needle)
		end := strings.Index(payload[start:], `"`)
		if end == -1 || end == 0 {
			return "", false
		}
		return payload[start : start+end], true
	}
}

// TenantFanOutConfig configures a tenant-aware FanOut. It extends the core
// FanOutConfig with a TenantExtractor so each message is routed only to
// connections belonging to the extracted tenant.
type TenantFanOutConfig struct {
	// Subscriber is the pub/sub implementation (Redis, NATS, etc.).
	Subscriber PubSubSubscriber

	// Channel is the pub/sub channel or pattern to subscribe to.
	Channel string

	// Topic is the SSE topic to publish events to. Defaults to Channel.
	Topic string

	// EventType is the SSE event type (`event:` field). Required.
	EventType string

	// Priority for delivered events (default: PriorityInstant).
	Priority Priority

	// CoalesceKey for PriorityCoalesced events. Defaults to EventType.
	CoalesceKey string

	// TTL for events. Zero means no expiration.
	TTL time.Duration

	// TenantExtractor extracts the tenant ID from the channel + payload.
	// If nil, events are broadcast to all connections on the topic.
	TenantExtractor TenantExtractor

	// FailClosed drops the message when TenantExtractor returns ok=false.
	// Set true for tenant-private data to prevent cross-tenant leakage.
	FailClosed bool

	// Transform optionally rewrites the raw payload into a custom Event.
	// Return nil to skip the message.
	Transform func(payload string) *Event
}

// TenantFanOut starts a tenant-aware fan-out goroutine that subscribes to
// an external pub/sub channel and routes each message to connections matching
// the extracted tenant ID via Group metadata.
//
// A single call handles all tenants on a wildcard channel:
//
//	cancel := hub.TenantFanOut(fibersse.TenantFanOutConfig{
//	    Subscriber:      myRedis,
//	    Channel:         "notifications:*",
//	    Topic:           "notifications",
//	    EventType:       "notification",
//	    Priority:        fibersse.PriorityInstant,
//	    TenantExtractor: fibersse.TenantFromChannelSegment(1),
//	    FailClosed:      true,
//	})
//	defer cancel()
func (h *Hub) TenantFanOut(cfg TenantFanOutConfig) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())

	topic := cfg.Topic
	if topic == "" {
		topic = cfg.Channel
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			err := cfg.Subscriber.Subscribe(ctx, cfg.Channel, func(channel, payload string) {
				event := buildTenantEvent(cfg, topic, channel, payload)
				if event != nil {
					h.Publish(*event)
				}
			})

			if err != nil && ctx.Err() == nil {
				h.logFanOutError(cfg.Channel, err)
				select {
				case <-time.After(3 * time.Second):
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return cancel
}

// buildTenantEvent constructs the Event for a TenantFanOut delivery,
// applying tenant scoping and FailClosed semantics.
func buildTenantEvent(cfg TenantFanOutConfig, topic, channel, payload string) *Event {
	var event Event

	if cfg.Transform != nil {
		transformed := cfg.Transform(payload)
		if transformed == nil {
			return nil
		}
		event = *transformed
	} else {
		event = Event{Type: cfg.EventType, Data: payload}
	}

	if len(event.Topics) == 0 {
		event.Topics = []string{topic}
	}
	if event.Type == "" {
		event.Type = cfg.EventType
	}
	if event.Priority == 0 && cfg.Priority != 0 {
		event.Priority = cfg.Priority
	}
	if event.CoalesceKey == "" && cfg.CoalesceKey != "" {
		event.CoalesceKey = cfg.CoalesceKey
	}
	if event.TTL == 0 && cfg.TTL > 0 {
		event.TTL = cfg.TTL
	}

	if cfg.TenantExtractor != nil {
		tenantID, ok := cfg.TenantExtractor(channel, payload)
		if !ok && cfg.FailClosed {
			return nil
		}
		if ok && tenantID != "" {
			event.Group = map[string]string{"tenant_id": tenantID}
		}
	}

	return &event
}

// DrainTenant closes all SSE connections belonging to the given tenant.
// Use for account suspension or forced reconnect after plan change.
// Returns the number of connections closed.
//
//	hub.DrainTenant("t_123")
func (h *Hub) DrainTenant(tenantID string) int {
	return h.CloseWhere(func(conn *Connection) bool {
		return conn.Metadata["tenant_id"] == tenantID
	})
}

// DrainUser closes all SSE connections belonging to the given user.
// Use for session revocation or forced re-auth after permission change.
// Returns the number of connections closed.
//
//	hub.DrainUser("u_456")
func (h *Hub) DrainUser(userID string) int {
	return h.CloseWhere(func(conn *Connection) bool {
		return conn.Metadata["user_id"] == userID
	})
}

// ActiveTenantIDs returns the unique set of tenant IDs that currently have
// at least one open connection subscribed to the given topic.
//
// Use this to scope expensive backend queries only to tenants who are
// actively watching — idle tenants skip the query entirely:
//
//	tenants := hub.ActiveTenantIDs("intelligence")
//	if len(tenants) == 0 {
//	    return // no one watching, skip DB query
//	}
//	rows, _ := db.Query("... WHERE tenant_id = ANY($1)", tenants)
func (h *Hub) ActiveTenantIDs(topic string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	seen := make(map[string]struct{})
	for _, conn := range h.connections {
		if conn.IsClosed() {
			continue
		}
		tid := conn.Metadata["tenant_id"]
		if tid == "" {
			continue
		}
		for _, t := range conn.Topics {
			if t == topic {
				seen[tid] = struct{}{}
				break
			}
		}
	}

	result := make([]string, 0, len(seen))
	for tid := range seen {
		result = append(result, tid)
	}
	return result
}
