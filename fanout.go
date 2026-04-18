package fibersse

import (
	"context"
	"time"
)

// PubSubSubscriber abstracts a pub/sub system (Redis, NATS, etc.) for
// auto-fan-out from an external message broker into the SSE hub.
//
// Implement this interface to bridge any pub/sub system to fibersse.
type PubSubSubscriber interface {
	// Subscribe listens on the given channel and sends received messages
	// to the provided callback. It blocks until ctx is cancelled.
	//
	// The callback receives both the channel name (useful for pattern
	// subscriptions like "notif:*") and the raw message payload.
	Subscribe(ctx context.Context, channel string, onMessage func(channel, payload string)) error
}

// FanOutConfig configures auto-fan-out from an external pub/sub to the hub.
type FanOutConfig struct {
	// Subscriber is the pub/sub implementation (Redis, NATS, etc.).
	Subscriber PubSubSubscriber

	// Channel is the pub/sub channel to subscribe to.
	Channel string

	// Topic is the SSE topic to publish events to. If empty, Channel is used.
	Topic string

	// EventType is the SSE event type (`event:` field). Required.
	EventType string

	// Priority for delivered events (default: PriorityInstant).
	Priority Priority

	// CoalesceKey for PriorityCoalesced events. If empty, EventType is used.
	CoalesceKey string

	// TTL for events. Zero means no expiration.
	TTL time.Duration

	// Transform optionally transforms the raw pub/sub message before
	// publishing to the hub. Return nil to skip the message.
	// If nil, the raw payload is used as-is.
	Transform func(payload string) *Event
}

// FanOut starts a goroutine that subscribes to an external pub/sub channel
// and automatically publishes received messages to the SSE hub.
//
// Returns a cancel function to stop the fan-out.
//
// Example with Redis:
//
//	cancel := hub.FanOut(fibersse.FanOutConfig{
//	    Subscriber: &RedisSubscriber{client: rdb},
//	    Channel:    "notifications:tenant_123",
//	    Topic:      "notifications",
//	    EventType:  "notification",
//	    Priority:   fibersse.PriorityInstant,
//	})
//	defer cancel()
func (h *Hub) FanOut(cfg FanOutConfig) context.CancelFunc {
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
				event := h.buildFanOutEvent(cfg, topic, payload)
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

// buildFanOutEvent creates an Event from a raw pub/sub payload using the
// FanOutConfig. Returns nil if the transform function filters the message.
func (h *Hub) buildFanOutEvent(cfg FanOutConfig, topic, payload string) *Event {
	var event Event

	if cfg.Transform != nil {
		transformed := cfg.Transform(payload)
		if transformed == nil {
			return nil
		}
		event = *transformed
	} else {
		event = Event{
			Type: cfg.EventType,
			Data: payload,
		}
	}

	// Apply defaults from config
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

	return &event
}

// logFanOutError logs a fan-out subscriber error if a logger is configured.
func (h *Hub) logFanOutError(channel string, err error) {
	if h.cfg.Logger != nil {
		h.cfg.Logger.Warn("fibersse: fan-out subscriber error, retrying",
			"channel", channel,
			"error", err,
		)
	}
}

// FanOutMulti starts multiple fan-out goroutines at once.
// Returns a single cancel function that stops all of them.
func (h *Hub) FanOutMulti(configs ...FanOutConfig) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())

	for _, cfg := range configs {
		cfg := cfg // capture
		innerCancel := h.FanOut(cfg)
		go func() {
			<-ctx.Done()
			innerCancel()
		}()
	}

	return cancel
}
