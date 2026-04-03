package fibersse

import (
	"sync"
	"time"
)

// Replayer stores events for replay when a client reconnects with Last-Event-ID.
// Implement this interface to use Redis Streams, a database, or any durable store.
type Replayer interface {
	// Store persists an event for potential future replay.
	Store(event MarshaledEvent, topics []string) error

	// Replay returns all events after lastEventID that match any of the given topics.
	// Events are returned in chronological order.
	// Returns nil, nil if lastEventID is not found (client should receive full state).
	Replay(lastEventID string, topics []string) ([]MarshaledEvent, error)
}

// replayEntry pairs an event with its topic set for filtering.
type replayEntry struct {
	event     MarshaledEvent
	topics    map[string]struct{}
	timestamp time.Time
}

// MemoryReplayer is an in-memory Replayer backed by a ring buffer.
// Events older than TTL or exceeding MaxEvents are evicted.
type MemoryReplayer struct {
	mu        sync.RWMutex
	entries   []replayEntry
	maxEvents int
	ttl       time.Duration
}

// MemoryReplayerConfig configures the in-memory replayer.
type MemoryReplayerConfig struct {
	// MaxEvents is the maximum number of events to retain (default: 1000).
	MaxEvents int

	// TTL is how long events are kept before eviction (default: 5m).
	TTL time.Duration
}

// NewMemoryReplayer creates an in-memory replayer.
func NewMemoryReplayer(cfg ...MemoryReplayerConfig) *MemoryReplayer {
	c := MemoryReplayerConfig{
		MaxEvents: 1000,
		TTL:       5 * time.Minute,
	}
	if len(cfg) > 0 {
		if cfg[0].MaxEvents > 0 {
			c.MaxEvents = cfg[0].MaxEvents
		}
		if cfg[0].TTL > 0 {
			c.TTL = cfg[0].TTL
		}
	}
	return &MemoryReplayer{
		entries:   make([]replayEntry, 0, c.MaxEvents),
		maxEvents: c.MaxEvents,
		ttl:       c.TTL,
	}
}

// Store adds an event to the replay buffer.
func (r *MemoryReplayer) Store(event MarshaledEvent, topics []string) error {
	topicSet := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		topicSet[t] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.entries = append(r.entries, replayEntry{
		event:     event,
		topics:    topicSet,
		timestamp: time.Now(),
	})

	// Evict oldest if over capacity
	if len(r.entries) > r.maxEvents {
		excess := len(r.entries) - r.maxEvents
		r.entries = r.entries[excess:]
	}

	return nil
}

// Replay returns events after lastEventID matching the given topics.
func (r *MemoryReplayer) Replay(lastEventID string, topics []string) ([]MarshaledEvent, error) {
	if lastEventID == "" {
		return nil, nil
	}

	topicSet := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		topicSet[t] = struct{}{}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	cutoff := time.Now().Add(-r.ttl)

	// Find the position of lastEventID
	startIdx := -1
	for i, entry := range r.entries {
		if entry.event.ID == lastEventID {
			startIdx = i + 1
			break
		}
	}

	// ID not found — client is too far behind
	if startIdx < 0 {
		return nil, nil
	}

	var result []MarshaledEvent
	for i := startIdx; i < len(r.entries); i++ {
		entry := r.entries[i]

		// Skip expired entries
		if entry.timestamp.Before(cutoff) {
			continue
		}

		// Check topic overlap
		if matchesAnyTopic(entry.topics, topicSet) {
			result = append(result, entry.event)
		}
	}

	return result, nil
}

// matchesAnyTopic returns true if the two topic sets share at least one key.
func matchesAnyTopic(a, b map[string]struct{}) bool {
	// Iterate over the smaller set for efficiency
	if len(a) > len(b) {
		a, b = b, a
	}
	for k := range a {
		if _, ok := b[k]; ok {
			return true
		}
	}
	return false
}
