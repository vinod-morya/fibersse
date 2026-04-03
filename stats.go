package fibersse

import "sync/atomic"

// HubStats provides a snapshot of the hub's current state.
type HubStats struct {
	// ActiveConnections is the total number of open SSE connections.
	ActiveConnections int `json:"active_connections"`

	// TotalTopics is the number of unique topics with at least one subscriber.
	TotalTopics int `json:"total_topics"`

	// EventsPublished is the lifetime count of events published to the hub.
	EventsPublished int64 `json:"events_published"`

	// EventsDropped is the lifetime count of events dropped due to backpressure.
	EventsDropped int64 `json:"events_dropped"`

	// ConnectionsByTopic maps each topic to its subscriber count.
	ConnectionsByTopic map[string]int `json:"connections_by_topic"`
}

// hubMetrics tracks lifetime counters for the hub.
type hubMetrics struct {
	eventsPublished atomic.Int64
	eventsDropped   atomic.Int64
}
