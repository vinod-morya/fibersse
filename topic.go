package fibersse

import "strings"

// topicMatch checks if a subscription pattern matches a concrete topic.
// Supports NATS-style wildcards:
//
//   - `*` matches exactly one segment (between dots)
//   - `>` matches one or more trailing segments (must be last token)
//   - No wildcards = exact match
//
// Examples:
//
//	topicMatch("notifications.*", "notifications.orders")     → true
//	topicMatch("notifications.*", "notifications.orders.new") → false
//	topicMatch("analytics.>", "analytics.live")               → true
//	topicMatch("analytics.>", "analytics.live.visitors")      → true
//	topicMatch("analytics.>", "analytics")                    → false
//	topicMatch("events", "events")                            → true
//	topicMatch("events", "events.sub")                        → false
func topicMatch(pattern, topic string) bool {
	// Fast path: no wildcards
	if !strings.ContainsAny(pattern, "*>") {
		return pattern == topic
	}

	patParts := strings.Split(pattern, ".")
	topParts := strings.Split(topic, ".")

	for i, pp := range patParts {
		switch pp {
		case ">":
			// `>` must be the last token and matches 1+ remaining segments
			return i < len(topParts)
		case "*":
			// `*` matches exactly one segment
			if i >= len(topParts) {
				return false
			}
			// Segment matched, continue
		default:
			// Literal match
			if i >= len(topParts) || pp != topParts[i] {
				return false
			}
		}
	}

	// All pattern parts consumed — topic must also be fully consumed
	return len(patParts) == len(topParts)
}

// topicMatchesAny returns true if the concrete topic matches any of the patterns.
func topicMatchesAny(patterns []string, topic string) bool {
	for _, p := range patterns {
		if topicMatch(p, topic) {
			return true
		}
	}
	return false
}

// connMatchesTopic returns true if a connection's subscription patterns
// match the given concrete topic.
func connMatchesTopic(conn *Connection, topic string) bool {
	return topicMatchesAny(conn.Topics, topic)
}
