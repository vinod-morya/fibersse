package fibersse

import (
	"sync"
	"time"
)

// AdaptiveThrottler monitors per-connection buffer saturation and adjusts
// the effective flush interval. Connections with high buffer usage get
// longer flush intervals (fewer sends), reducing backpressure. Connections
// with low usage get shorter intervals (faster delivery).
//
// This is applied during flushAll — connections whose adaptive interval
// hasn't elapsed since their last flush are skipped.
type adaptiveThrottler struct {
	mu sync.Mutex

	// Per-connection last flush time
	lastFlush map[string]time.Time

	// Base interval from HubConfig.FlushInterval
	baseInterval time.Duration

	// Min/max bounds
	minInterval time.Duration
	maxInterval time.Duration
}

func newAdaptiveThrottler(baseInterval time.Duration) *adaptiveThrottler {
	min := baseInterval / 4
	if min < 100*time.Millisecond {
		min = 100 * time.Millisecond
	}
	max := baseInterval * 4
	if max > 10*time.Second {
		max = 10 * time.Second
	}
	return &adaptiveThrottler{
		lastFlush:    make(map[string]time.Time),
		baseInterval: baseInterval,
		minInterval:  min,
		maxInterval:  max,
	}
}

// effectiveInterval calculates the flush interval for a connection based
// on its buffer saturation (0.0 = empty, 1.0 = full).
//
// AIMD-inspired: low saturation → decrease interval (faster delivery),
// high saturation → increase interval (slower, less pressure).
func (at *adaptiveThrottler) effectiveInterval(saturation float64) time.Duration {
	switch {
	case saturation > 0.8:
		// Critical — 4x slower to relieve pressure
		return at.maxInterval
	case saturation > 0.5:
		// Warning — 2x slower
		return at.baseInterval * 2
	case saturation < 0.1:
		// Healthy and fast client — deliver quicker
		return at.minInterval
	default:
		return at.baseInterval
	}
}

// shouldFlush returns true if enough time has passed since the last flush
// for this connection, given its current buffer saturation.
func (at *adaptiveThrottler) shouldFlush(connID string, saturation float64) bool {
	at.mu.Lock()
	defer at.mu.Unlock()

	interval := at.effectiveInterval(saturation)
	last, ok := at.lastFlush[connID]
	if !ok {
		at.lastFlush[connID] = time.Now()
		return true
	}

	if time.Since(last) >= interval {
		at.lastFlush[connID] = time.Now()
		return true
	}
	return false
}

// remove cleans up tracking for a disconnected connection.
func (at *adaptiveThrottler) remove(connID string) {
	at.mu.Lock()
	delete(at.lastFlush, connID)
	at.mu.Unlock()
}

// cleanup removes stale entries older than the given cutoff.
// Called periodically by the hub's run loop to prevent unbounded growth.
func (at *adaptiveThrottler) cleanup(cutoff time.Time) {
	at.mu.Lock()
	defer at.mu.Unlock()
	for k, v := range at.lastFlush {
		if v.Before(cutoff) {
			delete(at.lastFlush, k)
		}
	}
}
