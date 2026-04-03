<p align="center">
  <h1 align="center">fibersse</h1>
  <p align="center">
    Production-grade Server-Sent Events (SSE) for <a href="https://github.com/gofiber/fiber">Fiber v3</a>
  </p>
  <p align="center">
    <a href="https://pkg.go.dev/github.com/vinod-morya/fibersse"><img src="https://pkg.go.dev/badge/github.com/vinod-morya/fibersse.svg" alt="Go Reference"></a>
    <a href="https://goreportcard.com/report/github.com/vinod-morya/fibersse"><img src="https://goreportcard.com/badge/github.com/vinod-morya/fibersse" alt="Go Report Card"></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License"></a>
    <a href="https://github.com/vinod-morya/fibersse/releases"><img src="https://img.shields.io/github/v/release/vinod-morya/fibersse" alt="Release"></a>
  </p>
</p>

---

The **only** SSE library built natively for [Fiber v3](https://github.com/gofiber/fiber) (fasthttp). No `net/http` adapters, no broken disconnect detection, no workarounds.

Built for production SaaS: event coalescing, priority lanes, NATS-style topic wildcards, adaptive throttling, connection groups, built-in auth, Prometheus metrics, graceful drain, and auto fan-out from Redis/NATS.

## Why fibersse?

Every Fiber v3 project that needs SSE hand-rolls the same `SendStreamWriter` + `w.Flush()` pattern. Existing Go SSE libraries (`r3labs/sse`, `tmaxmax/go-sse`) are built on `net/http` and **break on Fiber** — specifically, client disconnect detection fails because `fasthttp.RequestCtx.Done()` only fires on server shutdown, not per-client disconnect. This causes zombie subscribers that leak forever.

**fibersse** solves this:

| Feature | r3labs/sse | tmaxmax/go-sse | **fibersse** |
|---------|-----------|----------------|-------------|
| Fiber v3 native | No | No | **Yes** |
| Disconnect detection | Broken on Fiber | Broken on Fiber | **Works** (flush-based) |
| Event coalescing | No | No | **Yes** (last-writer-wins) |
| Priority lanes | No | No | **Yes** (P0 instant / P1 batched / P2 coalesced) |
| Topic wildcards | No | No | **Yes** (NATS-style `*` and `>`) |
| Adaptive throttling | No | No | **Yes** (buffer-depth AIMD) |
| Connection groups | No | No | **Yes** (publish by metadata) |
| Backpressure | Blocks sender | Blocks sender | **Drops + reconnect hint** |
| Built-in auth | No | No | **Yes** (JWT + ticket helpers) |
| Prometheus metrics | No | No | **Yes** |
| Graceful drain | No | No | **Yes** (Kubernetes-style) |
| Event TTL | No | No | **Yes** |
| Last-Event-ID replay | Yes | Yes | **Yes** (pluggable) |
| Fan-out middleware | No | No | **Yes** (Redis/NATS bridge) |

## Install

```bash
go get github.com/vinod-morya/fibersse@latest
```

**Requirements**: Go 1.23+ and Fiber v3.

## Quick Start

```go
package main

import (
    "time"
    "github.com/gofiber/fiber/v3"
    "github.com/vinod-morya/fibersse"
)

func main() {
    app := fiber.New()

    // Create the SSE hub
    hub := fibersse.New(fibersse.HubConfig{
        FlushInterval:     2 * time.Second,
        HeartbeatInterval: 30 * time.Second,
        OnConnect: func(c fiber.Ctx, conn *fibersse.Connection) error {
            // Authenticate and set topics
            conn.Topics = []string{"notifications", "live"}
            conn.Metadata["user_id"] = "user_123"
            return nil
        },
    })

    // Mount the SSE endpoint
    app.Get("/events", hub.Handler())

    // Publish events from anywhere in your app
    go func() {
        for i := 0; ; i++ {
            hub.Publish(fibersse.Event{
                Type:   "heartbeat",
                Data:   map[string]int{"count": i},
                Topics: []string{"live"},
            })
            time.Sleep(5 * time.Second)
        }
    }()

    app.Listen(":3000")
}
```

**Client** (browser):
```javascript
const es = new EventSource('/events');
es.addEventListener('heartbeat', (e) => {
    console.log(JSON.parse(e.data)); // { count: 0 }
});
es.addEventListener('notification', (e) => {
    showToast(JSON.parse(e.data));
});
```

## Features

### Event Priority & Coalescing

Three priority lanes control how events reach clients:

```go
// P0: INSTANT — bypasses all buffering, sent immediately
// Use for: notifications, errors, chat messages, auth revocations
hub.Publish(fibersse.Event{
    Type:     "notification",
    Data:     map[string]string{"title": "New order!"},
    Topics:   []string{"notifications"},
    Priority: fibersse.PriorityInstant,
})

// P1: BATCHED — collected in a time window, all sent together
// Use for: status updates, media processing
hub.Publish(fibersse.Event{
    Type:     "media_status",
    Data:     map[string]string{"id": "m_1", "status": "ready"},
    Topics:   []string{"media"},
    Priority: fibersse.PriorityBatched,
})

// P2: COALESCED — last-writer-wins per key
// If progress goes 5% → 6% → 7% → 8% in 2 seconds, client receives only 8%
hub.Publish(fibersse.Event{
    Type:        "progress",
    Data:        map[string]int{"pct": 8},
    Topics:      []string{"tasks"},
    Priority:    fibersse.PriorityCoalesced,
    CoalesceKey: "task:abc123",
})
```

### Topic Wildcards (NATS-style)

Subscribe to topic patterns using `*` (one segment) and `>` (one or more trailing segments):

```go
// Client subscribes to "analytics.*"
conn.Topics = []string{"analytics.*"}

// These events all match:
hub.Publish(fibersse.Event{Topics: []string{"analytics.live"}})      // matched by *
hub.Publish(fibersse.Event{Topics: []string{"analytics.revenue"}})   // matched by *

// Subscribe to everything under analytics:
conn.Topics = []string{"analytics.>"}

// Now these also match:
hub.Publish(fibersse.Event{Topics: []string{"analytics.live.visitors"}})   // matched by >
hub.Publish(fibersse.Event{Topics: []string{"analytics.funnel.checkout"}}) // matched by >
```

### Connection Groups

Publish to connections by metadata instead of topics — perfect for multi-tenant SaaS:

```go
// During OnConnect, set metadata:
conn.Metadata["tenant_id"] = "t_123"
conn.Metadata["plan"] = "pro"

// Publish to ALL connections for a specific tenant:
hub.Publish(fibersse.Event{
    Type:  "tenant_update",
    Data:  map[string]string{"message": "Plan upgraded"},
    Group: map[string]string{"tenant_id": "t_123"},
})

// Publish to all pro-plan users:
hub.Publish(fibersse.Event{
    Type:  "feature_announcement",
    Data:  "New feature available!",
    Group: map[string]string{"plan": "pro"},
})
```

### Adaptive Throttling

The hub automatically adjusts flush intervals per connection based on buffer saturation:

| Buffer Saturation | Effective Interval | Behavior |
|---|---|---|
| < 10% (healthy) | FlushInterval / 4 | Fast delivery |
| 10-50% (normal) | FlushInterval | Default cadence |
| 50-80% (warning) | FlushInterval × 2 | Slowing down |
| > 80% (critical) | FlushInterval × 4 | Backpressure relief |

Mobile users on slow connections automatically get fewer updates. Desktop users on fast connections get near-real-time delivery. Zero configuration needed.

### Client Visibility Hints

Pause non-critical events for hidden browser tabs:

```go
// Server-side: pause/resume a connection
hub.SetPaused(connID, true)   // tab hidden → skip P1/P2 events
hub.SetPaused(connID, false)  // tab visible → resume all events
```

P0 (instant) events are **always** delivered regardless of pause state — critical messages like errors and auth revocations never get dropped.

### Built-in Authentication

**JWT Auth** — validate Bearer tokens or query parameters:

```go
hub := fibersse.New(fibersse.HubConfig{
    OnConnect: fibersse.JWTAuth(func(token string) (map[string]string, error) {
        claims, err := myJWTValidator(token)
        if err != nil {
            return nil, err
        }
        return map[string]string{
            "tenant_id": claims.TenantID,
            "user_id":   claims.UserID,
        }, nil
    }),
})
```

**Ticket Auth** — one-time tickets for EventSource (which can't send headers):

```go
store := fibersse.NewMemoryTicketStore() // or implement TicketStore with Redis

// Issue ticket (in your authenticated POST endpoint):
ticket, _ := fibersse.IssueTicket(store, `{"tenant":"t1","topics":"notifications,live"}`, 30*time.Second)

// Use ticket auth in hub:
hub := fibersse.New(fibersse.HubConfig{
    OnConnect: fibersse.TicketAuth(store, func(value string) (map[string]string, []string, error) {
        var data struct{ Tenant, Topics string }
        json.Unmarshal([]byte(value), &data)
        return map[string]string{"tenant_id": data.Tenant},
               strings.Split(data.Topics, ","), nil
    }),
})
```

### Auto Fan-Out (Redis/NATS Bridge)

Bridge external pub/sub to SSE with one line:

```go
// Redis pub/sub → SSE (implement PubSubSubscriber interface)
cancel := hub.FanOut(fibersse.FanOutConfig{
    Subscriber: myRedisSubscriber,
    Channel:    "notifications:tenant_123",
    Topic:      "notifications",
    EventType:  "notification",
    Priority:   fibersse.PriorityInstant,
})
defer cancel()

// Multiple channels at once:
cancel := hub.FanOutMulti(
    fibersse.FanOutConfig{Subscriber: redis, Channel: "notifications:*", Topic: "notifications", EventType: "notification", Priority: fibersse.PriorityInstant},
    fibersse.FanOutConfig{Subscriber: redis, Channel: "media:*", Topic: "media", EventType: "media_status", Priority: fibersse.PriorityBatched},
    fibersse.FanOutConfig{Subscriber: redis, Channel: "import:*", Topic: "import", EventType: "progress", Priority: fibersse.PriorityCoalesced},
)
defer cancel()
```

Implement the `PubSubSubscriber` interface for your broker:

```go
type PubSubSubscriber interface {
    Subscribe(ctx context.Context, channel string, onMessage func(payload string)) error
}
```

### Event TTL

Drop stale events instead of delivering outdated data:

```go
hub.Publish(fibersse.Event{
    Type:   "live_count",
    Data:   map[string]int{"visitors": 42},
    Topics: []string{"live"},
    TTL:    5 * time.Second, // useless after 5 seconds
})
```

### Prometheus Metrics

Built-in monitoring endpoints:

```go
// JSON metrics (for dashboards)
app.Get("/admin/sse/metrics", hub.MetricsHandler())

// Prometheus format (for Grafana/Datadog)
app.Get("/metrics/sse", hub.PrometheusHandler())
```

Exposed metrics:
- `fibersse_connections_active` — current open connections
- `fibersse_connections_paused` — hidden-tab connections
- `fibersse_events_published_total` — lifetime events published
- `fibersse_events_dropped_total` — events dropped (backpressure/TTL)
- `fibersse_pending_events` — events buffered in coalescers
- `fibersse_buffer_saturation_avg` — average send buffer usage
- `fibersse_buffer_saturation_max` — worst-case buffer usage
- `fibersse_connections_by_topic{topic="..."}` — per-topic breakdown

### Last-Event-ID Replay

Pluggable replay for reconnecting clients:

```go
hub := fibersse.New(fibersse.HubConfig{
    Replayer: fibersse.NewMemoryReplayer(fibersse.MemoryReplayerConfig{
        MaxEvents: 1000,
        TTL:       5 * time.Minute,
    }),
})
```

Implement the `Replayer` interface for Redis Streams or any durable store:

```go
type Replayer interface {
    Store(event marshaledEvent, topics []string) error
    Replay(lastEventID string, topics []string) ([]marshaledEvent, error)
}
```

### Graceful Drain (Kubernetes-style)

On shutdown, the hub:
1. Enters drain mode (rejects new connections with `503 + Retry-After: 5`)
2. Sends `server-shutdown` event to all connected clients
3. Waits for context deadline to let clients reconnect elsewhere
4. Closes all connections and stops the run loop

```go
// In your shutdown handler:
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
hub.Shutdown(ctx)
```

### Backpressure

Each connection has a bounded send buffer (default: 256 events). If a client can't keep up:
- New events are **dropped** (not queued infinitely)
- `MessagesDropped` counter increments
- Monitor via `hub.Metrics()` to identify slow clients
- The client's EventSource auto-reconnects and gets current state

## Configuration

```go
fibersse.HubConfig{
    FlushInterval:     2 * time.Second,   // P1/P2 coalescing window
    SendBufferSize:    256,               // per-connection buffer capacity
    HeartbeatInterval: 30 * time.Second,  // keepalive for disconnect detection
    MaxLifetime:       30 * time.Minute,  // max connection duration (0 = unlimited)
    RetryMS:           3000,              // client reconnection hint (ms)
    Replayer:          nil,               // Last-Event-ID replay (nil = disabled)
    Logger:            slog.Default(),    // structured logging (nil = disabled)
    OnConnect:         nil,               // auth + topic selection callback
    OnDisconnect:      nil,               // cleanup callback
}
```

## Architecture

```
                Publish()
                   │
                   ▼
  ┌────────────────────────────────────────┐
  │  Hub Run Loop (single goroutine)       │
  │                                        │
  │  register   ◄── new connections        │
  │  unregister ◄── disconnects            │
  │  events     ◄── published events       │
  │                                        │
  │  For each event:                       │
  │    1. Match topics (exact + wildcard)  │
  │    2. Match groups (metadata k-v)      │
  │    3. Skip paused connections (P1/P2)  │
  │    4. Route by priority:               │
  │       P0 → send channel (immediate)    │
  │       P1 → batch buffer               │
  │       P2 → coalesce buffer (LWW)      │
  │                                        │
  │  Flush ticker (every FlushInterval):   │
  │    Adaptive throttle per connection    │
  │    Drain batch + coalesce → send chan  │
  │                                        │
  │  Heartbeat ticker:                     │
  │    Send comment to idle connections    │
  └────────────────────────────────────────┘
                   │
                   ▼ (per-connection send channel)
  ┌────────────────────────────────────────┐
  │  Connection Writer (in SendStreamWriter)│
  │                                        │
  │  for event := range sendChan:          │
  │    write SSE format → bufio.Writer     │
  │    w.Flush() → detect disconnect       │
  └────────────────────────────────────────┘
```

## File Structure

```
fibersse/
├── hub.go          Hub struct, New(), Publish(), Handler(), Shutdown()
├── connection.go   Connection struct, writer goroutine, backpressure
├── coalescer.go    Per-connection batch + last-writer-wins buffers
├── event.go        Event struct, Priority constants, SSE wire format
├── topic.go        NATS-style wildcard topic matching
├── throttle.go     Adaptive per-connection flush interval (AIMD)
├── replayer.go     Replayer interface + MemoryReplayer
├── auth.go         JWTAuth, TicketAuth, TicketStore helpers
├── fanout.go       PubSubSubscriber interface, FanOut(), FanOutMulti()
├── metrics.go      MetricsHandler, PrometheusHandler, MetricsSnapshot
├── stats.go        HubStats struct
└── hub_test.go     29 tests covering all features
```

## Versioning

This project follows [Semantic Versioning](https://semver.org/):

- **v0.x.y** — Pre-1.0 development. API may change between minor versions.
- **v1.0.0** — Stable API. Breaking changes only in major versions.

Current: **v0.1.0** (initial release).

## Roadmap

- [ ] Redis Streams Replayer (durable replay across server restarts)
- [ ] Admin Dashboard (web UI for live connection monitoring)
- [ ] TypeScript client SDK (`@vinod-morya/fibersse-client`)
- [ ] WebSocket fallback transport
- [ ] Load testing CLI (`fibersse-bench`)
- [ ] OpenTelemetry tracing integration

## Contributing

Contributions are welcome! Please open an issue first to discuss what you'd like to change.

## License

[MIT](LICENSE) - Vinod Morya

## Author

**Vinod Morya** — [@vinod-morya](https://github.com/vinod-morya)

Built at [PersonaCart](https://personacart.com) — the creator commerce platform. `fibersse` powers all real-time features in PersonaCart: notifications, live analytics, media processing, curriculum generation progress, and more.
