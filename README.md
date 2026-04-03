<p align="center">
  <h1 align="center">FiberSSE</h1>
  <p align="center">
    Production-grade Server-Sent Events (SSE) for <a href="https://github.com/gofiber/fiber">Fiber v3</a>
  </p>
  <p align="center">
    <a href="https://pkg.go.dev/github.com/vinod-morya/fibersse"><img src="https://pkg.go.dev/badge/github.com/vinod-morya/fibersse.svg" alt="Go Reference"></a>
    <a href="https://goreportcard.com/report/github.com/vinod-morya/fibersse"><img src="https://goreportcard.com/badge/github.com/vinod-morya/fibersse" alt="Go Report Card"></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License"></a>
    <a href="https://github.com/vinod-morya/fibersse/releases"><img src="https://img.shields.io/github/v/release/vinod-morya/fibersse" alt="Release"></a>
    <a href="https://www.npmjs.com/package/fibersse-react"><img src="https://img.shields.io/npm/v/fibersse-react?label=React%20SDK&color=blue" alt="npm"></a>
  </p>
</p>

---

> **React SDK available:** [`npm install fibersse-react`](https://www.npmjs.com/package/fibersse-react) — hooks for TanStack Query / SWR cache invalidation. [GitHub](https://github.com/vinod-morya/fibersse-react)
>
> **Blog:** [How We Eliminated 90% of API Calls by Replacing Polling with SSE](https://personacart.com/blog/how-we-eliminated-90-percent-api-calls-replacing-polling-with-sse)

**Stop polling. Start pushing.** The only SSE library built natively for [Fiber v3](https://github.com/gofiber/fiber) — with built-in cache invalidation, event coalescing, and one-line domain event publishing.

Replace `setInterval` with one line of Go:

```go
// Before: client polls every 30 seconds (wasteful)
// setInterval(() => fetch("/api/orders"), 30_000)

// After: server pushes when data ACTUALLY changes
hub.Invalidate("orders", order.ID, "created")  // → client refetches instantly
```

**80-90% fewer API calls. Real-time UI. Zero polling.**

## Why FiberSSE?

### 1. The Only SSE Library That Works on Fiber

Every Go SSE library (`r3labs/sse`, `tmaxmax/go-sse`) is built on `net/http` and **breaks on Fiber** — `fasthttp.RequestCtx.Done()` only fires on server shutdown, not per-client disconnect. Zombie subscribers leak forever. `fibersse` uses Fiber's native `SendStreamWriter` with `w.Flush()` error detection.

### 2. Built to Kill Polling

Most SSE libraries just push events. `fibersse` has **built-in patterns for replacing polling**:

| API | What It Does | Replaces |
|-----|-------------|----------|
| `hub.Invalidate()` | Signal clients to refetch a resource | `setInterval` polling |
| `hub.DomainEvent()` | Structured event from any handler/worker | Manual event wiring |
| `hub.Progress()` | Coalesced progress (5%→8% sends only 8%) | 2s progress polling |
| `hub.Complete()` | Operation done signal (instant delivery) | Completion polling |
| `hub.Signal()` | Generic "something changed" refresh | Dashboard polling |

### 3. Every Feature a SaaS Needs



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

## Kill Polling Guide

### Step 1: Replace setInterval with Invalidation

**Backend** — publish when data changes:
```go
// In your order handler
func (h *OrderHandler) Create(c fiber.Ctx) error {
    order, err := h.svc.Create(...)
    if err != nil { return err }

    // One line — replaces 30s polling for ALL connected clients
    hub.InvalidateForTenant(tenantID, "orders", order.ID, "created")
    return c.JSON(order)
}
```

**Frontend** — listen and refetch:
```javascript
// With TanStack Query (React Query)
const es = new EventSource('/events?topics=orders');
es.addEventListener('invalidate', (e) => {
    const { resource } = JSON.parse(e.data);
    queryClient.invalidateQueries({ queryKey: [resource] });
});

// With SWR
es.addEventListener('invalidate', (e) => {
    const { resource } = JSON.parse(e.data);
    mutate(`/api/${resource}`);
});
```

### Step 2: Track Progress Without Polling

```go
// Backend — in your import worker
for i, row := range rows {
    processRow(row)
    hub.Progress("import", importID, tenantID, i+1, len(rows))
    // Fires 1000 times but client receives ~10 updates (coalesced!)
}
hub.Complete("import", importID, tenantID, true, nil)
```

```javascript
// Frontend
es.addEventListener('progress', (e) => {
    const { pct } = JSON.parse(e.data);
    setProgressBar(pct); // Smooth updates, no polling
});
es.addEventListener('complete', (e) => {
    showToast("Import complete!");
    queryClient.invalidateQueries({ queryKey: ['products'] });
});
```

### Step 3: Dashboard Signals (No Polling, Ever)

```go
// Backend — after ANY mutation that affects the dashboard
hub.SignalForTenant(tenantID, "dashboard") // coalesced, won't flood

// Or with hints:
hub.InvalidateWithHint("orders", orderID, "created", map[string]any{
    "total": 149.99,
    "customer": "John Doe",
})
```

### Impact

| Metric | Before (Polling) | After (SSE) |
|--------|-----------------|-------------|
| API calls per user/minute | ~12 (6 pages × 30s) | ~0-2 (only when data changes) |
| Time to see new data | 0-30 seconds | < 200ms |
| Server load | Constant (even idle users poll) | Proportional to actual changes |
| Battery drain (mobile) | High (constant network) | Minimal (idle connection) |

---

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
    Store(event MarshaledEvent, topics []string) error
    Replay(lastEventID string, topics []string) ([]MarshaledEvent, error)
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
├── hub.go             Core hub — New(), Publish(), Handler(), Shutdown()
├── invalidation.go    Kill polling — Invalidate(), Signal(), InvalidateForTenant()
├── domain_event.go    One-line publish — DomainEvent(), Progress(), Complete()
├── event.go           Event struct, Priority constants, SSE wire format
├── connection.go      Per-client connection, write loop, backpressure
├── coalescer.go       Batch + last-writer-wins buffers
├── topic.go           NATS-style wildcard topic matching (* and >)
├── throttle.go        Adaptive per-connection flush interval (AIMD)
├── auth.go            JWTAuth, TicketAuth, TicketStore helpers
├── fanout.go          PubSubSubscriber, FanOut(), FanOutMulti()
├── replayer.go        Last-Event-ID replay (pluggable MemoryReplayer)
├── metrics.go         PrometheusHandler, MetricsHandler
├── stats.go           HubStats struct
├── CLAUDE.md          Instructions for AI agents (Claude, Codex, Copilot)
└── hub_test.go        29 tests covering all features
```

## Integration with TanStack Query / SWR

The canonical pattern for bridging fibersse events to your React data layer:

### TanStack Query (React Query)

```typescript
import { useQueryClient } from '@tanstack/react-query';
import { useEffect } from 'react';

function useSSEInvalidation(topics: string[]) {
  const queryClient = useQueryClient();

  useEffect(() => {
    const es = new EventSource(`/events?topics=${topics.join(',')}`);

    // Single resource invalidation
    es.addEventListener('invalidate', (e) => {
      const { resource, resource_id, action, hint } = JSON.parse(e.data);

      // Invalidate the collection
      queryClient.invalidateQueries({ queryKey: [resource] });

      // Invalidate the specific item
      if (resource_id) {
        queryClient.invalidateQueries({ queryKey: [resource, resource_id] });
      }

      // Optional: update cache directly from hint (skip refetch)
      if (hint && resource_id) {
        queryClient.setQueryData([resource, resource_id], (old) =>
          old ? { ...old, ...hint } : old
        );
      }
    });

    // Batch invalidation (multiple resources in one event)
    es.addEventListener('batch', (e) => {
      const events = JSON.parse(e.data);
      const resources = new Set(events.map(e => e.resource));
      resources.forEach(resource => {
        queryClient.invalidateQueries({ queryKey: [resource] });
      });
    });

    // Progress tracking
    es.addEventListener('progress', (e) => {
      const { resource_id, pct } = JSON.parse(e.data);
      // Update local state for progress bars
    });

    // Completion
    es.addEventListener('complete', (e) => {
      const { resource_id, status } = JSON.parse(e.data);
      if (status === 'completed') {
        queryClient.invalidateQueries(); // refetch everything
      }
    });

    return () => es.close();
  }, [topics, queryClient]);
}

// Usage in any page:
function OrdersPage() {
  useSSEInvalidation(['orders', 'dashboard']);
  const { data } = useQuery({ queryKey: ['orders'], queryFn: fetchOrders });
  // ↑ Automatically refetches when server publishes hub.Invalidate("orders", ...)
}
```

### SWR

```typescript
import { useSWRConfig } from 'swr';

function useSSEInvalidation(topics: string[]) {
  const { mutate } = useSWRConfig();

  useEffect(() => {
    const es = new EventSource(`/events?topics=${topics.join(',')}`);

    es.addEventListener('invalidate', (e) => {
      const { resource, resource_id } = JSON.parse(e.data);
      mutate(`/api/${resource}`);
      if (resource_id) mutate(`/api/${resource}/${resource_id}`);
    });

    return () => es.close();
  }, [topics, mutate]);
}
```

## Versioning

This project follows [Semantic Versioning](https://semver.org/):

- **v0.x.y** — Pre-1.0 development. API may change between minor versions.
- **v1.0.0** — Stable API. Breaking changes only in major versions.

Current: **v0.3.0**.

## Roadmap

- [ ] Redis Streams Replayer (durable replay across server restarts)
- [x] React SDK ([`fibersse-react`](https://www.npmjs.com/package/fibersse-react)) — `useSSE()` and `useSSEInvalidation()` hooks
- [ ] Admin Dashboard (web UI for live connection monitoring)
- [ ] WebSocket fallback transport
- [ ] Load testing CLI (`fibersse-bench`)
- [ ] OpenTelemetry tracing integration
- [ ] TanStack Query integration example

## Contributing

Contributions are welcome! Please open an issue first to discuss what you'd like to change.

## License

[MIT](LICENSE) - Vinod Morya

## Author

**Vinod Morya** — [@vinod-morya](https://github.com/vinod-morya)

Built at [PersonaCart](https://personacart.com) — the creator commerce platform. `fibersse` powers all real-time features in PersonaCart: notifications, live analytics, media processing, curriculum generation progress, and more.
