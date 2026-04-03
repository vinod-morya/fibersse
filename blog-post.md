---
title: "How We Eliminated 90% of API Calls by Replacing Polling with SSE"
published: true
description: "We built fibersse — an open-source SSE library for Go Fiber v3 — and used it to replace setInterval polling across our entire SaaS platform. Here's what we learned."
tags: go, opensource, webdev, performance
cover_image: https://dev-to-uploads.s3.amazonaws.com/uploads/articles/placeholder.png
canonical_url: https://github.com/vinod-morya/fibersse
---

# How We Eliminated 90% of API Calls by Replacing Polling with SSE

At [PersonaCart](https://personacart.com), we run a creator commerce platform — 50+ pages, 14 languages, multi-tenant SaaS. Our dashboard, order management, analytics, and notifications all needed real-time data.

Like most teams, we started with polling:

```javascript
// This was EVERYWHERE in our codebase
setInterval(() => fetch("/api/orders").then(r => r.json()).then(setOrders), 30000);
```

Six pages polling every 30 seconds. Per user. Always. Even when nothing changed.

**The math was ugly:** 6 pages × 2 API calls each × 30s = 12 requests/minute per user. With 100 concurrent users, that's 1,200 API calls per minute hitting our Go backend. 90%+ of those returned the exact same data as the previous request.

We needed Server-Sent Events. But we run on Go [Fiber v3](https://github.com/gofiber/fiber), and there was a problem.

## Every SSE Library Breaks on Fiber

We tried the popular Go SSE libraries:

- **r3labs/sse** (1,000+ stars) — built on `net/http`
- **tmaxmax/go-sse** (500+ stars) — built on `net/http`

Both look great. Neither works with Fiber.

Fiber v3 is built on **fasthttp**, not the standard `net/http`. When you bridge these libraries to Fiber via `fasthttpadaptor`, there's a **fatal bug**: `fasthttp.RequestCtx.Done()` only fires on **server shutdown**, not when an individual client disconnects.

What this means in practice: every client that disconnects becomes a **zombie subscriber**. The SSE library keeps trying to send events to dead connections. Memory grows. Goroutines leak. The server eventually runs out of resources.

We confirmed this in [Fiber issue #3307](https://github.com/gofiber/fiber/issues/3307) and [#4145](https://github.com/gofiber/fiber/issues/4145). It's an architectural limitation of fasthttp, not a bug anyone is going to fix.

## So We Built Our Own

We open-sourced it: **[fibersse](https://github.com/vinod-morya/fibersse)** — the only SSE library built natively for Fiber v3.

But we didn't just build "SSE that works on Fiber." We built it to **kill polling**.

### The Key Insight: You Don't Need to Send Data. You Need to Send Signals.

Most SSE tutorials show this pattern:

```go
// Traditional: push the full data over SSE
hub.Publish(Event{Type: "orders", Data: allOrders})
```

But that's just moving the problem. Now your SSE connection is sending the same bulky payloads your polling was sending.

Instead, we built **cache invalidation signals**:

```go
// Our approach: just tell the client "orders changed"
hub.Invalidate("orders", order.ID, "created")
```

The client receives a tiny signal (`{resource: "orders", action: "created", resource_id: "ord_123"}`) and refetches ONLY the specific data that changed. Combined with TanStack Query's `invalidateQueries()`, the client cache handles everything:

```typescript
es.addEventListener('invalidate', (e) => {
    const { resource, resource_id } = JSON.parse(e.data);
    queryClient.invalidateQueries({ queryKey: [resource] });
    if (resource_id) {
        queryClient.invalidateQueries({ queryKey: [resource, resource_id] });
    }
});
```

**No polling. No stale data. The UI updates within 200ms of the server mutation.**

### Event Coalescing: The Feature Nobody Else Has

We have a CSV import feature that processes 10,000 rows. The worker fires a progress event per row. Without coalescing, that's 10,000 SSE events in ~30 seconds — the browser can't keep up.

fibersse has **three priority lanes**:

- **P0 Instant**: notifications, errors, chat messages → bypass all buffering
- **P1 Batched**: status changes → collected in a 2-second window
- **P2 Coalesced**: progress bars, live counters → **last-writer-wins per key**

For our import:

```go
for i, row := range rows {
    processRow(row)
    hub.Progress("import", importID, tenantID, i+1, len(rows))
    // Fires 10,000 times...
}
// ...but the client receives ~15 updates (one per 2-second flush window)
```

Progress goes 1% → 2% → 3% → 4% → ... → 8% within one flush window. The client receives only **8%**. The intermediate values are overwritten by the latest. Zero wasted bandwidth.

### Adaptive Throttling: Every Connection Gets Its Own Speed

Not all clients are equal. A developer on a fast desktop connection can handle updates every 200ms. A user on mobile 3G can't.

fibersse monitors each connection's **buffer saturation** and automatically adjusts the flush interval:

| Buffer Usage | Effective Interval | What's Happening |
|---|---|---|
| < 10% | 500ms | Client is fast, deliver quickly |
| 10-50% | 2s (default) | Normal operation |
| 50-80% | 4s | Client is falling behind |
| > 80% | 8s | Backpressure relief mode |

Zero configuration. The hub adapts per-connection in real-time. Mobile users stop getting overwhelmed. Desktop users get near-real-time updates.

## The Results

After migrating our 6 polling pages to SSE invalidation:

| Metric | Before (Polling) | After (SSE) | Change |
|--------|-----------------|-------------|--------|
| API calls per user/minute | ~12 | ~0.5 | **-96%** |
| Time to see new data | 0-30 seconds | < 200ms | **~100x faster** |
| Server CPU (100 users) | Constant 35% | 8% (idle), spikes on mutations | **-77%** |
| Goroutines (SSE) | 400+ (9 endpoints × users) | ~100 (1 endpoint × users) | **-75%** |

The backend went from constantly serving identical responses to serving nothing until data actually changes. The difference is dramatic.

## How to Use It

```bash
go get github.com/vinod-morya/fibersse@latest
```

### Backend (5 lines to start)

```go
hub := fibersse.New(fibersse.HubConfig{
    OnConnect: func(c fiber.Ctx, conn *fibersse.Connection) error {
        conn.Topics = []string{"orders", "dashboard"}
        conn.Metadata["tenant_id"] = getTenantID(c)
        return nil
    },
})
app.Get("/events", hub.Handler())
```

### Publish from any handler

```go
func (h *OrderHandler) Create(c fiber.Ctx) error {
    order, err := h.svc.Create(...)
    if err != nil { return err }

    // One line — replaces polling for ALL connected clients in this tenant
    hub.InvalidateForTenant(tenantID, "orders", order.ID, "created")

    return c.JSON(order)
}
```

### Frontend (TanStack Query)

```typescript
const es = new EventSource('/events?topics=orders,dashboard');

es.addEventListener('invalidate', (e) => {
    const { resource } = JSON.parse(e.data);
    queryClient.invalidateQueries({ queryKey: [resource] });
});
```

That's it. No more `setInterval`. No more wasted API calls. The UI is real-time.

## What's in the Library

Beyond basic SSE, fibersse ships with:

- **Event coalescing** (last-writer-wins per key)
- **3 priority lanes** (instant / batched / coalesced)
- **NATS-style topic wildcards** (`analytics.*` matches `analytics.live`, `analytics.revenue`)
- **Connection groups** (publish by tenant_id, plan, role — not just topics)
- **Adaptive per-connection throttling**
- **Built-in JWT + ticket auth** (EventSource can't send headers — we solve this)
- **Prometheus metrics** endpoint
- **Last-Event-ID replay** (pluggable — in-memory default, Redis optional)
- **Graceful Kubernetes-style drain** on shutdown
- **Batch domain events** (order + inventory + dashboard in one SSE frame)

~3,500 lines of Go. 29 tests. MIT license. Zero dependencies beyond Fiber v3.

## Try It

- **GitHub**: [github.com/vinod-morya/fibersse](https://github.com/vinod-morya/fibersse)
- **Go Docs**: [pkg.go.dev/github.com/vinod-morya/fibersse](https://pkg.go.dev/github.com/vinod-morya/fibersse)
- **React SDK**: `@fibersse/react` (coming soon)

If your Fiber app is polling, you're burning server resources for no reason. Switch to SSE invalidation. It took us a week to migrate 6 pages, and our server load dropped 77%.

---

*Built by [Vinod Morya](https://github.com/vinod-morya) at [PersonaCart](https://personacart.com).*
