# fibersse Launch Posts — Ready to Copy-Paste

## 1. Reddit r/golang

**URL**: https://www.reddit.com/r/golang/submit  
**Post Type**: Link post  
**Title**: `I built fibersse — production SSE for Fiber v3 because every existing library breaks on fasthttp`  
**URL**: `https://github.com/vinod-morya/fibersse`  

**Comment to add after posting** (this is where the engagement happens):

```
I built this because I needed SSE at scale for my SaaS product (PersonaCart) which runs on Fiber v3.

The problem: r3labs/sse and tmaxmax/go-sse are built on net/http. When you use them with Fiber via the fasthttpadaptor, client disconnect detection breaks — fasthttp.RequestCtx.Done() only fires on server shutdown, not per-client disconnect. This means zombie subscribers accumulate forever and leak memory.

Fiber's own official recipe (gofiber/recipes/sse) is just raw SendStreamWriter with no hub, no pub/sub, no heartbeat management.

So I built fibersse from scratch, native to Fiber v3:

- Hub pattern with single-goroutine event loop
- 3 priority lanes: Instant (bypass all buffering), Batched (time window), Coalesced (last-writer-wins per key)
- Event coalescing: if progress goes 5% → 6% → 7% → 8% in 2 seconds, client receives only 8%
- NATS-style topic wildcards (notifications.* matches notifications.orders)
- Adaptive throttling: slow clients automatically get longer flush intervals
- Built-in JWT and ticket auth helpers
- Prometheus metrics endpoint
- Graceful Kubernetes-style drain on shutdown

~1,500 lines, 29 tests, zero deps beyond Fiber v3. MIT license.

Would love feedback from folks who've dealt with SSE at scale. The event coalescing and adaptive throttling are the features I'm most interested in getting reviewed.

Install: `go get github.com/vinod-morya/fibersse@v0.1.0`
Docs: https://pkg.go.dev/github.com/vinod-morya/fibersse
```

---

## 2. Hacker News (Show HN)

**URL**: https://news.ycombinator.com/submit  
**Title**: `Show HN: fibersse – Production SSE for Go Fiber with event coalescing and adaptive throttling`  
**URL**: `https://github.com/vinod-morya/fibersse`  

*(No text body for Show HN link posts — add a comment after:)*

```
Built this because no Go SSE library works correctly with Fiber v3 (fasthttp). The core issue: fasthttp.RequestCtx.Done() only fires on server shutdown, not per-client disconnect. Every net/http-based SSE library creates zombie subscribers on Fiber.

Key features that don't exist in any other SSE library:

1. Event coalescing — progress 5%→8% sends only 8% (last-writer-wins per key)
2. Priority lanes — P0 (instant, bypass all buffering) vs P1/P2 (batched/coalesced)
3. Adaptive throttling — slow clients automatically get fewer updates (AIMD-based)
4. NATS-style topic wildcards — subscribe to "analytics.*" instead of listing every sub-topic
5. Connection groups — publish to all connections matching metadata like tenant_id

~1,500 lines of Go, 29 tests, MIT license. Currently powering all real-time features at PersonaCart (notifications, live analytics, media processing, curriculum generation).
```

---

## 3. Twitter/X Thread

```
🧵 I built fibersse — the first production-grade SSE library for @goaborit Fiber v3.

Why? Because every existing Go SSE library breaks on Fiber/fasthttp. Here's the story:

1/ The problem: r3labs/sse and tmaxmax/go-sse use net/http. When bridged to Fiber via fasthttpadaptor, client disconnect detection breaks. fasthttp.RequestCtx.Done() only fires on SERVER shutdown, not client disconnect. Result: zombie subscribers that leak forever.

2/ We had 9 SSE endpoints in our Go backend, each hand-rolling the same SendStreamWriter pattern. 1200 lines of scattered code. 4 different connection limit mechanisms. No consistency.

3/ So I built fibersse from scratch. Native to Fiber v3. No adapters.

Key features no other SSE library has:
- Event coalescing (progress 5→8%, send only 8%)
- 3 priority lanes (instant/batched/coalesced)
- NATS-style topic wildcards
- Adaptive per-connection throttling
- Built-in JWT + ticket auth

4/ The event coalescing is the killer feature. If your progress bar fires 20 events in 2 seconds, the client only receives 1 event with the latest value. Server-side intelligence, not client-side debouncing.

5/ Available now:
go get github.com/vinod-morya/fibersse@v0.1.0

GitHub: github.com/vinod-morya/fibersse
Docs: pkg.go.dev/github.com/vinod-morya/fibersse

~1,500 lines. 29 tests. MIT license. Zero deps beyond Fiber v3.

Built at @PersonaCart — would love feedback from the Go community 🙏
```

---

## 4. LinkedIn Post

```
I just open-sourced fibersse — a production-grade Server-Sent Events library for Go Fiber v3.

Why I built it:

At PersonaCart, we run 9 real-time SSE endpoints for notifications, live analytics, media processing, and curriculum generation. We're built on Go Fiber v3 (fasthttp).

The problem? No existing Go SSE library works with Fiber. They're all built on net/http, and the bridge adapter has a fatal bug — client disconnect detection breaks, causing memory leaks from zombie connections.

So we built our own. And open-sourced it.

What makes fibersse different:

→ Event coalescing: If progress fires 5%, 6%, 7%, 8% in 2 seconds, the client receives only 8%. Server-side intelligence.
→ 3 priority lanes: Critical events (errors, notifications) bypass all buffering. Progress updates get coalesced. You choose per event.
→ Adaptive throttling: Slow clients on mobile automatically get fewer updates. Fast desktop clients get near-real-time.
→ NATS-style topic wildcards: Subscribe to "analytics.*" instead of listing every sub-topic.

~1,500 lines of Go. 29 tests. MIT license. Zero external dependencies.

We've already proposed adding it to Fiber's official contrib packages.

Check it out: github.com/vinod-morya/fibersse

If you're building real-time features with Go Fiber, this saves you from reinventing SSE infrastructure.

#golang #opensource #sse #fiber #realtime #webdev
```

---

## 5. Dev.to Article (Publish at dev.to/new)

**Title**: `Why Every Go SSE Library Breaks on Fiber (and How I Fixed It)`  
**Tags**: `go, opensource, webdev, tutorial`  
**Cover image**: Use the Go gopher + Fiber logo  

*(Full article body — publish as-is)*

```markdown
Every Go project that needs Server-Sent Events on Fiber v3 hand-rolls the same pattern. I built a library so nobody has to do that again.

## The Problem

I run [PersonaCart](https://personacart.com), a creator commerce platform built on Go Fiber v3. We have 9 SSE endpoints — notifications, live analytics, media processing, curriculum generation, and more.

When I searched for a Go SSE library, I found:

- **r3labs/sse** (1,000+ stars) — built on `net/http`
- **tmaxmax/go-sse** (500+ stars) — built on `net/http`

Both have great APIs. Neither works with Fiber.

## Why They Break

Fiber v3 is built on **fasthttp**, not `net/http`. When you bridge SSE libraries to Fiber via `fasthttpadaptor`, there's a fatal bug:

`fasthttp.RequestCtx.Done()` only fires on **server shutdown**, not when an individual client disconnects.

This means:
- SSE library calls `r.Context().Done()` to detect disconnects
- On Fiber, this never fires for individual clients
- Zombie subscribers accumulate forever
- Memory leaks until the server restarts

This is confirmed in [Fiber #3307](https://github.com/gofiber/fiber/issues/3307) and [#4145](https://github.com/gofiber/fiber/issues/4145).

## The Solution: fibersse

I built [fibersse](https://github.com/vinod-morya/fibersse) — native to Fiber v3, using `SendStreamWriter` + `w.Flush()` error detection (the only reliable disconnect signal on fasthttp).

But I didn't stop at "SSE that works on Fiber." I added features that no other SSE library offers:

### Event Coalescing

If a progress bar fires updates at 5%, 6%, 7%, 8% — the client only receives **8%**. Last-writer-wins per key, server-side.

```go
hub.Publish(fibersse.Event{
    Type:        "progress",
    Data:        map[string]int{"pct": 8},
    Priority:    fibersse.PriorityCoalesced,
    CoalesceKey: "task:abc",
    Topics:      []string{"tasks"},
})
```

### Adaptive Throttling

The hub monitors each connection's buffer saturation. Slow clients (mobile on 3G) automatically get longer flush intervals. Fast clients get near-real-time delivery. Zero configuration.

### Topic Wildcards

NATS-style pattern matching. Subscribe to `analytics.*` to match `analytics.live`, `analytics.revenue`, `analytics.funnel`.

## Quick Start

```go
hub := fibersse.New(fibersse.HubConfig{
    FlushInterval: 2 * time.Second,
    OnConnect: func(c fiber.Ctx, conn *fibersse.Connection) error {
        conn.Topics = []string{"notifications", "live"}
        return nil
    },
})
app.Get("/events", hub.Handler())
```

## Install

```bash
go get github.com/vinod-morya/fibersse@v0.1.0
```

## Links

- [GitHub](https://github.com/vinod-morya/fibersse)
- [Go Docs](https://pkg.go.dev/github.com/vinod-morya/fibersse)
- [Fiber contrib proposal](https://github.com/gofiber/contrib/issues/1771)

Feedback welcome — especially on the coalescing and adaptive throttling. These are the features I'm most interested in battle-testing with other teams.
```

---

## 6. Go Weekly Newsletter Submission

**URL**: https://golangweekly.com/  
**Submit via**: Email peter@cooperpress.com or use their suggest form  

**Subject**: `New Go library: fibersse — Production SSE for Fiber v3`  

**Body**:
```
Hi,

I'd love to suggest fibersse for Go Weekly — a production-grade Server-Sent Events library built natively for Fiber v3 (fasthttp).

It fills a real gap: no existing Go SSE library works correctly with Fiber due to fasthttp's disconnect detection limitations. fibersse solves this with features like event coalescing, priority lanes, NATS-style topic wildcards, and adaptive per-connection throttling.

GitHub: https://github.com/vinod-morya/fibersse
Go Docs: https://pkg.go.dev/github.com/vinod-morya/fibersse

Thanks!
Vinod Morya
```

---

## Summary of What's Done vs What Needs Manual Posting

| Platform | Status | Action Needed |
|----------|--------|---------------|
| **Fiber contrib issue** | ✅ Done | gofiber/contrib#1771 |
| **Awesome Go PR** | ✅ Done | avelino/awesome-go#6190 |
| **Go Proxy (pkg.go.dev)** | ✅ Done | Indexed automatically |
| **GitHub Release** | ✅ Done | v0.1.0 published |
| **Reddit r/golang** | 📋 Copy above | Login needed |
| **Hacker News** | 📋 Copy above | Login/account needed |
| **Twitter/X** | 📋 Copy above | Login needed |
| **LinkedIn** | 📋 Copy above | Login needed |
| **Dev.to** | 📋 Copy above | Login needed |
| **Go Weekly** | 📋 Copy above | Email needed |
