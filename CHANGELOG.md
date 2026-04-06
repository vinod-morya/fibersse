# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-04-06

### Highlights

FiberSSE reaches stable release. Production-tested at [PersonaCart](https://personacart.com) across 7 real-time publishers, 300+ concurrent connections, multi-tenant SaaS with full tenant isolation.

- **Go Report Card: A+** (all 7 checks pass, zero issues)
- **Test Coverage: 88.6%** (77 tests + 42 benchmarks)
- **CI/CD: GitHub Actions** (Go 1.24 + 1.25 matrix, race detector, staticcheck)
- **Listed on [awesome-go](https://github.com/avelino/awesome-go)**
- **React SDK: [fibersse-react](https://www.npmjs.com/package/fibersse-react)** on npm

### Added
- 3 runnable examples: `basic/`, `chat/`, `polling-replacement/`
- 12 godoc `Example*` functions for pkg.go.dev
- `CONTRIBUTING.md` with development setup guide
- `CHANGELOG.md` (this file)
- `CLAUDE.md` — AI agent integration instructions
- CI pipeline with codecov integration
- 18-row feature comparison table vs Fiber's SSE recipe in README

### Changed
- Refactored `Handler()` (cyclomatic complexity 23 → 5 sub-functions)
- Refactored `routeEvent()` (complexity 22 → 3 sub-functions)
- Refactored `FanOut()` (complexity 19 → 2 sub-functions)
- All functions now pass `gocyclo -over 15` check

### Fixed
- Zero `gofmt -s` issues (hub.go, metrics.go reformatted)
- Zero `golint` warnings (3 missing godoc comments added)
- Zero `misspell` findings
- Zero `ineffassign` findings

## [0.5.0] - 2026-04-03

### Added
- 11 integration tests with real Fiber HTTP server + SSE client
- 42 micro-benchmarks covering all hot paths
- Blog post: "How We Eliminated 90% of API Calls"

### Benchmark Results (Apple M4 Max)
- Publish to 1 connection: **477ns**
- Publish to 1,000 connections: **82μs**
- Topic match (exact): **8ns, 0 allocs**
- Connection send: **14ns, 0 allocs**
- Backpressure drop: **2ns, 0 allocs**
- Event coalescing (same key): **21ns, 0 allocs**

## [0.4.0] - 2026-04-03

### Added
- `InvalidateForTenantWithHint()` — tenant-scoped invalidation with data hints
- `BatchDomainEvents()` — publish multiple resource changes as single SSE frame
- `Progress()` now accepts optional hint map
- `OnPause` / `OnResume` lifecycle callbacks on `HubConfig`
- Per-event-type breakdown in `Stats()` and `Metrics()`
- `fibersse_events_by_type_total{type="..."}` Prometheus metric
- TanStack Query + SWR integration section in README

## [0.3.0] - 2026-04-03

### Added — "Kill Polling" Toolkit
- `Invalidate()`, `InvalidateForTenant()`, `InvalidateWithHint()` — cache invalidation signals
- `DomainEvent()` — one-line structured events from any handler/worker
- `Progress()` — coalesced progress tracking (5%→8% sends only 8%)
- `Complete()` — instant completion signals
- `Signal()`, `SignalForTenant()`, `SignalThrottled()` — generic refresh signals
- `CLAUDE.md` — instructions for AI coding agents

## [0.2.0] - 2026-04-03

### Fixed — Critical Production Issues
- **Memory leak**: `MemoryTicketStore` — unconsumed tickets never evicted. Added background cleanup goroutine (30s sweep)
- **Memory leak**: `AdaptiveThrottler.lastFlush` — stale entries for disconnected connections. Added periodic cleanup (5 min)
- **Race condition**: Shutdown watcher — `trySend()` on already-closed connection. Added `IsClosed()` check
- **Lock contention**: `flushAll` — held `RLock` for entire iteration. Changed to snapshot-and-release pattern
- **Replay bug**: Wildcard topic matching ignored in `Last-Event-ID` replay. Added wildcard support
- Removed unused `atomicMax` function
- `formatFloat` now handles NaN/Inf values

## [0.1.0] - 2026-04-03

### Added — Initial Release
- Hub pattern with single-goroutine event loop
- 3 priority lanes: Instant (P0), Batched (P1), Coalesced (P2)
- Event coalescing: last-writer-wins per key
- NATS-style topic wildcards (`*` and `>` patterns)
- Adaptive throttling: AIMD-based per-connection flush interval
- Connection groups: publish by metadata
- Client visibility hints: pause P1/P2 for hidden tabs
- Built-in auth: JWT validation + one-time ticket helpers
- Auto fan-out: bridge Redis/NATS pub/sub to SSE
- Prometheus + JSON metrics endpoints
- Last-Event-ID replay with pluggable Replayer interface
- Event TTL: drop stale events automatically
- Graceful drain: Kubernetes-style shutdown with Retry-After
- Backpressure: bounded buffers, drop slow clients
- 29 tests, MIT license

[1.0.0]: https://github.com/vinod-morya/fibersse/compare/v0.5.0...v1.0.0
[0.5.0]: https://github.com/vinod-morya/fibersse/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/vinod-morya/fibersse/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/vinod-morya/fibersse/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/vinod-morya/fibersse/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/vinod-morya/fibersse/releases/tag/v0.1.0
