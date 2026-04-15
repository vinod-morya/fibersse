# Contributing to fibersse

Thank you for your interest in contributing to fibersse! This document covers the development workflow.

## Prerequisites

- Go 1.24+
- Familiarity with [Fiber v3](https://github.com/gofiber/fiber)

## Development Setup

```bash
git clone https://github.com/vinod-morya/fibersse.git
cd fibersse
go mod download
```

## Running Tests

```bash
# Run all tests with race detector
go test -race ./...

# Run with coverage
go test -coverprofile=coverage.out ./...
go tool cover -func=coverage.out | grep total

# Generate HTML coverage report
go tool cover -html=coverage.out -o coverage.html

# Run benchmarks
go test -bench=. -benchmem ./...
```

## Code Style

All code must pass these checks with zero issues:

```bash
gofmt -l .          # must output nothing
go vet ./...        # must pass
staticcheck ./...   # must pass
```

- Use `gofmt` for formatting (no exceptions)
- All exported types and functions must have godoc comments
- Keep cyclomatic complexity under 15 per function

## Pull Request Process

1. Fork the repository
2. Create a feature branch from `main`
3. Write tests for new functionality (target 80%+ coverage)
4. Ensure all lint checks pass
5. Submit a PR with a clear description

### PR Checklist

- [ ] `go test -race ./...` passes
- [ ] `gofmt -l .` outputs nothing
- [ ] `go vet ./...` passes
- [ ] `staticcheck ./...` passes
- [ ] New public APIs have godoc comments
- [ ] Tests cover the new code path
- [ ] Benchmarks added for performance-sensitive code

## Architecture

```
fibersse/
├── hub.go             Core hub: New(), Handler(), Publish(), Shutdown()
├── event.go           Event struct, MarshaledEvent, SSE wire format
├── connection.go      Per-client connection, write loop, backpressure
├── coalescer.go       P1 batch + P2 last-writer-wins buffers
├── invalidation.go    Invalidate(), Signal() — polling replacement helpers
├── domain_event.go    DomainEvent(), Progress(), Complete(), BatchDomainEvents()
├── topic.go           NATS-style wildcard matching (*, >)
├── throttle.go        Adaptive per-connection flush interval
├── auth.go            JWTAuth(), TicketAuth(), TicketStore
├── fanout.go          PubSubSubscriber interface, FanOut(), FanOutMulti()
├── replayer.go        Last-Event-ID replay (MemoryReplayer)
├── metrics.go         MetricsHandler(), PrometheusHandler()
└── stats.go           HubStats, hubMetrics
```

## Reporting Issues

Please open an issue on GitHub with:
- Go version (`go version`)
- Fiber version
- Minimal reproduction code
- Expected vs actual behavior
