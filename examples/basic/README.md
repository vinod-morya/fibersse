# Basic Example

Minimal fibersse setup: one hub, one topic, one publisher, one browser client.

## Run

```bash
go run main.go
```

Open http://localhost:3000 in your browser. You'll see events arriving every 3 seconds.

## What it demonstrates

- Creating a hub with `fibersse.New()`
- Mounting the SSE handler with `hub.Handler()`
- Publishing events from a background goroutine
- Browser EventSource connecting and receiving typed events
- Connection/disconnection lifecycle callbacks
