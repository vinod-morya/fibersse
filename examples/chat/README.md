# Chat Example

Multi-room chat using topic wildcards and connection metadata.

## Run

```bash
go run main.go
```

Open http://localhost:3000 in multiple browser tabs. Pick different rooms and usernames.

## What it demonstrates

- Topic wildcards: `chat.*` pattern matching
- Connection metadata: room and user stored per-connection
- POST endpoint publishes events through the hub
- Multiple rooms with isolated message streams
- Real-time message delivery via SSE
