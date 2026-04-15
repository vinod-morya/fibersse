# Polling Replacement Example

The killer example: shows BEFORE (polling with setInterval) and AFTER (SSE invalidation) side by side.

## Run

```bash
go run main.go
```

Open two browser tabs:
- http://localhost:3000/polling — traditional 5-second polling
- http://localhost:3000/sse — SSE invalidation (zero polling)

Click "Create Order" on either page and watch:
- **Polling page**: API calls increment every 5 seconds whether or not data changed
- **SSE page**: API calls only increment when data actually changes

## What it demonstrates

- `hub.Invalidate()` as the primary polling-replacement API
- Same UI, same data, dramatically different network usage
- API call counters prove the difference
- The SSE page is also faster — updates appear instantly instead of up to 5s later
