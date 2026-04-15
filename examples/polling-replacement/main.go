// Polling replacement example: shows the BEFORE (polling) and AFTER (SSE) patterns.
//
// Run:
//
//	go run main.go
//
// Then open:
//   - http://localhost:3000/polling — traditional 5-second polling
//   - http://localhost:3000/sse     — SSE invalidation (zero polling)
//
// Click "Create Order" and watch the difference in API call counts.
package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/vinod-morya/fibersse"
)

// In-memory "database"
var (
	orders   []Order
	ordersMu sync.RWMutex
	orderSeq atomic.Int64

	// Counters to show the difference
	pollingAPICalls atomic.Int64
	sseAPICalls     atomic.Int64
)

// Order is a simple order model.
type Order struct {
	ID        string `json:"id"`
	Item      string `json:"item"`
	Total     int    `json:"total"`
	CreatedAt string `json:"created_at"`
}

func main() {
	hub := fibersse.New(fibersse.HubConfig{
		FlushInterval: 1 * time.Second,
		OnConnect: func(_ fiber.Ctx, conn *fibersse.Connection) error {
			conn.Topics = []string{"orders"}
			return nil
		},
	})

	app := fiber.New()

	// SSE endpoint
	app.Get("/events", hub.Handler())

	// API: list orders (used by both polling and SSE clients)
	app.Get("/api/orders", func(c fiber.Ctx) error {
		source := c.Query("source", "unknown")
		if source == "polling" {
			pollingAPICalls.Add(1)
		} else if source == "sse" {
			sseAPICalls.Add(1)
		}

		ordersMu.RLock()
		result := make([]Order, len(orders))
		copy(result, orders)
		ordersMu.RUnlock()

		return c.JSON(result)
	})

	// API: create order (triggers SSE invalidation)
	app.Post("/api/orders", func(c fiber.Ctx) error {
		id := fmt.Sprintf("ord_%d", orderSeq.Add(1))
		order := Order{
			ID:        id,
			Item:      fmt.Sprintf("Item #%d", orderSeq.Load()),
			Total:     100 + int(orderSeq.Load())*10,
			CreatedAt: time.Now().Format(time.RFC3339),
		}

		ordersMu.Lock()
		orders = append(orders, order)
		ordersMu.Unlock()

		// This one line replaces all polling
		hub.Invalidate("orders", id, "created")

		return c.JSON(order)
	})

	// API: get call counts
	app.Get("/api/stats", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"polling_api_calls": pollingAPICalls.Load(),
			"sse_api_calls":     sseAPICalls.Load(),
		})
	})

	// Serve HTML pages
	app.Get("/polling", func(c fiber.Ctx) error {
		c.Set("Content-Type", "text/html")
		return c.SendString(pollingHTML)
	})
	app.Get("/sse", func(c fiber.Ctx) error {
		c.Set("Content-Type", "text/html")
		return c.SendString(sseHTML)
	})
	app.Get("/", func(c fiber.Ctx) error {
		c.Set("Content-Type", "text/html")
		return c.SendString(indexHTML)
	})

	log.Println("Server starting on http://localhost:3000")
	log.Fatal(app.Listen(":3000"))
}

const indexHTML = `<!DOCTYPE html>
<html>
<head><title>fibersse — Polling vs SSE</title>
<style>body{font-family:system-ui,sans-serif;max-width:600px;margin:40px auto;padding:0 20px;} a{display:block;padding:16px;margin:12px 0;border-radius:8px;text-decoration:none;font-size:1.2em;text-align:center;} .polling{background:#f8d7da;color:#721c24;} .sse{background:#d4edda;color:#155724;}</style>
</head>
<body>
<h1>Polling vs SSE</h1>
<p>Open both pages side by side. Create orders and watch the difference in API calls.</p>
<a class="polling" href="/polling">Polling (setInterval every 5s)</a>
<a class="sse" href="/sse">SSE (instant, zero polling)</a>
</body></html>`

const pollingHTML = `<!DOCTYPE html>
<html>
<head><title>BEFORE: Polling</title>
<style>
body{font-family:system-ui,sans-serif;max-width:600px;margin:40px auto;padding:0 20px;}
h1{color:#721c24;} .badge{background:#f8d7da;color:#721c24;padding:4px 12px;border-radius:12px;font-size:0.85em;}
button{padding:10px 20px;background:#dc3545;color:white;border:none;border-radius:6px;cursor:pointer;font-size:1em;margin:8px 0;}
#orders{list-style:none;padding:0;} #orders li{padding:10px;margin:4px 0;background:#f8f9fa;border-radius:6px;}
.stats{background:#fff3cd;padding:12px;border-radius:8px;margin:12px 0;}
</style></head>
<body>
<h1>BEFORE: Polling <span class="badge">setInterval(5s)</span></h1>
<p>This page polls <code>/api/orders</code> every 5 seconds, whether or not data changed.</p>
<button onclick="createOrder()">Create Order</button>
<div class="stats">API calls made: <strong id="calls">0</strong></div>
<ul id="orders"></ul>
<script>
let callCount = 0;
function fetchOrders() {
  callCount++;
  document.getElementById('calls').textContent = callCount;
  fetch('/api/orders?source=polling').then(r=>r.json()).then(orders=>{
    const ul = document.getElementById('orders');
    ul.innerHTML = orders.map(o=>'<li><strong>'+o.id+'</strong> — '+o.item+' ($'+o.total+')</li>').join('');
  });
}
function createOrder() {
  fetch('/api/orders',{method:'POST'});
}
setInterval(fetchOrders, 5000);
fetchOrders();
</script></body></html>`

const sseHTML = `<!DOCTYPE html>
<html>
<head><title>AFTER: SSE</title>
<style>
body{font-family:system-ui,sans-serif;max-width:600px;margin:40px auto;padding:0 20px;}
h1{color:#155724;} .badge{background:#d4edda;color:#155724;padding:4px 12px;border-radius:12px;font-size:0.85em;}
button{padding:10px 20px;background:#28a745;color:white;border:none;border-radius:6px;cursor:pointer;font-size:1em;margin:8px 0;}
#orders{list-style:none;padding:0;} #orders li{padding:10px;margin:4px 0;background:#f8f9fa;border-radius:6px;}
.stats{background:#d4edda;padding:12px;border-radius:8px;margin:12px 0;}
</style></head>
<body>
<h1>AFTER: SSE Invalidation <span class="badge">zero polling</span></h1>
<p>This page only fetches <code>/api/orders</code> when the server says data changed.</p>
<button onclick="createOrder()">Create Order</button>
<div class="stats">API calls made: <strong id="calls">0</strong></div>
<ul id="orders"></ul>
<script>
let callCount = 0;
function fetchOrders() {
  callCount++;
  document.getElementById('calls').textContent = callCount;
  fetch('/api/orders?source=sse').then(r=>r.json()).then(orders=>{
    const ul = document.getElementById('orders');
    ul.innerHTML = orders.map(o=>'<li><strong>'+o.id+'</strong> — '+o.item+' ($'+o.total+')</li>').join('');
  });
}
function createOrder() {
  fetch('/api/orders',{method:'POST'});
}

// Initial fetch
fetchOrders();

// SSE: only refetch when server says data changed
const es = new EventSource('/events');
es.addEventListener('invalidate', (e) => {
  const data = JSON.parse(e.data);
  if (data.resource === 'orders') {
    fetchOrders(); // refetch only when data actually changed
  }
});
</script></body></html>`
