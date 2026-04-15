// Basic example: one hub, one topic, periodic publisher, browser client.
//
// Run:
//
//	go run main.go
//
// Then open http://localhost:3000 in your browser.
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/vinod-morya/fibersse"
)

func main() {
	hub := fibersse.New(fibersse.HubConfig{
		FlushInterval: 1 * time.Second,
		OnConnect: func(c fiber.Ctx, conn *fibersse.Connection) error {
			conn.Topics = []string{"notifications"}
			log.Printf("Client connected: %s", conn.ID)
			return nil
		},
		OnDisconnect: func(conn *fibersse.Connection) {
			log.Printf("Client disconnected: %s", conn.ID)
		},
	})

	app := fiber.New()

	// Serve the SSE endpoint
	app.Get("/events", hub.Handler())

	// Serve a simple HTML page
	app.Get("/", func(c fiber.Ctx) error {
		c.Set("Content-Type", "text/html")
		return c.SendString(indexHTML)
	})

	// Background publisher — sends a notification every 3 seconds
	go func() {
		i := 0
		for {
			time.Sleep(3 * time.Second)
			i++
			hub.Publish(fibersse.Event{
				Type:     "notification",
				Data:     fmt.Sprintf(`{"message":"Event #%d","time":"%s"}`, i, time.Now().Format(time.RFC3339)),
				Topics:   []string{"notifications"},
				Priority: fibersse.PriorityInstant,
			})
			log.Printf("Published event #%d", i)
		}
	}()

	log.Println("Server starting on http://localhost:3000")
	log.Fatal(app.Listen(":3000"))
}

const indexHTML = `<!DOCTYPE html>
<html>
<head>
  <title>fibersse — Basic Example</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 600px; margin: 40px auto; padding: 0 20px; }
    h1 { color: #333; }
    #status { padding: 8px 16px; border-radius: 4px; display: inline-block; margin-bottom: 16px; }
    .connected { background: #d4edda; color: #155724; }
    .disconnected { background: #f8d7da; color: #721c24; }
    #events { list-style: none; padding: 0; }
    #events li { padding: 12px; margin: 8px 0; background: #f8f9fa; border-radius: 8px; border-left: 4px solid #007bff; }
    .time { color: #666; font-size: 0.85em; }
  </style>
</head>
<body>
  <h1>fibersse — Basic Example</h1>
  <div id="status" class="disconnected">Connecting...</div>
  <ul id="events"></ul>

  <script>
    const status = document.getElementById('status');
    const events = document.getElementById('events');
    const es = new EventSource('/events');

    es.addEventListener('connected', (e) => {
      const data = JSON.parse(e.data);
      status.textContent = 'Connected: ' + data.connection_id.slice(0, 8) + '...';
      status.className = 'connected';
    });

    es.addEventListener('notification', (e) => {
      const data = JSON.parse(e.data);
      const li = document.createElement('li');
      li.innerHTML = '<strong>' + data.message + '</strong><br><span class="time">' + data.time + '</span>';
      events.prepend(li);
    });

    es.onerror = () => {
      status.textContent = 'Disconnected — reconnecting...';
      status.className = 'disconnected';
    };
  </script>
</body>
</html>`
