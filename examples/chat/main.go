// Chat example: multi-room chat using topic wildcards and connection groups.
//
// Run:
//
//	go run main.go
//
// Then open http://localhost:3000 in your browser.
// Open multiple tabs to simulate different users in different rooms.
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
		FlushInterval: 500 * time.Millisecond,
		OnConnect: func(c fiber.Ctx, conn *fibersse.Connection) error {
			room := c.Query("room", "general")
			user := c.Query("user", "anonymous")

			// Subscribe to room-specific topic + global announcements
			conn.Topics = []string{
				"chat." + room,  // room-specific messages
				"chat.announce", // global announcements
			}
			conn.Metadata["room"] = room
			conn.Metadata["user"] = user

			log.Printf("[%s] %s joined room %s", conn.ID[:8], user, room)
			return nil
		},
		OnDisconnect: func(conn *fibersse.Connection) {
			log.Printf("[%s] %s left room %s", conn.ID[:8], conn.Metadata["user"], conn.Metadata["room"])
		},
	})

	app := fiber.New()

	// SSE endpoint
	app.Get("/events", hub.Handler())

	// Send a message to a room
	app.Post("/send", func(c fiber.Ctx) error {
		room := c.FormValue("room", "general")
		user := c.FormValue("user", "anonymous")
		message := c.FormValue("message", "")

		if message == "" {
			return c.Status(400).SendString("message required")
		}

		hub.Publish(fibersse.Event{
			Type: "chat-message",
			Data: fmt.Sprintf(`{"room":"%s","user":"%s","message":"%s","time":"%s"}`,
				room, user, message, time.Now().Format("15:04:05")),
			Topics:   []string{"chat." + room},
			Priority: fibersse.PriorityInstant,
		})

		return c.SendString("ok")
	})

	// Stats endpoint
	app.Get("/stats", func(c fiber.Ctx) error {
		return c.JSON(hub.Stats())
	})

	// Serve HTML
	app.Get("/", func(c fiber.Ctx) error {
		c.Set("Content-Type", "text/html")
		return c.SendString(indexHTML)
	})

	log.Println("Chat server starting on http://localhost:3000")
	log.Fatal(app.Listen(":3000"))
}

const indexHTML = `<!DOCTYPE html>
<html>
<head>
  <title>fibersse — Chat Example</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 700px; margin: 40px auto; padding: 0 20px; }
    h1 { color: #333; }
    .controls { display: flex; gap: 8px; margin-bottom: 16px; }
    .controls input, .controls select, .controls button { padding: 8px 12px; border-radius: 6px; border: 1px solid #ccc; }
    .controls button { background: #007bff; color: white; border: none; cursor: pointer; }
    #status { padding: 6px 12px; border-radius: 4px; font-size: 0.85em; margin-bottom: 12px; display: inline-block; }
    .connected { background: #d4edda; color: #155724; }
    .disconnected { background: #f8d7da; color: #721c24; }
    #messages { list-style: none; padding: 0; max-height: 400px; overflow-y: auto; }
    #messages li { padding: 8px 12px; margin: 4px 0; border-radius: 6px; background: #f0f0f0; }
    #messages li.announce { background: #fff3cd; border-left: 4px solid #ffc107; }
    .msg-user { font-weight: bold; color: #007bff; }
    .msg-time { color: #999; font-size: 0.8em; float: right; }
  </style>
</head>
<body>
  <h1>fibersse — Chat Example</h1>

  <div class="controls">
    <input id="user" type="text" placeholder="Your name" value="User1" />
    <select id="room">
      <option value="general">General</option>
      <option value="dev">Dev</option>
      <option value="random">Random</option>
    </select>
    <button onclick="connect()">Join</button>
  </div>

  <div id="status" class="disconnected">Not connected</div>

  <div class="controls">
    <input id="message" type="text" placeholder="Type a message..." style="flex:1" onkeydown="if(event.key==='Enter')send()" />
    <button onclick="send()">Send</button>
  </div>

  <ul id="messages"></ul>

  <script>
    let es = null;
    const statusEl = document.getElementById('status');
    const messagesEl = document.getElementById('messages');

    function connect() {
      if (es) es.close();

      const user = document.getElementById('user').value || 'anonymous';
      const room = document.getElementById('room').value;

      es = new EventSource('/events?room=' + room + '&user=' + user);

      es.addEventListener('connected', (e) => {
        statusEl.textContent = 'Connected to #' + room + ' as ' + user;
        statusEl.className = 'connected';
      });

      es.addEventListener('chat-message', (e) => {
        const data = JSON.parse(e.data);
        addMessage(data.user, data.message, data.time, false);
      });

      es.onerror = () => {
        statusEl.textContent = 'Disconnected — reconnecting...';
        statusEl.className = 'disconnected';
      };
    }

    function send() {
      const msg = document.getElementById('message').value;
      if (!msg) return;

      const user = document.getElementById('user').value || 'anonymous';
      const room = document.getElementById('room').value;

      fetch('/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: 'room=' + room + '&user=' + user + '&message=' + encodeURIComponent(msg)
      });

      document.getElementById('message').value = '';
    }

    function addMessage(user, text, time, isAnnounce) {
      const li = document.createElement('li');
      if (isAnnounce) li.className = 'announce';
      li.innerHTML = '<span class="msg-time">' + time + '</span><span class="msg-user">' + user + ':</span> ' + text;
      messagesEl.prepend(li);
    }

    // Auto-connect on load
    connect();
  </script>
</body>
</html>`
