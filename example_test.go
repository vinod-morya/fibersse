package fibersse_test

import (
	"context"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/vinod-morya/fibersse"
)

func ExampleNew() {
	hub := fibersse.New(fibersse.HubConfig{
		FlushInterval: 2 * time.Second,
		OnConnect: func(c fiber.Ctx, conn *fibersse.Connection) error {
			conn.Topics = []string{"notifications"}
			return nil
		},
	})
	defer hub.Shutdown(context.TODO())

	app := fiber.New()
	app.Get("/events", hub.Handler())
	fmt.Println("Hub created with SSE handler mounted")
	// Output: Hub created with SSE handler mounted
}

func ExampleHub_Invalidate() {
	hub := fibersse.New()
	defer hub.Shutdown(context.TODO())

	// After creating an order, invalidate the cache
	hub.Invalidate("orders", "ord_123", "created")
	fmt.Println("Invalidation sent")
	// Output: Invalidation sent
}

func ExampleHub_InvalidateForTenant() {
	hub := fibersse.New()
	defer hub.Shutdown(context.TODO())

	// Only tenant t_123's connections receive this
	hub.InvalidateForTenant("t_123", "orders", "ord_456", "created")
	fmt.Println("Tenant invalidation sent")
	// Output: Tenant invalidation sent
}

func ExampleHub_Progress() {
	hub := fibersse.New()
	defer hub.Shutdown(context.TODO())

	// Simulate progress — coalesced so client gets ~10 updates, not 1000
	for i := 1; i <= 1000; i++ {
		hub.Progress("import", "imp_1", "t_123", i, 1000)
	}
	fmt.Println("Progress updates sent")
	// Output: Progress updates sent
}

func ExampleHub_DomainEvent() {
	hub := fibersse.New()
	defer hub.Shutdown(context.TODO())

	// One line from any handler, service, or worker
	hub.DomainEvent("orders", "created", "ord_789", "t_123", map[string]any{
		"total": 99.99,
	})
	fmt.Println("Domain event published")
	// Output: Domain event published
}

func ExampleHub_Complete() {
	hub := fibersse.New()
	defer hub.Shutdown(context.TODO())

	hub.Complete("import", "imp_1", "t_123", true, nil)
	fmt.Println("Completion signal sent")
	// Output: Completion signal sent
}

func ExampleHub_Signal() {
	hub := fibersse.New()
	defer hub.Shutdown(context.TODO())

	hub.Signal("dashboard")
	fmt.Println("Dashboard refresh signal sent")
	// Output: Dashboard refresh signal sent
}

func ExampleNewMemoryReplayer() {
	replayer := fibersse.NewMemoryReplayer(fibersse.MemoryReplayerConfig{
		MaxEvents: 500,
		TTL:       5 * time.Minute,
	})

	hub := fibersse.New(fibersse.HubConfig{
		Replayer: replayer,
		OnConnect: func(c fiber.Ctx, conn *fibersse.Connection) error {
			conn.Topics = []string{"orders"}
			return nil
		},
	})
	defer hub.Shutdown(context.TODO())

	fmt.Println("Hub with replay enabled")
	// Output: Hub with replay enabled
}

func ExampleJWTAuth() {
	hub := fibersse.New(fibersse.HubConfig{
		OnConnect: fibersse.JWTAuth(func(token string) (map[string]string, error) {
			// Validate your JWT here
			if token == "" {
				return nil, fmt.Errorf("empty token")
			}
			return map[string]string{
				"tenant_id": "t_123",
				"user_id":   "u_456",
			}, nil
		}),
	})
	defer hub.Shutdown(context.TODO())

	fmt.Println("Hub with JWT auth")
	// Output: Hub with JWT auth
}

func ExampleTicketAuth() {
	store := fibersse.NewMemoryTicketStore()

	hub := fibersse.New(fibersse.HubConfig{
		OnConnect: fibersse.TicketAuth(store, func(value string) (map[string]string, []string, error) {
			return map[string]string{"user_id": "u_1"}, []string{"notifications"}, nil
		}),
	})
	defer hub.Shutdown(context.TODO())

	// Issue a ticket from your REST API
	ticket, _ := fibersse.IssueTicket(store, "user-data", 30*time.Second)
	fmt.Printf("Ticket issued: %d chars\n", len(ticket))
	// Output: Ticket issued: 48 chars
}

func ExampleHub_BatchDomainEvents() {
	hub := fibersse.New()
	defer hub.Shutdown(context.TODO())

	hub.BatchDomainEvents("t_123", []fibersse.DomainEventSpec{
		{Resource: "orders", Action: "created", ResourceID: "ord_1"},
		{Resource: "inventory", Action: "updated", ResourceID: "sku_1"},
		{Resource: "dashboard", Action: "refresh"},
	})
	fmt.Println("Batch domain events published")
	// Output: Batch domain events published
}
