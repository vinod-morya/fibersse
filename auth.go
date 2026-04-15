package fibersse

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v3"
)

// JWTAuth returns an OnConnect handler that validates a JWT Bearer token
// from the Authorization header or a `token` query parameter.
//
// The validateFunc receives the raw token string and should return the
// claims as a map. Return an error to reject the connection.
//
// Example:
//
//	hub := fibersse.New(fibersse.HubConfig{
//	    OnConnect: fibersse.JWTAuth(func(token string) (map[string]string, error) {
//	        claims, err := validateJWT(token, secret)
//	        if err != nil {
//	            return nil, err
//	        }
//	        return map[string]string{
//	            "tenant_id": claims.TenantID,
//	            "user_id":   claims.UserID,
//	        }, nil
//	    }),
//	})
func JWTAuth(validateFunc func(token string) (map[string]string, error)) func(fiber.Ctx, *Connection) error {
	return func(c fiber.Ctx, conn *Connection) error {
		token := ""

		// Try Authorization header first
		auth := c.Get("Authorization")
		if strings.HasPrefix(auth, "Bearer ") {
			token = strings.TrimPrefix(auth, "Bearer ")
		} else if strings.HasPrefix(auth, "bearer ") {
			token = strings.TrimPrefix(auth, "bearer ")
		}

		// Fallback to query parameter
		if token == "" {
			token = c.Query("token")
		}

		if token == "" {
			return errors.New("missing authentication token")
		}

		claims, err := validateFunc(token)
		if err != nil {
			return fmt.Errorf("authentication failed: %w", err)
		}

		// Set metadata from claims
		for k, v := range claims {
			conn.Metadata[k] = v
		}

		return nil
	}
}

// TicketStore is the interface for ticket-based SSE authentication.
// Implement this with Redis, in-memory, or any key-value store.
type TicketStore interface {
	// Set stores a ticket with the given value and TTL.
	Set(ticket string, value string, ttl time.Duration) error

	// GetDel atomically retrieves and deletes a ticket (one-time use).
	// Returns empty string and nil error if not found.
	GetDel(ticket string) (string, error)
}

// MemoryTicketStore is an in-memory TicketStore for development/testing.
type MemoryTicketStore struct {
	mu      sync.Mutex
	tickets map[string]memTicket
}

type memTicket struct {
	value   string
	expires time.Time
}

// NewMemoryTicketStore creates an in-memory ticket store with a background
// cleanup goroutine that evicts expired tickets every 30 seconds.
func NewMemoryTicketStore() *MemoryTicketStore {
	s := &MemoryTicketStore{
		tickets: make(map[string]memTicket),
	}
	// Background cleanup prevents unbounded memory growth from unconsumed tickets
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			s.mu.Lock()
			now := time.Now()
			for k, v := range s.tickets {
				if now.After(v.expires) {
					delete(s.tickets, k)
				}
			}
			s.mu.Unlock()
		}
	}()
	return s
}

// Set stores a ticket with the given value and TTL.
func (s *MemoryTicketStore) Set(ticket string, value string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tickets[ticket] = memTicket{value: value, expires: time.Now().Add(ttl)}
	return nil
}

// GetDel atomically retrieves and deletes a ticket (one-time use).
// Returns empty string and nil error if not found or expired.
func (s *MemoryTicketStore) GetDel(ticket string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tickets[ticket]
	if !ok {
		return "", nil
	}
	delete(s.tickets, ticket)
	if time.Now().After(t.expires) {
		return "", nil
	}
	return t.value, nil
}

// TicketAuth returns an OnConnect handler that validates a one-time ticket
// from the `ticket` query parameter. Tickets are issued separately (via
// IssueTicket) and consumed on first use.
//
// The parseValue function converts the stored ticket value back into
// connection metadata. It receives the raw value string stored during
// IssueTicket.
//
// Example:
//
//	store := fibersse.NewMemoryTicketStore()
//	hub := fibersse.New(fibersse.HubConfig{
//	    OnConnect: fibersse.TicketAuth(store, func(value string) (map[string]string, []string, error) {
//	        // value is the JSON stored during IssueTicket
//	        var data struct{ TenantID, Topics string }
//	        json.Unmarshal([]byte(value), &data)
//	        return map[string]string{"tenant_id": data.TenantID},
//	               strings.Split(data.Topics, ","), nil
//	    }),
//	})
func TicketAuth(
	store TicketStore,
	parseValue func(value string) (metadata map[string]string, topics []string, err error),
) func(fiber.Ctx, *Connection) error {
	return func(c fiber.Ctx, conn *Connection) error {
		ticket := c.Query("ticket")
		if ticket == "" {
			return errors.New("missing ticket parameter")
		}

		value, err := store.GetDel(ticket)
		if err != nil {
			return fmt.Errorf("ticket validation error: %w", err)
		}
		if value == "" {
			return errors.New("invalid or expired ticket")
		}

		metadata, topics, err := parseValue(value)
		if err != nil {
			return fmt.Errorf("ticket parse error: %w", err)
		}

		for k, v := range metadata {
			conn.Metadata[k] = v
		}
		if len(topics) > 0 {
			conn.Topics = topics
		}

		return nil
	}
}

// IssueTicket creates a one-time ticket and stores it. Returns the
// ticket string that the client should pass as `?ticket=<value>`.
//
// Typical usage: expose a POST endpoint that issues a ticket after
// validating the user's JWT, then the client opens EventSource with
// the ticket.
func IssueTicket(store TicketStore, value string, ttl time.Duration) (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate ticket: %w", err)
	}
	ticket := hex.EncodeToString(b)
	if err := store.Set(ticket, value, ttl); err != nil {
		return "", err
	}
	return ticket, nil
}
