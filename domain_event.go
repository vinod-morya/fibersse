package fibersse

// ──────────────────────────────────────────────────────────────────────────────
// Domain Event Publisher
//
// One-line helpers for publishing domain events from your handlers, services,
// and background workers. These are the building blocks for replacing polling.
//
// Pattern: When your backend mutates data (create/update/delete), publish
// a domain event. The SSE hub delivers it to subscribed clients, who refetch
// only the specific data that changed.
//
//	// In your order handler:
//	func (h *OrderHandler) Create(c fiber.Ctx) error {
//	    order, err := h.svc.CreateOrder(...)
//	    if err != nil { return err }
//
//	    // One line — replaces 30s polling for ALL connected clients
//	    h.sse.DomainEvent("orders", "created", order.ID, order.TenantID, map[string]any{
//	        "total": order.Total,
//	    })
//
//	    return c.JSON(order)
//	}
//
// ──────────────────────────────────────────────────────────────────────────────

// DomainEvent publishes a domain event to the hub. This is the primary
// method for triggering real-time UI updates from your backend code.
//
// Parameters:
//   - resource: what changed ("orders", "products", "customers")
//   - action: what happened ("created", "updated", "deleted", "refresh")
//   - resourceID: specific item ID (empty for collection-level events)
//   - tenantID: tenant scope (empty for global events)
//   - hint: optional small payload (nil if not needed)
//
// Example:
//
//	// Order created — notifies all users in the tenant
//	hub.DomainEvent("orders", "created", "ord_123", "t_456", map[string]any{
//	    "total": 99.99, "customer": "John",
//	})
//
//	// Product stock changed — no hint needed, client refetches
//	hub.DomainEvent("products", "updated", "prod_789", "t_456", nil)
//
//	// Bulk import done — collection-level refresh
//	hub.DomainEvent("products", "refresh", "", "t_456", nil)
func (h *Hub) DomainEvent(resource, action, resourceID, tenantID string, hint map[string]any) {
	evt := InvalidationEvent{
		Resource:   resource,
		Action:     action,
		ResourceID: resourceID,
		Hint:       hint,
	}

	event := Event{
		Type:        "invalidate",
		Topics:      []string{resource},
		Data:        evt,
		Priority:    PriorityInstant,
		CoalesceKey: "invalidate:" + resource + ":" + resourceID,
	}

	if tenantID != "" {
		event.Group = map[string]string{"tenant_id": tenantID}
	}

	h.Publish(event)
}

// Progress publishes a progress update for a long-running operation.
// Uses PriorityCoalesced — if progress goes 5%→6%→7%→8% in one flush
// window, only 8% is sent to the client.
//
// Example:
//
//	// In your import worker:
//	for i, row := range rows {
//	    processRow(row)
//	    hub.Progress("import", importID, tenantID, i+1, len(rows))
//	}
//	// Client receives: 0%...25%...50%...75%...100% (not every single row)
func (h *Hub) Progress(topic, resourceID, tenantID string, current, total int) {
	pct := 0
	if total > 0 {
		pct = (current * 100) / total
	}

	event := Event{
		Type:   "progress",
		Topics: []string{topic},
		Data: map[string]any{
			"resource_id": resourceID,
			"current":     current,
			"total":       total,
			"pct":         pct,
		},
		Priority:    PriorityCoalesced,
		CoalesceKey: "progress:" + topic + ":" + resourceID,
	}

	if tenantID != "" {
		event.Group = map[string]string{"tenant_id": tenantID}
	}

	h.Publish(event)
}

// Complete publishes a completion signal for a long-running operation.
// Uses PriorityInstant — completion always delivers immediately.
//
// Example:
//
//	hub.Complete("import", importID, tenantID, true, nil)  // success
//	hub.Complete("import", importID, tenantID, false, map[string]any{
//	    "error": "CSV parse failed at row 42",
//	})
func (h *Hub) Complete(topic, resourceID, tenantID string, success bool, hint map[string]any) {
	action := "completed"
	if !success {
		action = "failed"
	}

	data := map[string]any{
		"resource_id": resourceID,
		"status":      action,
	}
	for k, v := range hint {
		data[k] = v
	}

	event := Event{
		Type:     "complete",
		Topics:   []string{topic},
		Data:     data,
		Priority: PriorityInstant,
	}

	if tenantID != "" {
		event.Group = map[string]string{"tenant_id": tenantID}
	}

	h.Publish(event)
}
