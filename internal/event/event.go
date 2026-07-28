// Package event defines the Event record persisted by the track-api and
// passed to downstream consumers. Single source of truth for the JSON
// shape on POST /events and the columns in the events table.
package event

import (
	"encoding/json"
	"time"
)

// Event is what one Track API call records.
type Event struct {
	WorkspaceID string          `json:"workspace_id"`
	EventID     string          `json:"event_id"` // UUIDv4, server-assigned
	PersonID    string          `json:"person_id"`
	Name        string          `json:"event_name"`
	Payload     json.RawMessage `json:"payload"`
	ReceivedAt  time.Time       `json:"received_at"`
}
