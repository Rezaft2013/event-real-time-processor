package model

import (
	"errors"
	"github.com/google/uuid"
)

// Event model fields definition
type Event struct {
	ID        uuid.UUID
	Type      string `json:"type"`
	TenantID  string `json:"tenant_id"`
	Version   string `json:"version"`
	Timestamp int64  `json:"timestamp"`
	Payload   string `json:"payload"`
}

// Validate: validates required fields, if validation fails system returns error and does not ingest the event
func (evt Event) Validate() error {
	if evt.Type == "" {
		return errors.New("invalid event type")
	}

	if evt.TenantID == "" {
		return errors.New("invalid event tenant id")
	}

	if evt.Version == "" {
		return errors.New("invalid event version")
	}

	if evt.Timestamp == 0 {
		return errors.New("invalid event timestamp")
	}

	if evt.Payload == "" {
		return errors.New("invalid event payload")
	}

	return nil
}
