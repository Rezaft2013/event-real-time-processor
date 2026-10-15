package model

import (
	"errors"
	"github.com/google/uuid"
)

type Event struct {
	ID        uuid.UUID `json:"id"`
	Type      string    `json:"type"`
	TenantID  string    `json:"tenant_id"`
	Version   string    `json:"version"`
	Timestamp int64     `json:"timestamp"`
	Payload   string    `json:"payload"`
}

func (evt Event) Validate() error {
	if uuid.Validate(evt.ID.String()) != nil {
		return errors.New("invalid event id")
	}

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
