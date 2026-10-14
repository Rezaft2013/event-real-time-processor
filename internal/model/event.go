package model

import (
	"uuid"
	//"time"
)

type Event struct {
	ID        uuid.UUID `json:"id"`
	Type      string    `json:"type"`
	TenantID  string    `json:"tenant_id"`
	Version   string    `json:"version"`
	Timestamp int64     `json:"timestamp"`
	Payload   string    `json:"payload"`
}
