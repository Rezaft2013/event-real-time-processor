package model

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestModel_Validate(t *testing.T) {
	evt := Event{
		ID:        uuid.New(),
		Type:      "test",
		TenantID:  "1234",
		Timestamp: 123234566,
		Payload:   "{}",
		Version:   "",
	}

	err := evt.Validate()
	assert.NotNil(t, err)

	evt.Version = "1.0"
	err = evt.Validate()
	assert.Nil(t, err)
}
