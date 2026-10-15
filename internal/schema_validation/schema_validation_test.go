package schema_validation

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestModel1_0_Schema(t *testing.T) {
	validator := NewSchemaValidator()
	jsonDoc := ` { "name" : "jason", "email" : "rtest@test.com" }`
	err := validator.Validate("1.0", jsonDoc)
	assert.Nil(t, err)

	jsonDoc = ` { "name-a" : "jason", "email" : "rtest@test.com" }`
	err = validator.Validate("1.0", jsonDoc)
	assert.NotNil(t, err)
}
