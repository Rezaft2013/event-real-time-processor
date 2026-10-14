package schema_validation

import (
	"errors"
	"github.com/xeipuuv/gojsonschema"
)

const model_1_0_schema = `{
    "type": "object",
	"properties": {
		"name": {"type": "string", "minLength": 1},
		"email": {"type": "string", "format": "email"},
		"age": {"type": "integer", "minimum": 0}
	},
	"required": ["name", "email"]
}`

const model_1_1_schema = `{
    "type": "object",
	"properties": {
		"name": {"type": "string", "minLength": 1},
		"email": {"type": "string", "format": "email"},
		"age": {"type": "integer", "minimum": 0},
		"phone" : { {"type": "string"},
	},
	"required": ["name", "email", "phone"]
}`

type schemaValidator struct {
	validSchemaVersions map[string]string
}

func NewSchemaValidator() *schemaValidator {
	return &schemaValidator{validSchemaVersions: map[string]string{"1.0": model_1_0_schema, "1.1": model_1_1_schema}}
}

func (validator schemaValidator) Validate(modelVersion string, jsonValue string) error {

	schema, found := validator.validSchemaVersions[modelVersion]

	if !found {
		return errors.New("invalid model")
	}

	if modelVersion == "1.0" {
		return validator.model_validator(jsonValue, schema)
	}

	if modelVersion == "1.1" {
		return validator.model_validator(jsonValue, schema)
	}

	return errors.New("invalid model")
}

func (validator schemaValidator) model_validator(jsonValue string, schema string) error {
	schemaLoader := gojsonschema.NewStringLoader(schema)
	documentLoader := gojsonschema.NewStringLoader(jsonValue)

	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return err
	}

	if result.Valid() {
		return nil
	}

	errs := "schema validation errors:"
	for _, desc := range result.Errors() {
		errs += (desc.Description() + ",")
	}

	return errors.New(errs)
}
