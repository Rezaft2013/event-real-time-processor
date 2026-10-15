BINARY_NAME := event-real-time-processor

.PHONY: build run run-db-upgrade clean test fmt vet

build:
	@echo "Building..."
	go mod tidy
	GOTOOLCHAIN=go1.27.0 go build -o $(BINARY_NAME) ./cmd/server/
	@echo "Build complete."

test:
	GOTOOLCHAIN=go1.27.0 go test -v ./...

# Run only tests labeled with the 'integration' tag
integration-test:
	GOTOOLCHAIN=go1.27.0 go test -tags=integration -v ./...