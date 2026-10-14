BINARY_NAME := event-real-time-processor

.PHONY: build run run-db-upgrade clean test fmt vet

build:
	@echo "Building..."
	go mod tidy
	go build -o $(BINARY_NAME) ./cmd/server/
	@echo "Build complete."