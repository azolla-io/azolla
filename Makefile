.PHONY: help dev-up dev-down dev-logs build run test clean

# Default target
help:
	@echo "Available commands:"
	@echo "  dev-up     - Start PostgreSQL database for local development"
	@echo "  dev-down   - Stop PostgreSQL database"
	@echo "  dev-logs   - Show database logs"
	@echo "  build      - Build the application"
	@echo "  run        - Run the application locally (requires dev-up)"
	@echo "  test       - Run tests"
	@echo "  clean      - Clean build artifacts"
	@echo "  docker-up  - Start full stack (database + azolla in containers)"
	@echo "  docker-down- Stop full stack"

# Development database commands
dev-up:
	docker-compose -f docker-compose-dev.yml up -d

dev-down:
	docker-compose -f docker-compose-dev.yml down

# Application commands
build:
	cargo build

dev-run: dev-up
	RUST_LOG=info cargo run

test:
	cargo test

clean:
	cargo clean