.PHONY: help dev-up dev-down dev-clean build test clean

# Default target
help:
	@echo "Available commands:"
	@echo "  dev-up     - Start PostgreSQL database for local development"
	@echo "  dev-down   - Stop PostgreSQL database"
	@echo "  dev-clean  - Stop and remove all dev containers, volumes, and orphans"
	@echo "  build      - Build the application"
	@echo "  test       - Run tests"
	@echo "  clean      - Clean build artifacts"

# Development environment commands
dev-up:
	docker-compose -f docker-compose-dev.yml up -d

dev-down:
	docker-compose -f docker-compose-dev.yml down

dev-clean:
	docker-compose -f docker-compose-dev.yml down --volumes --remove-orphans

# Build, test, clean
build:
	cargo build

test:
	cargo test

clean:
	cargo clean