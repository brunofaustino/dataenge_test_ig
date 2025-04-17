.PHONY: setup build up down clean test lint format

# Default target
all: setup build up

# Setup project structure
setup:
	mkdir -p data/raw data/processed data/final
	mkdir -p logs plugins dags

# Build Docker images
build:
	docker compose build

# Start services
up:
	docker compose up -d

# Stop services
down:
	docker compose down

# Clean up
clean:
	docker compose down -v
	rm -rf data/raw/* data/processed/* data/final/*
	rm -rf logs/*

# Run tests
test:
	pytest tests/

# Run linting
lint:
	flake8 src/
	black --check src/
	isort --check-only src/

# Format code
format:
	black src/
	isort src/

# Initialize Airflow
init-airflow:
	docker compose run --rm airflow airflow db init
	docker compose run --rm airflow airflow users create \
		--username admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com \
		--password admin

# Help
help:
	@echo "Available commands:"
	@echo "  make setup      - Create project directory structure"
	@echo "  make build      - Build Docker images"
	@echo "  make up         - Start services"
	@echo "  make down       - Stop services"
	@echo "  make clean      - Clean up all data and containers"
	@echo "  make test       - Run tests"
	@echo "  make lint       - Run linting"
	@echo "  make format     - Format code"
	@echo "  make init-airflow - Initialize Airflow" 