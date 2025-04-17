.PHONY: setup build up down clean clean-all clean-data clean-venv test lint format setup-airflow

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

# Comprehensive cleanup (containers, volumes, images, data)
clean-all:
	docker compose down -v
	docker rmi -f dataenge_test_ig-airflow dataenge_test_ig-postgres || true
	rm -rf data/raw/* data/processed/* data/final/*
	rm -rf logs/* airflow/logs/*

# Clean up only data files
clean-data:
	rm -rf data/raw/* data/processed/* data/final/*
	rm -rf logs/* airflow/logs/*

# Remove Python virtual environment
clean-venv:
	rm -rf venv/

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

# Comprehensive Airflow setup
setup-airflow: setup build up
	@echo "Waiting for Airflow services to start..."
	@sleep 10
	@echo "Initializing Airflow database..."
	docker compose run --rm airflow airflow db init
	@echo "Creating Airflow admin user..."
	docker compose run --rm airflow airflow users create \
		--username admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com \
		--password admin
	@echo "Setting up Airflow connections and variables..."
	docker compose run --rm airflow python /opt/airflow/init_airflow.py
	@echo "Airflow setup completed successfully!"
	@echo "You can now access the Airflow UI at http://localhost:8080"
	@echo "Username: admin"
	@echo "Password: admin"

# Help
help:
	@echo "Available commands:"
	@echo "  make setup      - Create project directory structure"
	@echo "  make build      - Build Docker images"
	@echo "  make up         - Start services"
	@echo "  make down       - Stop services"
	@echo "  make clean      - Clean up all data and containers"
	@echo "  make clean-all  - Comprehensive cleanup (containers, volumes, images, data)"
	@echo "  make clean-data - Clean up only data files"
	@echo "  make clean-venv - Remove Python virtual environment"
	@echo "  make test       - Run tests"
	@echo "  make lint       - Run linting"
	@echo "  make format     - Format code"
	@echo "  make init-airflow - Initialize Airflow"
	@echo "  make setup-airflow - Comprehensive Airflow setup (build, start, init, configure)" 