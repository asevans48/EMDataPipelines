# Emergency Management Data Pipeline Makefile
# Simplified commands for development and deployment

.PHONY: help start stop restart status logs clean build test lint format

# Default target
help:
	@echo "Emergency Management Data Pipeline"
	@echo "=================================="
	@echo ""
	@echo "Available commands:"
	@echo "  make start      - Start the entire pipeline"
	@echo "  make stop       - Stop all services"
	@echo "  make restart    - Restart all services"
	@echo "  make status     - Check service status"
	@echo "  make logs       - View logs for all services"
	@echo "  make clean      - Clean up containers and volumes"
	@echo "  make build      - Build custom Docker images"
	@echo "  make test       - Run data quality tests"
	@echo "  make lint       - Run code linting"
	@echo "  make format     - Format Python code"
	@echo "  make troubleshoot - Run troubleshooting script"
	@echo ""
	@echo "Service-specific commands:"
	@echo "  make logs-dagster     - View Dagster logs"
	@echo "  make logs-flink       - View Flink logs"
	@echo "  make logs-starrocks   - View StarRocks logs"
	@echo "  make logs-scrapers    - View scrapers logs"
	@echo ""
	@echo "Development commands:"
	@echo "  make shell-dagster    - Open shell in Dagster container"
	@echo "  make shell-flink      - Open shell in Flink container"
	@echo "  make db-connect       - Connect to StarRocks database"

# Start the pipeline
start:
	@echo "🚀 Starting Emergency Management Pipeline..."
	@chmod +x start.sh
	@./start.sh

# Stop all services
stop:
	@echo "🛑 Stopping all services..."
	@docker-compose down

# Restart all services
restart: stop start

# Check service status
status:
	@echo "📊 Service Status:"
	@docker-compose ps

# View logs for all services
logs:
	@docker-compose logs -f

# Service-specific logs
logs-dagster:
	@docker-compose logs -f dagster-webserver dagster-daemon

logs-flink:
	@docker-compose logs -f flink-jobmanager flink-taskmanager

logs-starrocks:
	@docker-compose logs -f starrocks-fe starrocks-be

logs-scrapers:
	@docker-compose logs -f data-scrapers

# Clean up containers and volumes
clean:
	@echo "🧹 Cleaning up containers and volumes..."
	@docker-compose down -v --remove-orphans
	@docker system prune -f
	@docker volume prune -f

# Build custom Docker images
build:
	@echo "🔨 Building custom Docker images..."
	@docker-compose build --no-cache

# Run troubleshooting
troubleshoot:
	@chmod +x troubleshoot.sh
	@./troubleshoot.sh

# Development shells
shell-dagster:
	@docker-compose exec dagster-webserver /bin/bash

shell-flink:
	@docker-compose exec flink-jobmanager /bin/bash

shell-scrapers:
	@docker-compose exec data-scrapers /bin/bash

# Database connection
db-connect:
	@docker-compose exec starrocks-fe mysql -h127.0.0.1 -P9030 -uroot

# Development and testing commands
test:
	@echo "🧪 Running data quality tests..."
	@docker-compose exec dagster-webserver dagster asset materialize --select tag:test

lint:
	@echo "🔍 Running code linting..."
	@docker-compose exec dagster-webserver python -m flake8 emergency_pipeline/
	@docker-compose exec data-scrapers python -m flake8 .

format:
	@echo "✨ Formatting Python code..."
	@docker-compose exec dagster-webserver python -m black emergency_pipeline/
	@docker-compose exec data-scrapers python -m black .

# Backup and restore
backup:
	@echo "💾 Creating backup..."
	@mkdir -p backups
	@docker-compose exec starrocks-fe mysqldump -h127.0.0.1 -P9030 -uroot emergency_data > backups/emergency_data_$(shell date +%Y%m%d_%H%M%S).sql
	@echo "Backup created in backups/ directory"

# Performance monitoring
monitor:
	@echo "📈 Performance monitoring..."
	@echo "Flink Dashboard: http://localhost:8081"
	@echo "Dagster UI: http://localhost:3000"
	@echo "StarRocks Metrics: http://localhost:8030"

# Quick health check
health:
	@echo "🏥 Health Check..."
	@curl -s http://localhost:3000 > /dev/null && echo "✓ Dagster: Healthy" || echo "✗ Dagster: Unhealthy"
	@curl -s http://localhost:8081 > /dev/null && echo "✓ Flink: Healthy" || echo "✗ Flink: Unhealthy"
	@curl -s http://localhost:8030 > /dev/null && echo "✓ StarRocks: Healthy" || echo "✗ StarRocks: Unhealthy"

# Development workflow
dev-setup: build start
	@echo "🔧 Development environment ready!"
	@echo "Access Dagster UI: http://localhost:3000"

# Production deployment preparation
prod-check:
	@echo "🔒 Production readiness check..."
	@echo "Checking security configurations..."
	@grep -q "ssl_enabled.*true" starrocks/fe.conf && echo "✓ StarRocks SSL enabled" || echo "⚠ StarRocks SSL disabled"
	@grep -q "security.ssl.enabled.*true" flink/conf/flink-conf.yaml && echo "✓ Flink SSL enabled" || echo "⚠ Flink SSL disabled"
	@echo "Review security settings before production deployment!"

# Data pipeline triggers
trigger-ingestion:
	@echo "🔄 Triggering data ingestion..."
	@docker-compose exec dagster-webserver dagster job execute --job data_ingestion_job

trigger-processing:
	@echo "⚙️ Triggering data processing..."
	@docker-compose exec dagster-webserver dagster job execute --job data_processing_job

trigger-quality:
	@echo "🔍 Triggering data quality checks..."
	@docker-compose exec dagster-webserver dagster job execute --job data_quality_job

# Emergency procedures
emergency-stop:
	@echo "🚨 Emergency stop - killing all containers..."
	@docker-compose kill
	@docker-compose down -v

emergency-restart:
	@echo "🚨 Emergency restart..."
	@make emergency-stop
	@sleep 5
	@make start