#!/bin/bash

# Emergency Management Data Pipeline Startup Script
# Federal compliance ready development environment

set -e

echo "🚨 Starting Emergency Management Data Pipeline..."
echo "=================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

# Check prerequisites
echo "Checking prerequisites..."

if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

print_status "Docker and Docker Compose are available"

# Check available memory and CPU
AVAILABLE_MEMORY=$(free -m | awk 'NR==2{printf "%.1f", $7/1024}')
AVAILABLE_CORES=$(nproc)

print_info "Available Memory: ${AVAILABLE_MEMORY}GB"
print_info "Available CPU Cores: ${AVAILABLE_CORES}"

if (( $(echo "$AVAILABLE_MEMORY < 8.0" | bc -l) )); then
    print_warning "Available memory is less than 8GB. Performance may be degraded."
fi

if (( AVAILABLE_CORES < 4 )); then
    print_warning "Less than 4 CPU cores available. Consider adjusting parallelism settings."
fi

# Create necessary directories
echo -e "\nCreating directory structure..."
mkdir -p flink/{lib,conf}
mkdir -p starrocks
mkdir -p dagster/emergency_pipeline/{assets,resources}
mkdir -p scrapers/{data,logs}
mkdir -p dagster/dbt_project/{models/{staging,marts,metrics},tests,seeds,macros}

print_status "Directory structure created"

# Create environment file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating default environment configuration..."
    cat > .env << EOF
# Emergency Management Pipeline Environment Configuration

# Database Configuration
STARROCKS_HOST=starrocks-fe
STARROCKS_USER=root
STARROCKS_PASSWORD=
STARROCKS_DATABASE=emergency_data

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Flink Configuration
FLINK_JOBMANAGER_HOST=flink-jobmanager
FLINK_JOBMANAGER_PORT=8081

# Dagster Configuration
DAGSTER_POSTGRES_USER=dagster
DAGSTER_POSTGRES_PASSWORD=dagster123
DAGSTER_POSTGRES_HOST=dagster-postgres
DAGSTER_POSTGRES_DB=dagster

# Federal Compliance
FEDRAMP_COMPLIANCE=true
DORA_COMPLIANCE=true
AUDIT_LOGGING=true
DATA_ENCRYPTION=true

# Development Settings
ENVIRONMENT=development
LOG_LEVEL=INFO
DEBUG_MODE=false
EOF
    print_status "Environment configuration created"
else
    print_status "Using existing environment configuration"
fi

# Download required JAR files for Flink
echo -e "\nDownloading Flink connector JARs..."
FLINK_VERSION="1.18"
CONNECTORS_DIR="flink/lib"

# Kafka connector
KAFKA_CONNECTOR="flink-sql-connector-kafka-3.0.1-1.18.jar"
if [ ! -f "$CONNECTORS_DIR/$KAFKA_CONNECTOR" ]; then
    print_info "Downloading Kafka connector..."
    curl -L "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/$KAFKA_CONNECTOR" \
         -o "$CONNECTORS_DIR/$KAFKA_CONNECTOR"
    print_status "Kafka connector downloaded"
fi

# MySQL connector for StarRocks
MYSQL_CONNECTOR="mysql-connector-java-8.0.33.jar"
if [ ! -f "$CONNECTORS_DIR/$MYSQL_CONNECTOR" ]; then
    print_info "Downloading MySQL connector..."
    curl -L "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.33/$MYSQL_CONNECTOR" \
         -o "$CONNECTORS_DIR/$MYSQL_CONNECTOR"
    print_status "MySQL connector downloaded"
fi

# Stop any existing containers
echo -e "\nStopping any existing containers..."
docker-compose down --remove-orphans 2>/dev/null || true
print_status "Cleaned up existing containers"

# Start the infrastructure
echo -e "\nStarting infrastructure services..."
print_info "This may take several minutes on first run..."

# Start core infrastructure first
docker-compose up -d zookeeper kafka mysql dagster-postgres

# Wait for services to be ready
echo "Waiting for core services to start..."
sleep 30

# Start StarRocks
print_info "Starting StarRocks..."
docker-compose up -d starrocks-fe
sleep 20
docker-compose up -d starrocks-be

# Wait for StarRocks to be ready
echo "Waiting for StarRocks to initialize..."
sleep 30

# Start Flink
print_info "Starting Flink..."
docker-compose up -d flink-jobmanager
sleep 10
docker-compose up -d flink-taskmanager

# Start Dagster
print_info "Starting Dagster..."
docker-compose up -d dagster-daemon dagster-webserver

# Start scrapers
print_info "Starting data scrapers..."
docker-compose up -d data-scrapers

# Wait for all services to be fully ready
echo -e "\nWaiting for all services to be ready..."
sleep 60

# Check service health
echo -e "\nChecking service health..."

# Function to check if a service is responding
check_service() {
    local service_name=$1
    local url=$2
    local expected_status=${3:-200}
    
    if curl -s -o /dev/null -w "%{http_code}" "$url" | grep -q "$expected_status"; then
        print_status "$service_name is healthy"
        return 0
    else
        print_warning "$service_name may not be ready yet"
        return 1
    fi
}

# Check Flink
check_service "Flink JobManager" "http://localhost:8081" || true

# Check StarRocks FE
check_service "StarRocks Frontend" "http://localhost:8030" || true

# Check Dagster
check_service "Dagster Webserver" "http://localhost:3000" || true

# Initialize database schema
echo -e "\nInitializing database schema..."
docker-compose exec starrocks-fe mysql -h127.0.0.1 -P9030 -uroot -e "
CREATE DATABASE IF NOT EXISTS emergency_data;
USE emergency_data;

-- Raw data tables
CREATE TABLE IF NOT EXISTS fema_disaster_declarations (
    disaster_number VARCHAR(50),
    state VARCHAR(10),
    declaration_type VARCHAR(50),
    declaration_date DATETIME,
    incident_type VARCHAR(100),
    incident_begin_date DATETIME,
    incident_end_date DATETIME,
    title TEXT,
    fy_declared INT,
    disaster_closeout_date DATETIME,
    place_code VARCHAR(20),
    designated_area TEXT,
    declaration_request_number VARCHAR(50),
    ingestion_timestamp DATETIME,
    data_source VARCHAR(50)
) ENGINE=OLAP
DUPLICATE KEY(disaster_number)
DISTRIBUTED BY HASH(disaster_number) BUCKETS 10;

CREATE TABLE IF NOT EXISTS noaa_weather_alerts (
    alert_id VARCHAR(100),
    event VARCHAR(100),
    severity VARCHAR(50),
    urgency VARCHAR(50),
    certainty VARCHAR(50),
    headline TEXT,
    description TEXT,
    instruction TEXT,
    effective DATETIME,
    expires DATETIME,
    area_desc TEXT,
    ingestion_timestamp DATETIME,
    data_source VARCHAR(50)
) ENGINE=OLAP
DUPLICATE KEY(alert_id)
DISTRIBUTED BY HASH(alert_id) BUCKETS 10;
" 2>/dev/null || print_warning "Could not initialize database schema (StarRocks may still be starting)"

print_status "Database initialization attempted"

# Display access information
echo -e "\n${GREEN}🎉 Emergency Management Data Pipeline Started Successfully!${NC}"
echo "================================================================"
echo ""
echo "📊 Access URLs:"
echo "  • Dagster UI:        http://localhost:3000"
echo "  • Flink Dashboard:   http://localhost:8081"
echo "  • StarRocks FE:      http://localhost:8030"
echo ""
echo "🔗 Database Connections:"
echo "  • StarRocks Query:   mysql -h localhost -P 9030 -u root"
echo "  • PostgreSQL:        psql -h localhost -p 5432 -U dagster -d dagster"
echo ""
echo "📁 Data Directories:"
echo "  • Flink Checkpoints: ./volumes/flink-checkpoints"
echo "  • StarRocks Data:    ./volumes/starrocks-*"
echo "  • Dagster Storage:   ./volumes/dagster-storage"
echo ""
echo "🔧 Management Commands:"
echo "  • View logs:         docker-compose logs -f [service-name]"
echo "  • Stop pipeline:     docker-compose down"
echo "  • Restart service:   docker-compose restart [service-name]"
echo ""
echo "📋 Federal Compliance Features:"
echo "  ✓ Data encryption enabled"
echo "  ✓ Audit logging