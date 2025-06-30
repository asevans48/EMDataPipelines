#!/bin/bash

# Emergency Management Pipeline Troubleshooting Script
# Diagnoses common issues and provides resolution steps

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

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

echo "🔧 Emergency Management Pipeline Troubleshooter"
echo "==============================================="

# Check Docker and Docker Compose
print_header "Docker Environment"
if command -v docker &> /dev/null; then
    DOCKER_VERSION=$(docker --version)
    print_status "Docker: $DOCKER_VERSION"
else
    print_error "Docker not found. Please install Docker."
    exit 1
fi

if command -v docker-compose &> /dev/null; then
    COMPOSE_VERSION=$(docker-compose --version)
    print_status "Docker Compose: $COMPOSE_VERSION"
else
    print_error "Docker Compose not found. Please install Docker Compose."
    exit 1
fi

# Check system resources
print_header "System Resources"
MEMORY_GB=$(free -m | awk 'NR==2{printf "%.1f", $2/1024}')
AVAILABLE_MEMORY_GB=$(free -m | awk 'NR==2{printf "%.1f", $7/1024}')
CPU_CORES=$(nproc)
DISK_USAGE=$(df -h . | awk 'NR==2 {print $5}' | sed 's/%//')

print_info "Total Memory: ${MEMORY_GB}GB"
print_info "Available Memory: ${AVAILABLE_MEMORY_GB}GB" 
print_info "CPU Cores: $CPU_CORES"
print_info "Disk Usage: ${DISK_USAGE}%"

if (( $(echo "$AVAILABLE_MEMORY_GB < 4.0" | bc -l) )); then
    print_warning "Low memory available. Consider freeing up memory or increasing system RAM."
fi

if (( DISK_USAGE > 85 )); then
    print_warning "Disk usage is high (${DISK_USAGE}%). Consider cleaning up disk space."
fi

# Check container status
print_header "Container Status"
if docker-compose ps &> /dev/null; then
    CONTAINERS=$(docker-compose ps --format "table {{.Service}}\t{{.State}}\t{{.Ports}}")
    echo "$CONTAINERS"
    
    # Count running containers
    RUNNING_COUNT=$(docker-compose ps | grep "Up" | wc -l)
    TOTAL_COUNT=$(docker-compose ps | tail -n +3 | wc -l)
    
    if [ "$RUNNING_COUNT" -eq "$TOTAL_COUNT" ] && [ "$TOTAL_COUNT" -gt 0 ]; then
        print_status "All containers are running ($RUNNING_COUNT/$TOTAL_COUNT)"
    else
        print_warning "Some containers are not running ($RUNNING_COUNT/$TOTAL_COUNT)"
    fi
else
    print_warning "No containers found. Run ./start.sh to start the pipeline."
fi

# Check service health
print_header "Service Health Checks"

check_service_health() {
    local service=$1
    local url=$2
    local timeout=${3:-5}
    
    if curl -s --max-time $timeout "$url" > /dev/null 2>&1; then
        print_status "$service is responding"
        return 0
    else
        print_error "$service is not responding at $url"
        return 1
    fi
}

# Check each service
SERVICES_HEALTHY=0
TOTAL_SERVICES=0

# Flink JobManager
TOTAL_SERVICES=$((TOTAL_SERVICES + 1))
if check_service_health "Flink JobManager" "http://localhost:8081"; then
    SERVICES_HEALTHY=$((SERVICES_HEALTHY + 1))
fi

# StarRocks Frontend
TOTAL_SERVICES=$((TOTAL_SERVICES + 1))
if check_service_health "StarRocks Frontend" "http://localhost:8030"; then
    SERVICES_HEALTHY=$((SERVICES_HEALTHY + 1))
fi

# Dagster Webserver
TOTAL_SERVICES=$((TOTAL_SERVICES + 1))
if check_service_health "Dagster Webserver" "http://localhost:3000"; then
    SERVICES_HEALTHY=$((SERVICES_HEALTHY + 1))
fi

echo ""
if [ "$SERVICES_HEALTHY" -eq "$TOTAL_SERVICES" ]; then
    print_status "All services are healthy ($SERVICES_HEALTHY/$TOTAL_SERVICES)"
else
    print_warning "Some services are unhealthy ($SERVICES_HEALTHY/$TOTAL_SERVICES)"
fi

# Check logs for errors
print_header "Recent Error Analysis"
print_info "Checking recent logs for errors..."

ERROR_COUNT=0

# Function to check logs for errors
check_logs() {
    local service=$1
    local error_patterns=("ERROR" "FATAL" "Exception" "failed" "timeout")
    
    if docker-compose logs --tail=50 "$service" 2>/dev/null | grep -i -E "(ERROR|FATAL|Exception|failed|timeout)" > /dev/null; then
        RECENT_ERRORS=$(docker-compose logs --tail=10 "$service" 2>/dev/null | grep -i -E "(ERROR|FATAL|Exception|failed|timeout)" | wc -l)
        if [ "$RECENT_ERRORS" -gt 0 ]; then
            print_warning "$service has $RECENT_ERRORS recent error(s)"
            ERROR_COUNT=$((ERROR_COUNT + RECENT_ERRORS))
            
            # Show sample error
            SAMPLE_ERROR=$(docker-compose logs --tail=10 "$service" 2>/dev/null | grep -i -E "(ERROR|FATAL|Exception|failed|timeout)" | head -1)
            print_info "Sample error: ${SAMPLE_ERROR:0:80}..."
        fi
    fi
}

# Check logs for each service
SERVICES=("flink-jobmanager" "flink-taskmanager" "starrocks-fe" "starrocks-be" "dagster-webserver" "dagster-daemon" "data-scrapers")

for service in "${SERVICES[@]}"; do
    if docker-compose ps | grep -q "$service.*Up"; then
        check_logs "$service"
    fi
done

if [ "$ERROR_COUNT" -eq 0 ]; then
    print_status "No recent errors found in logs"
else
    print_warning "Found $ERROR_COUNT recent error(s) in logs"
fi

# Check data flow
print_header "Data Flow Verification"

# Check if Kafka topics exist and have data
if docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "fema_disasters"; then
    print_status "Kafka topics are created"
    
    # Check message count in topics
    TOPIC_COUNT=$(docker-compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic fema_disasters --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}' || echo "0")
    
    if [ "$TOPIC_COUNT" -gt 0 ]; then
        print_status "Kafka topics have data ($TOPIC_COUNT messages)"
    else
        print_warning "Kafka topics exist but appear empty"
    fi
else
    print_warning "Kafka topics may not be created yet"
fi

# Check StarRocks tables
if docker-compose exec -T starrocks-fe mysql -h127.0.0.1 -P9030 -uroot -e "USE emergency_data; SHOW TABLES;" 2>/dev/null | grep -q "fema_disaster_declarations"; then
    print_status "StarRocks tables are created"
    
    # Check row counts
    ROW_COUNT=$(docker-compose exec -T starrocks-fe mysql -h127.0.0.1 -P9030 -uroot -e "USE emergency_data; SELECT COUNT(*) FROM fema_disaster_declarations;" 2>/dev/null | tail -1 || echo "0")
    
    if [ "$ROW_COUNT" -gt 0 ]; then
        print_status "StarRocks tables have data ($ROW_COUNT rows)"
    else
        print_warning "StarRocks tables exist but appear empty"
    fi
else
    print_warning "StarRocks tables may not be created yet"
fi

# Provide troubleshooting recommendations
print_header "Troubleshooting Recommendations"

if [ "$SERVICES_HEALTHY" -lt "$TOTAL_SERVICES" ]; then
    echo "🔧 Service Issues:"
    echo "   1. Check container logs: docker-compose logs [service-name]"
    echo "   2. Restart unhealthy services: docker-compose restart [service-name]"
    echo "   3. If persistent, try: docker-compose down && docker-compose up -d"
fi

if [ "$ERROR_COUNT" -gt 0 ]; then
    echo ""
    echo "🔧 Error Resolution:"
    echo "   1. Check full logs: docker-compose logs [service-name]"
    echo "   2. Verify configuration files are correct"
    echo "   3. Ensure sufficient system resources"
    echo "   4. Check network connectivity between services"
fi

if [ "$SERVICES_HEALTHY" -eq "$TOTAL_SERVICES" ] && [ "$ERROR_COUNT" -eq 0 ]; then
    echo ""
    print_status "System appears to be running normally"
    echo ""
    echo "📚 Additional Debugging:"
    echo "   • View real-time logs: docker-compose logs -f"
    echo "   • Check Flink jobs: http://localhost:8081"
    echo "   • Monitor Dagster runs: http://localhost:3000"
    echo "   • Query StarRocks: mysql -h localhost -P 9030 -u root"
fi

# Common solutions
echo ""
echo "🛠️  Common Solutions:"
echo "   • Port conflicts: Check if ports 3000, 8030, 8081, 9092 are available"
echo "   • Memory issues: Increase Docker memory allocation"
echo "   • Slow startup: Wait longer for services to initialize (especially StarRocks)"
echo "   • Permission issues: Ensure Docker has proper permissions"
echo "   • Network issues: Reset Docker networks: docker network prune"
echo ""
echo "📞 Need Help?"
echo "   • Check documentation: README.md"
echo "   • View logs: docker-compose logs [service-name]"
echo "   • Reset everything: docker-compose down -v && ./start.sh"