# Emergency Management Data Pipeline

A comprehensive, scalable, and federally compliant data pipeline for emergency management data processing. This system ingests data from multiple sources including FEMA, NOAA, CoAgMet, USDA, and custom APIs, processes it through Apache Flink, stores it in StarRocks, and orchestrates workflows using Dagster with DBT transformations.

## 🌐 Public Data Pipeline

This pipeline processes and shares **publicly available** emergency management data while maintaining the flexibility for future sensitive data handling:

**Current Data Sources (All Public):**
- ✅ FEMA disaster declarations and public assistance data
- ✅ NOAA weather alerts and observations 
- ✅ CoAgMet agricultural weather station data
- ✅ USDA agricultural disaster and insurance data

**Key Features:**
- ✅ Real-time public data streaming
- ✅ Open API for public access
- ✅ Usage analytics and rate limiting
- ✅ Multiple data formats (REST, streaming, bulk)
- ✅ Organization-aware access (government, academic, commercial, public)
- ✅ **Ready for future sensitive data** with built-in tenant separation capability

## 🏗️ Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Data      │    │   Apache    │    │  StarRocks  │    │   Dagster   │
│  Sources    │───▶│   Flink     │───▶│  Database   │───▶│Orchestration│
│             │    │(Streaming)  │    │ (Storage)   │    │    +DBT     │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
      │                    │                   │                   │
  ┌───▼───┐           ┌────▼────┐         ┌────▼────┐         ┌────▼────┐
  │ FEMA  │           │ Kafka   │         │ MySQL   │         │ PostSQL │
  │ NOAA  │           │Message  │         │Metadata │         │Metadata │
  │CoAgMet│           │ Queue   │         │ Store   │         │ Store   │
  │ USDA  │           └─────────┘         └─────────┘         └─────────┘
  └───────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker Desktop or Docker Engine (4+ GB RAM allocated)
- Docker Compose v2.0+
- 8+ GB available system memory
- 4+ CPU cores (recommended)
- 20+ GB available disk space

### 1. Clone and Start

```bash
git clone <repository-url>
cd emergency-management-pipeline

# Start the entire pipeline
./start.sh

# Or using Make
make start
```

### 2. Access Services

- **Dagster UI**: http://localhost:3000
- **Flink Dashboard**: http://localhost:8081  
- **StarRocks Frontend**: http://localhost:8030

### 3. Verify Installation

```bash
# Check service status
make status

# Run troubleshooting
make troubleshoot

# View logs
make logs
```

## 📊 Data Sources

The pipeline supports modular ingestion from multiple emergency management sources:

| Source | Type | Frequency | Description |
|--------|------|-----------|-------------|
| **FEMA OpenFEMA** | REST API | Real-time | Disaster declarations, public assistance |
| **NOAA Weather** | REST API | Hourly | Weather alerts, observations |
| **CoAgMet** | REST API | Hourly | Agricultural weather stations |
| **USDA** | REST API | Daily | Agricultural disaster data |
| **Custom Sources** | Configurable | Variable | Extensible for new data sources |

## 🔧 Configuration

### Environment Variables

Create a `.env` file or modify the existing one:

```bash
# Database Configuration
STARROCKS_HOST=starrocks-fe
STARROCKS_USER=root
STARROCKS_PASSWORD=
STARROCKS_DATABASE=emergency_data

# Kafka Configuration  
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Federal Compliance
FEDRAMP_COMPLIANCE=true
DORA_COMPLIANCE=true
AUDIT_LOGGING=true
DATA_ENCRYPTION=true
```

### Scaling Configuration

For production or higher-volume environments:

```yaml
# In docker-compose.yml, adjust resource limits:
flink-taskmanager:
  scale: 4  # Increase number of task managers
  environment:
    - taskmanager.numberOfTaskSlots=8
    - taskmanager.memory.process.size=4096m
```

## 📈 Data Pipeline

### 1. Data Ingestion (Scrapers → Kafka → Flink)

```python
# Example: Adding a new data source
@asset(
    description="New emergency data source",
    group_name="raw_data",
    compute_kind="api_ingestion",
)
def new_data_source(context, kafka: KafkaResource):
    # Fetch data from API
    data = fetch_from_api()
    
    # Send to Kafka for real-time processing
    producer = kafka.get_producer()
    producer.send('new_data_topic', value=data)
    
    return data
```

### 2. Stream Processing (Flink)

```sql
-- Real-time aggregations in Flink
CREATE TABLE disaster_alerts AS
SELECT 
    disaster_type,
    state,
    COUNT(*) as incident_count,
    TUMBLE_END(declaration_date, INTERVAL '1' HOUR) as window_end
FROM fema_disasters
GROUP BY disaster_type, state, TUMBLE(declaration_date, INTERVAL '1' HOUR);
```

### 3. Data Transformation (DBT)

```sql
-- models/staging/stg_fema_disasters.sql
{{ config(materialized='view', tags=['staging', 'emergency_data']) }}

SELECT 
    disaster_number,
    UPPER(state) as state_code,
    CAST(declaration_date AS TIMESTAMP) as declaration_date,
    incident_type,
    -- Data quality validations
    CASE WHEN disaster_number IS NULL THEN 1 ELSE 0 END as data_quality_issues
FROM {{ source('raw_data', 'fema_disaster_declarations') }}
WHERE data_quality_issues = 0
```

### 4. Orchestration (Dagster)

```python
# Scheduled jobs with federal compliance
data_ingestion_schedule = ScheduleDefinition(
    job=data_ingestion_job,
    cron_schedule="*/15 * * * *",  # Every 15 minutes
    default_status=DefaultScheduleStatus.RUNNING,
)
```

## 🛠️ Development

### Common Commands

```bash
# Start development environment
make dev-setup

# View service logs
make logs-dagster
make logs-flink
make logs-starrocks

# Run data quality tests
make test

# Format code
make format

# Database access
make db-connect
```

### Adding New Data Sources

1. **Create scraper in `scrapers/`**:
```python
class NewScraper(EmergencyDataScraper):
    async def scrape_data(self):
        # Implementation
        pass
```

2. **Add Dagster asset in `dagster/emergency_pipeline/assets/`**:
```python
@asset(group_name="raw_data")
def new_source_data(context, kafka: KafkaResource):
    # Asset implementation
    pass
```

3. **Create DBT model in `dagster/dbt_project/models/`**:
```sql
-- staging/stg_new_source.sql
SELECT * FROM {{ source('raw_data', 'new_source_table') }}
```

### Flink Job Development

```python
# Deploy new Flink streaming job
def deploy_custom_flink_job():
    env = flink_resource.get_execution_environment()
    
    # Define data stream processing
    data_stream = env.add_source(kafka_source)
    processed_stream = data_stream.map(process_function)
    processed_stream.add_sink(starrocks_sink)
    
    env.execute("custom_emergency_job")
```

## 🔒 Security & Compliance

### Federal Requirements

- **FedRAMP Moderate**: Baseline security controls implemented
- **DORA**: Digital operational resilience monitoring
- **Data Isolation**: Multi-tenant data separation
- **Audit Logging**: Complete audit trail for all operations
- **Encryption**: AES-256 encryption at rest, TLS 1.3 in transit

### Production Security Checklist

```bash
# Enable SSL/TLS
sed -i 's/ssl_enabled: false/ssl_enabled: true/g' starrocks/fe.conf
sed -i 's/security.ssl.enabled: false/security.ssl.enabled: true/g' flink/conf/flink-conf.yaml

# Configure authentication
# Set strong passwords in .env file
# Enable network security groups
# Configure backup encryption
```

## 📋 Monitoring & Alerting

### Built-in Monitoring

- **Dagster**: Job success/failure, asset freshness
- **Flink**: Job health, throughput, latency
- **StarRocks**: Query performance, storage usage
- **Data Quality**: Great Expectations validation

### Alerts Configuration

```python
# Dagster sensors for automated alerting
@sensor(minimum_interval_seconds=300)
def data_freshness_sensor(context):
    if data_is_stale():
        return RunRequest(run_key="refresh_data")
    return SkipReason("Data is fresh")
```

## 🚨 Troubleshooting

### Common Issues

1. **Services not starting**:
```bash
make troubleshoot
docker-compose logs [service-name]
```

2. **Port conflicts**:
```bash
# Check port usage
netstat -tulpn | grep :3000
# Kill conflicting processes
sudo kill -9 $(lsof -t -i:3000)
```

3. **Memory issues**:
```bash
# Increase Docker memory allocation
# Reduce parallelism in docker-compose.yml
```

4. **Data not flowing**:
```bash
# Check Kafka topics
make shell-kafka
kafka-topics --list --bootstrap-server localhost:9092

# Check StarRocks tables
make db-connect
SHOW TABLES;
```

### Emergency Procedures

```bash
# Emergency stop
make emergency-stop

# Complete reset
make clean
make start
```

## 📚 Documentation

- **API Documentation**: `/docs/api/`
- **Data Dictionary**: `/docs/data-dictionary.md`
- **Security Guide**: `/docs/security.md`
- **Deployment Guide**: `/docs/deployment.md`
- **Troubleshooting**: `./troubleshoot.sh`

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/new-source`
3. Make changes following code standards
4. Run tests: `make test`
5. Submit pull request

### Code Standards

- **Python**: Black formatting, Flake8 linting
- **SQL**: DBT style guide
- **Documentation**: Update README and docs
- **Testing**: Add tests for new features

## 📄 License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Issues**: GitHub Issues
- **Documentation**: `/docs/`
- **Troubleshooting**: `./troubleshoot.sh`
- **Health Check**: `make health`

---

## 📈 Scaling to Production

### Government Cloud Deployment

For AWS GovCloud or Azure Government:

1. **Infrastructure as Code**: Use Terraform/CloudFormation
2. **Container Orchestration**: EKS/AKS with federal compliance
3. **Networking**: VPC with proper security groups
4. **Storage**: Encrypted EBS/Azure Disk
5. **Monitoring**: CloudWatch/Azure Monitor with compliance logging

### Performance Tuning

- **Flink**: Increase parallelism and memory
- **StarRocks**: Add more Backend nodes
- **Kafka**: Increase partitions and replicas
- **Dagster**: Use Kubernetes run launcher

The pipeline is designed to scale from development on a single quad-core machine to production environments handling thousands of emergency data sources with federal compliance requirements.