# Emergency Management Data Pipeline - DBT Project

## Overview

This DBT project provides comprehensive data transformation and analytics for emergency management data, designed to meet federal compliance requirements including FedRAMP, DORA, FISMA, and NIST standards. The project processes data from multiple sources including FEMA, NOAA, CoAgMet, and USDA to support emergency preparedness and response operations.

## Architecture

### Data Sources
- **FEMA OpenFEMA**: Disaster declarations and emergency events
- **NOAA Weather API**: Weather alerts and meteorological warnings  
- **Colorado AgMet (CoAgMet)**: Agricultural weather station data
- **USDA RMA**: Agricultural insurance and risk data

### Target Database
- **StarRocks**: High-performance OLAP database optimized for analytics
- **Engine**: OLAP with distributed hash partitioning
- **Compression**: LZ4 for optimal storage and query performance
- **Replication**: 3x replication for federal data durability requirements

## Project Structure

```
dbt_project/
├── dbt_project.yml              # Main project configuration
├── profiles.yml                 # Database connection profiles
├── models/
│   ├── staging/                 # Raw data standardization
│   │   ├── stg_fema_disasters.sql
│   │   ├── stg_noaa_weather.sql
│   │   ├── stg_coagmet_data.sql
│   │   └── stg_usda_data.sql
│   ├── marts/                   # Business logic models
│   │   ├── emergency_events.sql
│   │   ├── weather_impacts.sql
│   │   └── disaster_analytics.sql
│   ├── metrics/                 # Data quality and performance
│   │   ├── data_quality_metrics.sql
│   │   ├── usage_metrics.sql
│   │   └── api_performance.sql
│   └── public/                  # Public API optimized models
│       ├── public_disasters.sql
│       ├── public_weather_alerts.sql
│       └── public_agricultural_data.sql
├── tests/                       # Data quality tests
├── seeds/                       # Reference data
├── macros/                      # Reusable functions
├── snapshots/                   # Historical change tracking
└── docs/                        # Documentation
```

## Federal Compliance Features

### Data Classification
All models include mandatory federal data classification levels:
- `PUBLIC`: Publicly accessible emergency data
- `INTERNAL`: Internal government use only
- `RESTRICTED`: Limited access for authorized personnel
- `CONFIDENTIAL`: Highly sensitive national security data

### Data Retention
Automated retention date calculation per federal requirements:
- **Emergency Events**: 7 years (2555 days) per FEMA regulations
- **Weather Data**: 3 years (1095 days) per NOAA requirements
- **Agricultural Data**: 3 years (1095 days) per USDA standards
- **Audit Logs**: 7 years (2555 days) per federal audit requirements

### Audit Trail
Complete audit trail for all data transformations:
- Processing timestamps
- DBT version tracking
- Data lineage documentation
- User access logging
- Compliance framework tracking

## Key Models

### Staging Models
**Purpose**: Standardize and validate raw data with federal compliance metadata

- `stg_fema_disasters`: FEMA disaster declarations with standardized fields
- `stg_noaa_weather`: Weather alerts with risk level assessment
- `stg_coagmet_data`: Agricultural weather data with quality validation
- `stg_usda_data`: Agricultural insurance data with loss calculations

### Mart Models
**Purpose**: Business logic and analytics for emergency management

- `emergency_events`: Unified view of disasters and significant weather events
- `weather_impacts`: Weather impact analysis with agricultural risk assessment
- `disaster_analytics`: Historical trend analysis and risk scoring

### Public API Models
**Purpose**: Privacy-preserving public data access with rate limiting

- `public_disasters`: Public disaster information with usage tracking
- `public_weather_alerts`: Current weather alerts for public consumption
- `public_agricultural_data`: Aggregated agricultural data (privacy-compliant)

### Metrics Models
**Purpose**: Data quality monitoring and performance tracking

- `data_quality_metrics`: Completeness, accuracy, and timeliness monitoring
- `usage_metrics`: API usage patterns and system performance
- `api_performance`: Dataset health and freshness monitoring

## Configuration

### Environment Variables
```bash
# Development Environment
STARROCKS_HOST=localhost
STARROCKS_PORT=9030
STARROCKS_DATABASE=emergency_data_dev
STARROCKS_SCHEMA=emergency_dev
STARROCKS_USER=dev_user
STARROCKS_PASSWORD=dev_password

# Production Environment  
STARROCKS_PROD_HOST=starrocks-prod.emergency.gov
STARROCKS_PROD_PORT=9030
STARROCKS_PROD_DATABASE=emergency_data_prod
STARROCKS_PROD_SCHEMA=emergency_prod
STARROCKS_PROD_USER=prod_user
STARROCKS_PROD_PASSWORD=${ENCRYPTED_PROD_PASSWORD}

# Federal Environment
STARROCKS_FEDERAL_HOST=starrocks-federal.dhs.gov
STARROCKS_FEDERAL_PORT=9030
STARROCKS_FEDERAL_DATABASE=emergency_data_federal
STARROCKS_FEDERAL_SCHEMA=tenant_federal_dhs
STARROCKS_FEDERAL_USER=federal_user
STARROCKS_FEDERAL_PASSWORD=${ENCRYPTED_FEDERAL_PASSWORD}
```

### DBT Variables
Key variables for customization:
- `classification_levels`: Data classification mapping
- `retention_periods`: Retention requirements by data type
- `compliance_frameworks`: Applicable compliance standards
- `quality_thresholds`: Data quality acceptance criteria

## Usage

### Initial Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Configure DBT profile
cp profiles.yml ~/.dbt/profiles.yml

# Verify connection
dbt debug --target dev

# Load reference data
dbt seed --target dev

# Run staging models
dbt run --models staging --target dev
```

### Development Workflow
```bash
# Run models incrementally
dbt run --target dev

# Test data quality
dbt test --target dev

# Generate documentation
dbt docs generate --target dev
dbt docs serve --port 8080

# Create snapshots
dbt snapshot --target dev
```

### Production Deployment
```bash
# Deploy to production
dbt run --target prod --full-refresh

# Run compliance tests
dbt test --target prod --models tag:compliance

# Validate federal requirements
dbt test --target prod --models tag:fedramp
```

## Data Quality Testing

### Compliance Tests
- **Federal Data Classification**: Validates all records have proper classification
- **Retention Date Compliance**: Ensures retention dates meet federal requirements
- **Data Freshness**: Monitors data update frequency per source requirements
- **PII Protection**: Scans for potential PII data in public datasets

### Business Logic Tests  
- **Temporal Consistency**: Validates date ranges and time sequences
- **Cross-Reference Integrity**: Ensures foreign key relationships
- **Calculation Accuracy**: Validates computed fields and aggregations
- **Geographic Consistency**: Checks state codes and geographic references

### Performance Tests
- **Table Size Monitoring**: Alerts on table growth patterns
- **Partition Distribution**: Ensures optimal partitioning strategy
- **Query Performance**: Monitors model execution times

## Monitoring and Alerting

### Data Quality Dashboards
- Real-time data quality scores by source
- Compliance status monitoring
- Data freshness alerts
- Error rate tracking

### Federal Compliance Reporting
- Automated compliance audit reports
- Data retention compliance status
- Access audit trails
- Security violation alerts

### Performance Monitoring
- Model execution times
- Resource utilization
- API response times
- System health status

## Security and Privacy

### Data Encryption
- Encryption at rest for all sensitive data
- Encryption in transit with TLS 1.3
- Database-level encryption for federal data

### Access Control
- Role-based access control (RBAC)
- Multi-factor authentication (MFA) required
- Session timeout enforcement
- IP address whitelisting for federal access

### Privacy Protection
- Automatic PII detection and masking
- Minimum aggregation thresholds for public data
- Privacy impact assessments for all models
- GDPR and CCPA compliance features

## Disaster Recovery

### Backup Strategy
- Daily automated backups of all data
- Cross-region replication for federal data
- Point-in-time recovery capability
- 99.9% data durability guarantee

### Business Continuity
- Real-time data replication
- Automated failover procedures
- Recovery time objective (RTO): 4 hours
- Recovery point objective (RPO): 1 hour

## Support and Maintenance

### Team Contacts
- **Data Engineering**: data-team@emergency.gov
- **Compliance Officer**: compliance@emergency.gov  
- **Security Team**: security@emergency.gov
- **Emergency Response**: response@emergency.gov

### Service Level Agreements
- **Data Freshness**: 99.5% within SLA thresholds
- **System Availability**: 99.9% uptime
- **Query Performance**: 95% under 30 seconds
- **Support Response**: 4 hours for critical issues

### Documentation
- Technical documentation: `/docs/technical/`
- User guides: `/docs/users/`
- API documentation: `/docs/api/`
- Compliance documentation: `/docs/compliance/`

## Contributing

### Development Standards
- All models must include federal compliance metadata
- Comprehensive testing required for all changes
- Code review required for production deployments
- Security review required for federal data access

### Change Management
- Version control with Git
- Automated testing pipeline
- Staged deployment process
- Rollback procedures documented

For detailed technical documentation, see the `/docs/` directory or contact the data engineering team.