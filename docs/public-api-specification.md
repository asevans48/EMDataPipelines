# Public Emergency Data API Specification

## Overview

The Emergency Management Data Pipeline provides free, public access to emergency management data from federal and state sources. All data is publicly available and can be accessed without authentication, though rate limits apply based on organization type.

## Base URL
```
https://api.emergency-data.gov/v1
```

## Data Classification
All data served by this API is classified as **PUBLIC** and comes from publicly available sources:
- FEMA OpenFEMA API
- NOAA Weather Service
- CoAgMet (Colorado Agricultural Weather Network)  
- USDA Risk Management Agency

## Rate Limits

| Organization Type | API Calls/Hour | Records/Hour | Features |
|------------------|----------------|--------------|----------|
| **Government** | 10,000 | 1,000,000 | All features, bulk downloads |
| **Academic** | 5,000 | 500,000 | All features, bulk downloads |
| **Commercial** | 2,000 | 200,000 | Real-time alerts, historical data |
| **Public** | 1,000 | 100,000 | Real-time alerts, 30-day history |

## Authentication (Optional)
While authentication is not required, providing an organization identifier enables higher rate limits and usage analytics:

```http
X-Organization: colorado_emergency_management
X-Organization-Type: government
```

## Endpoints

### 1. Disaster Data

#### Get Recent Disasters
```http
GET /disasters
```

**Parameters:**
- `state` (optional): Two-letter state code (e.g., "CO", "CA")
- `incident_type` (optional): Type of disaster (e.g., "Flood", "Fire", "Severe Storm")
- `days` (optional): Number of days to look back (default: 30, max: 365 for government/academic)
- `limit` (optional): Number of results (default: 100, max: 1000)

**Example:**
```http
GET /disasters?state=CO&incident_type=Flood&days=90&limit=50
```

**Response:**
```json
{
  "data": [
    {
      "disaster_number": "4586",
      "state": "CO",
      "declaration_type": "Major Disaster",
      "declaration_date": "2023-06-15T00:00:00Z",
      "incident_type": "Flood",
      "incident_begin_date": "2023-06-10T00:00:00Z",
      "incident_end_date": "2023-06-14T00:00:00Z",
      "title": "Colorado Severe Storms and Flooding",
      "designated_area": "Boulder County, Larimer County",
      "data_source": "FEMA_OpenFEMA",
      "ingestion_timestamp": "2023-06-15T09:30:00Z"
    }
  ],
  "metadata": {
    "total_records": 1,
    "page": 1,
    "per_page": 50,
    "data_classification": "PUBLIC"
  }
}
```

#### Get Disaster by ID
```http
GET /disasters/{disaster_number}
```

#### Get Public Assistance Data
```http
GET /disasters/{disaster_number}/assistance
```

### 2. Weather Data

#### Get Current Weather Alerts
```http
GET /weather/alerts
```

**Parameters:**
- `state` (optional): Two-letter state code
- `severity` (optional): Alert severity ("Minor", "Moderate", "Severe", "Extreme")
- `event` (optional): Event type ("Tornado Warning", "Flood Warning", etc.)
- `active_only` (optional): Only active alerts (default: true)

**Example:**
```http
GET /weather/alerts?state=CO&severity=Severe&active_only=true
```

**Response:**
```json
{
  "data": [
    {
      "alert_id": "NWS-IDP-PROD-4661839",
      "event": "Severe Thunderstorm Warning",
      "severity": "Severe",
      "urgency": "Immediate",
      "certainty": "Observed",
      "headline": "Severe Thunderstorm Warning issued June 15 at 2:30PM MDT",
      "effective": "2023-06-15T20:30:00Z",
      "expires": "2023-06-15T23:00:00Z",
      "area_desc": "Adams County, CO",
      "data_source": "NOAA_Weather_API",
      "ingestion_timestamp": "2023-06-15T20:31:00Z"
    }
  ],
  "metadata": {
    "total_records": 1,
    "active_alerts": 1,
    "data_classification": "PUBLIC"
  }
}
```

#### Get Weather Observations
```http
GET /weather/observations
```

**Parameters:**
- `station_id` (optional): Specific weather station
- `state` (optional): Two-letter state code
- `hours` (optional): Hours to look back (default: 24, max: 168)

#### Get Agricultural Weather Data
```http
GET /weather/agricultural
```

**Parameters:**
- `station_id` (optional): CoAgMet station ID
- `hours` (optional): Hours to look back
- `measurements` (optional): Comma-separated list (temperature,humidity,wind_speed,precipitation)

### 3. Agricultural Data

#### Get USDA Agricultural Disaster Data
```http
GET /agricultural/disasters
```

**Parameters:**
- `state` (optional): Two-letter state code  
- `commodity` (optional): Agricultural commodity
- `program_year` (optional): Program year (default: current year)

### 4. Bulk Downloads (Government/Academic Only)

#### Get Historical Disaster Data
```http
GET /bulk/disasters/historical
```

**Parameters:**
- `start_date`: Start date (YYYY-MM-DD)
- `end_date`: End date (YYYY-MM-DD)
- `format`: Response format ("json", "csv", "parquet")
- `state` (optional): Two-letter state code

**Response:** Large JSON/CSV/Parquet file download

#### Get Historical Weather Data  
```http
GET /bulk/weather/historical
```

## Real-Time Streaming

### WebSocket Endpoints

#### Weather Alerts Stream
```
wss://api.emergency-data.gov/stream/weather/alerts
```

#### Disaster Updates Stream
```
wss://api.emergency-data.gov/stream/disasters/updates
```

**Example WebSocket Message:**
```json
{
  "type": "weather_alert",
  "timestamp": "2023-06-15T20:31:00Z",
  "data": {
    "alert_id": "NWS-IDP-PROD-4661839",
    "event": "Tornado Warning",
    "severity": "Extreme",
    "area_desc": "Denver County, CO",
    "effective": "2023-06-15T20:31:00Z",
    "expires": "2023-06-15T21:00:00Z"
  }
}
```

### Server-Sent Events (SSE)
```http
GET /stream/events
```

**Parameters:**
- `types`: Comma-separated event types (weather_alerts,disaster_updates)
- `states`: Comma-separated state codes

## Data Formats

### Standard Response Format
```json
{
  "data": [...],
  "metadata": {
    "total_records": 100,
    "page": 1,
    "per_page": 50,
    "data_classification": "PUBLIC",
    "last_updated": "2023-06-15T20:31:00Z",
    "source_attribution": [
      "Federal Emergency Management Agency (FEMA)",
      "National Weather Service (NOAA)"
    ]
  },
  "links": {
    "self": "/disasters?page=1",
    "next": "/disasters?page=2", 
    "last": "/disasters?page=10"
  }
}
```

### Error Response Format
```json
{
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "Rate limit exceeded for organization type",
    "details": {
      "limit": 1000,
      "reset_time": "2023-06-15T21:00:00Z"
    }
  }
}
```

## Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `RATE_LIMIT_EXCEEDED` | 429 | API rate limit exceeded |
| `INVALID_PARAMETER` | 400 | Invalid request parameter |
| `NOT_FOUND` | 404 | Resource not found |
| `SERVER_ERROR` | 500 | Internal server error |
| `SERVICE_UNAVAILABLE` | 503 | Data source temporarily unavailable |

## Usage Examples

### Python
```python
import requests

# Get recent Colorado disasters
response = requests.get(
    'https://api.emergency-data.gov/v1/disasters',
    params={'state': 'CO', 'days': 30},
    headers={'X-Organization': 'university_of_colorado'}
)

disasters = response.json()['data']
for disaster in disasters:
    print(f"Disaster {disaster['disaster_number']}: {disaster['title']}")
```

### JavaScript
```javascript
// Get current weather alerts
fetch('https://api.emergency-data.gov/v1/weather/alerts?state=CO&severity=Severe')
  .then(response => response.json())
  .then(data => {
    data.data.forEach(alert => {
      console.log(`${alert.event}: ${alert.headline}`);
    });
  });
```

### cURL
```bash
# Get agricultural weather data
curl -H "X-Organization: colorado_state_government" \
     "https://api.emergency-data.gov/v1/weather/agricultural?hours=24"
```

## Data Attribution

All API responses include proper attribution to data sources. When using this data, please include:

**Recommended Attribution:**
"Emergency data provided by the Emergency Management Data Pipeline (EMDP), aggregating data from FEMA, NOAA, CoAgMet, and USDA."

## Support and Documentation

- **API Documentation**: https://docs.emergency-data.gov
- **Status Page**: https://status.emergency-data.gov  
- **Usage Analytics**: https://analytics.emergency-data.gov
- **GitHub Repository**: https://github.com/emergency-management/data-pipeline
- **Issue Tracking**: https://github.com/emergency-management/data-pipeline/issues

## Terms of Service

- Data is provided as-is for informational purposes
- Users should verify critical information with authoritative sources
- Commercial use permitted with proper attribution
- Redistribution permitted with attribution
- No warranty expressed or implied
- Rate limits enforced to ensure fair access

## Future Enhancements

The pipeline is designed to support future sensitive data with proper tenant separation when needed, but currently focuses on maximizing access to public emergency information for community preparedness and research.