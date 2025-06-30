"""
Data Ingestion Operations for Emergency Management Pipeline
Modular operations for fetching data from various emergency management APIs
"""

import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from dagster import (
    op,
    OpExecutionContext,
    Config,
    get_dagster_logger,
    Out,
    In,
)
from pydantic import Field

from ..public_resources import PublicDataStarRocksResource, PublicDataKafkaResource


class APISourceConfig(Config):
    """Configuration for API data source ingestion"""
    source_name: str = Field(description="Name of the data source")
    api_endpoint: str = Field(description="API endpoint URL")
    api_key: str = Field(default="", description="API key if required")
    params: Dict[str, Any] = Field(default_factory=dict, description="Query parameters")
    headers: Dict[str, str] = Field(default_factory=dict, description="Request headers")
    timeout_seconds: int = Field(default=30, description="Request timeout")
    rate_limit_delay: float = Field(default=1.0, description="Delay between requests")
    batch_size: int = Field(default=1000, description="Batch size for processing")


class DataValidationConfig(Config):
    """Configuration for data validation"""
    required_fields: List[str] = Field(default_factory=list, description="Required fields")
    data_types: Dict[str, str] = Field(default_factory=dict, description="Expected data types")
    min_records: int = Field(default=1, description="Minimum number of records")
    max_records: int = Field(default=100000, description="Maximum number of records")
    allow_empty: bool = Field(default=False, description="Allow empty datasets")


@op(
    description="Fetch data from external API endpoints",
    out=Out(Dict[str, Any], description="Raw API response data"),
    config_schema=APISourceConfig,
)
def fetch_api_data_op(context: OpExecutionContext) -> Dict[str, Any]:
    """
    Generic operation to fetch data from any emergency management API
    Handles authentication, rate limiting, and error handling
    """
    logger = get_dagster_logger()
    config = context.op_config
    
    source_name = config["source_name"]
    api_endpoint = config["api_endpoint"]
    api_key = config.get("api_key", "")
    params = config.get("params", {})
    headers = config.get("headers", {})
    timeout = config.get("timeout_seconds", 30)
    
    # Set up authentication if API key provided
    if api_key:
        if "fema.gov" in api_endpoint.lower():
            headers["X-API-Key"] = api_key
        elif "weather.gov" in api_endpoint.lower():
            headers["User-Agent"] = f"EmergencyManagement/1.0 (+{api_key})"
        else:
            headers["Authorization"] = f"Bearer {api_key}"
    
    # Add default user agent if not specified
    if "User-Agent" not in headers:
        headers["User-Agent"] = "EmergencyManagement/1.0"
    
    try:
        logger.info(f"Fetching data from {source_name} at {api_endpoint}")
        
        response = requests.get(
            api_endpoint,
            params=params,
            headers=headers,
            timeout=timeout
        )
        response.raise_for_status()
        
        data = response.json()
        
        # Extract actual data from various API response formats
        if isinstance(data, dict):
            # Try common data container keys
            for key in ['data', 'results', 'items', 'records', 'features']:
                if key in data and isinstance(data[key], list):
                    data_records = data[key]
                    break
            else:
                # If no container found, use the whole response
                data_records = [data] if not isinstance(data, list) else data
        else:
            data_records = data if isinstance(data, list) else [data]
        
        result = {
            "source_name": source_name,
            "data": data_records,
            "metadata": {
                "api_endpoint": api_endpoint,
                "fetch_timestamp": datetime.now().isoformat(),
                "record_count": len(data_records),
                "response_size_bytes": len(response.content),
                "response_time_ms": response.elapsed.total_seconds() * 1000,
            }
        }
        
        logger.info(f"Successfully fetched {len(data_records)} records from {source_name}")
        
        return result
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from {source_name}: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON response from {source_name}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching data from {source_name}: {str(e)}")
        raise


@op(
    description="Validate raw data quality and structure",
    ins={"raw_data": In(Dict[str, Any])},
    out=Out(pd.DataFrame, description="Validated and cleaned data"),
    config_schema=DataValidationConfig,
)
def validate_raw_data_op(context: OpExecutionContext, raw_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Validate and clean raw data from API responses
    Ensures data meets quality standards before processing
    """
    logger = get_dagster_logger()
    config = context.op_config
    
    source_name = raw_data["source_name"]
    data_records = raw_data["data"]
    
    # Convert to DataFrame
    df = pd.DataFrame(data_records)
    
    if df.empty and not config.get("allow_empty", False):
        raise ValueError(f"No data received from {source_name}")
    
    # Check record count limits
    min_records = config.get("min_records", 1)
    max_records = config.get("max_records", 100000)
    
    if len(df) < min_records:
        raise ValueError(f"Too few records from {source_name}: {len(df)} < {min_records}")
    
    if len(df) > max_records:
        logger.warning(f"Large dataset from {source_name}: {len(df)} records, truncating to {max_records}")
        df = df.head(max_records)
    
    # Check required fields
    required_fields = config.get("required_fields", [])
    missing_fields = [field for field in required_fields if field not in df.columns]
    
    if missing_fields:
        logger.warning(f"Missing required fields from {source_name}: {missing_fields}")
        # Add missing fields with null values
        for field in missing_fields:
            df[field] = None
    
    # Apply data type conversions
    data_types = config.get("data_types", {})
    for column, dtype in data_types.items():
        if column in df.columns:
            try:
                if dtype == "datetime":
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                elif dtype == "numeric":
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                elif dtype == "string":
                    df[column] = df[column].astype(str)
            except Exception as e:
                logger.warning(f"Error converting {column} to {dtype}: {str(e)}")
    
    # Add standard metadata columns
    df['ingestion_timestamp'] = datetime.now()
    df['data_source'] = source_name
    df['data_classification'] = 'PUBLIC'  # Default for emergency data
    
    # Basic data cleaning
    df = df.dropna(how='all')  # Remove completely empty rows
    df = df.drop_duplicates()  # Remove exact duplicates
    
    logger.info(f"Validated {len(df)} records from {source_name}")
    
    return df


@op(
    description="Store raw data in StarRocks database",
    ins={"validated_data": In(pd.DataFrame)},
    out=Out(int, description="Number of records stored"),
)
def store_raw_data_op(
    context: OpExecutionContext, 
    validated_data: pd.DataFrame,
    starrocks: PublicDataStarRocksResource,
) -> int:
    """
    Store validated data in StarRocks for further processing
    Handles table creation and data insertion
    """
    logger = get_dagster_logger()
    
    if validated_data.empty:
        logger.info("No data to store")
        return 0
    
    # Determine table name from data source
    source_name = validated_data['data_source'].iloc[0]
    table_mapping = {
        'FEMA_OpenFEMA': 'fema_disaster_declarations',
        'NOAA_Weather_API': 'noaa_weather_alerts',
        'CoAgMet': 'coagmet_weather_data',
        'USDA_RMA': 'usda_agricultural_data',
    }
    
    table_name = table_mapping.get(source_name, f"raw_data_{source_name.lower()}")
    
    # Convert DataFrame to list of dictionaries for bulk insert
    records = validated_data.to_dict('records')
    
    try:
        rows_inserted = starrocks.bulk_insert_public_data(
            table=table_name,
            data=records,
            source_organization="system_ingestion"
        )
        
        logger.info(f"Stored {rows_inserted} records in table {table_name}")
        
        return rows_inserted
        
    except Exception as e:
        logger.error(f"Error storing data in {table_name}: {str(e)}")
        raise


@op(
    description="Send data to Kafka for real-time processing",
    ins={"validated_data": In(pd.DataFrame)},
    out=Out(int, description="Number of messages sent"),
)
def send_to_kafka_op(
    context: OpExecutionContext,
    validated_data: pd.DataFrame,
    kafka: PublicDataKafkaResource,
) -> int:
    """
    Send validated data to Kafka topics for real-time stream processing
    Partitions data by source and geographic region
    """
    logger = get_dagster_logger()
    
    if validated_data.empty:
        logger.info("No data to send to Kafka")
        return 0
    
    source_name = validated_data['data_source'].iloc[0]
    
    # Topic mapping based on data source
    topic_mapping = {
        'FEMA_OpenFEMA': 'fema_disasters',
        'NOAA_Weather_API': 'noaa_weather_alerts',
        'CoAgMet': 'coagmet_weather',
        'USDA_RMA': 'usda_agricultural_data',
    }
    
    topic = topic_mapping.get(source_name, f"emergency_data_{source_name.lower()}")
    
    try:
        producer = kafka.get_public_producer(organization="system_ingestion")
        
        messages_sent = 0
        
        for _, row in validated_data.iterrows():
            record = row.to_dict()
            
            # Create partition key based on geographic info
            partition_key = None
            if 'state' in record and record['state']:
                partition_key = str(record['state'])
            elif 'station_id' in record and record['station_id']:
                partition_key = str(record['station_id'])
            
            producer.send(
                topic,
                key=partition_key,
                value=record
            )
            messages_sent += 1
        
        producer.flush()
        producer.close()
        
        logger.info(f"Sent {messages_sent} messages to Kafka topic {topic}")
        
        return messages_sent
        
    except Exception as e:
        logger.error(f"Error sending data to Kafka: {str(e)}")
        raise


@op(
    description="Process microbatch data for Flink streaming",
    ins={"validated_data": In(pd.DataFrame)},
    out=Out(Dict[str, Any], description="Microbatch processing results"),
)
def process_microbatch_op(
    context: OpExecutionContext,
    validated_data: pd.DataFrame,
) -> Dict[str, Any]:
    """
    Process data in microbatches optimized for Flink streaming
    Prepares data for real-time analytics and alerting
    """
    logger = get_dagster_logger()
    
    if validated_data.empty:
        return {"batches_processed": 0, "total_records": 0}
    
    source_name = validated_data['data_source'].iloc[0]
    batch_size = 100  # Optimize for Flink microbatching
    
    # Split data into microbatches
    batches = []
    for i in range(0, len(validated_data), batch_size):
        batch = validated_data.iloc[i:i + batch_size]
        
        # Add batch metadata
        batch_info = {
            "batch_id": f"{source_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{i//batch_size}",
            "source": source_name,
            "batch_size": len(batch),
            "batch_timestamp": datetime.now().isoformat(),
            "records": batch.to_dict('records')
        }
        
        batches.append(batch_info)
    
    # In a real implementation, these batches would be sent to Flink
    # For now, we'll simulate the processing
    
    result = {
        "source_name": source_name,
        "batches_processed": len(batches),
        "total_records": len(validated_data),
        "microbatch_size": batch_size,
        "processing_timestamp": datetime.now().isoformat(),
        "batches": batches
    }
    
    logger.info(f"Processed {len(batches)} microbatches for {source_name}")
    
    return result