"""
Data Processing Operations for Emergency Management Pipeline
Operations for data transformation, enrichment, and aggregation
"""

import pandas as pd
import numpy as np
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

from ..public_resources import PublicDataStarRocksResource


class TransformationConfig(Config):
    """Configuration for data transformation operations"""
    transformation_type: str = Field(description="Type of transformation to apply")
    target_schema: Dict[str, str] = Field(default_factory=dict, description="Target schema mapping")
    aggregation_rules: Dict[str, Any] = Field(default_factory=dict, description="Aggregation rules")
    filters: Dict[str, Any] = Field(default_factory=dict, description="Data filters to apply")
    enrichment_sources: List[str] = Field(default_factory=list, description="External data sources for enrichment")


class AggregationConfig(Config):
    """Configuration for data aggregation"""
    group_by_fields: List[str] = Field(description="Fields to group by")
    aggregation_functions: Dict[str, str] = Field(description="Aggregation functions")
    time_window: str = Field(default="1H", description="Time window for aggregation")
    output_table: str = Field(description="Output table name")


@op(
    description="Transform raw data into standardized format",
    ins={"raw_data": In(pd.DataFrame)},
    out=Out(pd.DataFrame, description="Transformed data"),
    config_schema=TransformationConfig,
)
def transform_data_op(context: OpExecutionContext, raw_data: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw data into standardized emergency management format
    Handles data type conversions, field mapping, and standardization
    """
    logger = get_dagster_logger()
    config = context.op_config
    
    if raw_data.empty:
        logger.info("No data to transform")
        return pd.DataFrame()
    
    df = raw_data.copy()
    source_name = df['data_source'].iloc[0] if 'data_source' in df.columns else 'unknown'
    
    transformation_type = config.get("transformation_type", "standard")
    target_schema = config.get("target_schema", {})
    filters = config.get("filters", {})
    
    logger.info(f"Transforming {len(df)} records from {source_name} using {transformation_type} transformation")
    
    # Apply filters first
    for field, filter_value in filters.items():
        if field in df.columns:
            if isinstance(filter_value, dict):
                # Range filter
                if 'min' in filter_value:
                    df = df[df[field] >= filter_value['min']]
                if 'max' in filter_value:
                    df = df[df[field] <= filter_value['max']]
            elif isinstance(filter_value, list):
                # Include list
                df = df[df[field].isin(filter_value)]
            else:
                # Exact match
                df = df[df[field] == filter_value]
    
    # Apply source-specific transformations
    if source_name == 'FEMA_OpenFEMA':
        df = _transform_fema_data(df)
    elif source_name == 'NOAA_Weather_API':
        df = _transform_noaa_data(df)
    elif source_name == 'CoAgMet':
        df = _transform_coagmet_data(df)
    elif source_name == 'USDA_RMA':
        df = _transform_usda_data(df)
    
    # Apply target schema mapping
    if target_schema:
        df = df.rename(columns=target_schema)
    
    # Add transformation metadata
    df['transformation_timestamp'] = datetime.now()
    df['transformation_type'] = transformation_type
    
    logger.info(f"Transformed data: {len(df)} records after transformation")
    
    return df


def _transform_fema_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform FEMA disaster data to standard format"""
    
    # Standardize date fields
    date_fields = ['declarationDate', 'incidentBeginDate', 'incidentEndDate']
    for field in date_fields:
        if field in df.columns:
            df[field] = pd.to_datetime(df[field], errors='coerce')
    
    # Standardize state codes
    if 'state' in df.columns:
        df['state'] = df['state'].str.upper().str.strip()
    
    # Calculate disaster duration
    if 'incidentBeginDate' in df.columns and 'incidentEndDate' in df.columns:
        df['disaster_duration_days'] = (
            df['incidentEndDate'] - df['incidentBeginDate']
        ).dt.days
    
    # Standardize disaster types
    if 'incidentType' in df.columns:
        df['incident_type_standardized'] = df['incidentType'].str.upper()
    
    return df


def _transform_noaa_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform NOAA weather data to standard format"""
    
    # Standardize time fields
    time_fields = ['effective', 'expires', 'onset']
    for field in time_fields:
        if field in df.columns:
            df[field] = pd.to_datetime(df[field], errors='coerce')
    
    # Calculate alert duration
    if 'effective' in df.columns and 'expires' in df.columns:
        df['alert_duration_hours'] = (
            df['expires'] - df['effective']
        ).dt.total_seconds() / 3600
    
    # Standardize severity levels
    if 'severity' in df.columns:
        severity_mapping = {
            'Extreme': 4,
            'Severe': 3,
            'Moderate': 2,
            'Minor': 1
        }
        df['severity_score'] = df['severity'].map(severity_mapping).fillna(0)
    
    # Extract state from area description
    if 'areaDesc' in df.columns and 'state' not in df.columns:
        # Simple extraction - in practice would be more sophisticated
        df['state'] = df['areaDesc'].str.extract(r'([A-Z]{2})')
    
    return df


def _transform_coagmet_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform CoAgMet weather station data"""
    
    # Standardize timestamp
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # Convert temperature units if needed
    if 'temperature' in df.columns:
        df['temperature_celsius'] = pd.to_numeric(df['temperature'], errors='coerce')
        # Assume input is Celsius, convert to Fahrenheit for standardization
        df['temperature_fahrenheit'] = (df['temperature_celsius'] * 9/5) + 32
    
    # Standardize numeric fields
    numeric_fields = ['humidity', 'wind_speed', 'precipitation']
    for field in numeric_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce')
    
    # Add Colorado state identifier
    df['state'] = 'CO'
    
    return df


def _transform_usda_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform USDA agricultural data"""
    
    # Standardize monetary fields
    monetary_fields = ['premium_amount', 'liability_amount', 'indemnity_amount']
    for field in monetary_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce')
    
    # Standardize commodity names
    if 'commodity' in df.columns:
        df['commodity_standardized'] = df['commodity'].str.upper().str.strip()
    
    # Calculate loss ratio
    if 'indemnity_amount' in df.columns and 'premium_amount' in df.columns:
        df['loss_ratio'] = np.where(
            df['premium_amount'] > 0,
            df['indemnity_amount'] / df['premium_amount'],
            0
        )
    
    return df


@op(
    description="Aggregate data by time windows and geographic regions",
    ins={"transformed_data": In(pd.DataFrame)},
    out=Out(pd.DataFrame, description="Aggregated data"),
    config_schema=AggregationConfig,
)
def aggregate_data_op(context: OpExecutionContext, transformed_data: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate emergency management data for analytics and reporting
    Supports temporal and geographic aggregation
    """
    logger = get_dagster_logger()
    config = context.op_config
    
    if transformed_data.empty:
        logger.info("No data to aggregate")
        return pd.DataFrame()
    
    group_by_fields = config["group_by_fields"]
    aggregation_functions = config["aggregation_functions"]
    time_window = config.get("time_window", "1H")
    
    df = transformed_data.copy()
    
    # Add time-based grouping if requested
    if 'time_period' in group_by_fields:
        # Find the primary timestamp field
        timestamp_fields = ['timestamp', 'declarationDate', 'effective', 'ingestion_timestamp']
        timestamp_field = None
        
        for field in timestamp_fields:
            if field in df.columns:
                timestamp_field = field
                break
        
        if timestamp_field:
            df['time_period'] = df[timestamp_field].dt.floor(time_window)
        else:
            logger.warning("No timestamp field found for time-based aggregation")
            group_by_fields = [f for f in group_by_fields if f != 'time_period']
    
    # Perform aggregation
    if group_by_fields:
        # Filter group_by_fields to only include existing columns
        valid_group_fields = [field for field in group_by_fields if field in df.columns]
        
        if valid_group_fields:
            grouped = df.groupby(valid_group_fields)
            
            # Apply aggregation functions
            agg_dict = {}
            for column, func in aggregation_functions.items():
                if column in df.columns:
                    agg_dict[column] = func
            
            if agg_dict:
                aggregated = grouped.agg(agg_dict).reset_index()
                
                # Add aggregation metadata
                aggregated['aggregation_timestamp'] = datetime.now()
                aggregated['aggregation_window'] = time_window
                aggregated['record_count'] = grouped.size().values
                
                logger.info(f"Aggregated {len(df)} records into {len(aggregated)} groups")
                
                return aggregated
    
    logger.warning("No valid aggregation performed, returning original data")
    return df


@op(
    description="Enrich data with external reference information",
    ins={"base_data": In(pd.DataFrame)},
    out=Out(pd.DataFrame, description="Enriched data"),
)
def enrich_data_op(
    context: OpExecutionContext, 
    base_data: pd.DataFrame,
    starrocks: PublicDataStarRocksResource,
) -> pd.DataFrame:
    """
    Enrich base data with additional context from reference tables
    Adds geographic, demographic, and historical context
    """
    logger = get_dagster_logger()
    
    if base_data.empty:
        logger.info("No data to enrich")
        return pd.DataFrame()
    
    df = base_data.copy()
    
    # Enrich with state information
    if 'state' in df.columns:
        df = _enrich_with_state_info(df, starrocks)
    
    # Enrich with county information
    if 'county_code' in df.columns or 'designated_area' in df.columns:
        df = _enrich_with_county_info(df, starrocks)
    
    # Enrich with historical context
    df = _enrich_with_historical_context(df, starrocks)
    
    # Add enrichment metadata
    df['enrichment_timestamp'] = datetime.now()
    
    logger.info(f"Enriched {len(df)} records with additional context")
    
    return df


def _enrich_with_state_info(df: pd.DataFrame, starrocks: PublicDataStarRocksResource) -> pd.DataFrame:
    """Enrich data with state-level information"""
    
    # State reference data (in practice, this would come from a reference table)
    state_info = {
        'CO': {'state_name': 'Colorado', 'region': 'West', 'population': 5773714},
        'CA': {'state_name': 'California', 'region': 'West', 'population': 39538223},
        'TX': {'state_name': 'Texas', 'region': 'South', 'population': 29145505},
        'FL': {'state_name': 'Florida', 'region': 'South', 'population': 21538187},
        'NY': {'state_name': 'New York', 'region': 'Northeast', 'population': 20201249},
    }
    
    # Add state information
    for state_code, info in state_info.items():
        mask = df['state'] == state_code
        for field, value in info.items():
            df.loc[mask, field] = value
    
    return df


def _enrich_with_county_info(df: pd.DataFrame, starrocks: PublicDataStarRocksResource) -> pd.DataFrame:
    """Enrich data with county-level information"""
    
    # This would typically query a county reference table
    # For now, add placeholder enrichment
    df['county_enriched'] = True
    df['urban_rural_classification'] = 'mixed'  # Would be based on actual county data
    
    return df


def _enrich_with_historical_context(df: pd.DataFrame, starrocks: PublicDataStarRocksResource) -> pd.DataFrame:
    """Add historical context to current events"""
    
    # Add historical frequency metrics
    if 'incident_type_standardized' in df.columns and 'state' in df.columns:
        # This would query historical data in practice
        df['historical_frequency_state'] = 'medium'  # Placeholder
        df['historical_severity_comparison'] = 'normal'  # Placeholder
    
    return df


@op(
    description="Calculate emergency management metrics and indicators",
    ins={"processed_data": In(pd.DataFrame)},
    out=Out(Dict[str, Any], description="Calculated metrics"),
)
def calculate_metrics_op(context: OpExecutionContext, processed_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculate key emergency management metrics and performance indicators
    Provides insights for decision-making and reporting
    """
    logger = get_dagster_logger()
    
    if processed_data.empty:
        return {"metrics": {}, "record_count": 0}
    
    metrics = {}
    
    # Basic metrics
    metrics['total_records'] = len(processed_data)
    metrics['data_sources'] = processed_data['data_source'].nunique() if 'data_source' in processed_data.columns else 0
    
    # Time-based metrics
    if 'ingestion_timestamp' in processed_data.columns:
        latest_data = processed_data['ingestion_timestamp'].max()
        oldest_data = processed_data['ingestion_timestamp'].min()
        metrics['data_freshness_hours'] = (datetime.now() - latest_data).total_seconds() / 3600
        metrics['data_span_hours'] = (latest_data - oldest_data).total_seconds() / 3600
    
    # Geographic coverage
    if 'state' in processed_data.columns:
        metrics['states_covered'] = processed_data['state'].nunique()
        metrics['state_distribution'] = processed_data['state'].value_counts().to_dict()
    
    # Source-specific metrics
    source_name = processed_data['data_source'].iloc[0] if 'data_source' in processed_data.columns else 'unknown'
    
    if source_name == 'FEMA_OpenFEMA':
        metrics.update(_calculate_fema_metrics(processed_data))
    elif source_name == 'NOAA_Weather_API':
        metrics.update(_calculate_noaa_metrics(processed_data))
    elif source_name == 'CoAgMet':
        metrics.update(_calculate_coagmet_metrics(processed_data))
    elif source_name == 'USDA_RMA':
        metrics.update(_calculate_usda_metrics(processed_data))
    
    # Data quality metrics
    metrics['data_quality'] = {
        'completeness_percent': (1 - processed_data.isnull().sum().sum() / (len(processed_data) * len(processed_data.columns))) * 100,
        'duplicate_records': processed_data.duplicated().sum(),
        'null_values_by_column': processed_data.isnull().sum().to_dict()
    }
    
    logger.info(f"Calculated metrics for {len(processed_data)} records from {source_name}")
    
    return {
        "metrics": metrics,
        "calculation_timestamp": datetime.now().isoformat(),
        "record_count": len(processed_data),
        "source": source_name
    }


def _calculate_fema_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculate FEMA-specific metrics"""
    metrics = {}
    
    if 'incidentType' in df.columns:
        metrics['disaster_types'] = df['incidentType'].value_counts().to_dict()
        metrics['most_common_disaster'] = df['incidentType'].mode().iloc[0] if not df['incidentType'].mode().empty else None
    
    if 'disaster_duration_days' in df.columns:
        metrics['avg_disaster_duration_days'] = df['disaster_duration_days'].mean()
        metrics['max_disaster_duration_days'] = df['disaster_duration_days'].max()
    
    return metrics


def _calculate_noaa_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculate NOAA weather metrics"""
    metrics = {}
    
    if 'severity' in df.columns:
        metrics['severity_distribution'] = df['severity'].value_counts().to_dict()
    
    if 'alert_duration_hours' in df.columns:
        metrics['avg_alert_duration_hours'] = df['alert_duration_hours'].mean()
        metrics['active_alerts'] = (df['alert_duration_hours'] > 0).sum()
    
    return metrics


def _calculate_coagmet_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculate CoAgMet weather station metrics"""
    metrics = {}
    
    if 'temperature_celsius' in df.columns:
        metrics['avg_temperature_celsius'] = df['temperature_celsius'].mean()
        metrics['min_temperature_celsius'] = df['temperature_celsius'].min()
        metrics['max_temperature_celsius'] = df['temperature_celsius'].max()
    
    if 'station_id' in df.columns:
        metrics['stations_reporting'] = df['station_id'].nunique()
    
    return metrics


def _calculate_usda_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculate USDA agricultural metrics"""
    metrics = {}
    
    if 'loss_ratio' in df.columns:
        metrics['avg_loss_ratio'] = df['loss_ratio'].mean()
        metrics['high_loss_events'] = (df['loss_ratio'] > 1.0).sum()
    
    if 'commodity_standardized' in df.columns:
        metrics['commodities_covered'] = df['commodity_standardized'].nunique()
        metrics['commodity_distribution'] = df['commodity_standardized'].value_counts().to_dict()
    
    return metrics