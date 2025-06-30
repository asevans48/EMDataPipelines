"""
Processed Data Assets - Transform raw data into analysis-ready formats
Includes data quality validation and business logic transformations
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List

from dagster import (
    asset,
    AssetIn,
    MetadataValue,
    get_dagster_logger,
    Config,
    multi_asset,
    AssetOut,
)
from pydantic import Field

from ..resources import StarRocksResource, KafkaResource, FlinkResource


@asset(
    ins={
        "fema_disasters": AssetIn("fema_disaster_data"),
    },
    description="Processed FEMA disaster data with enhanced analytics",
    group_name="processed_data",
    compute_kind="data_transformation",
)
def processed_disasters(
    context,
    fema_disasters: pd.DataFrame,
    starrocks: StarRocksResource,
) -> pd.DataFrame:
    """
    Transform raw FEMA disaster data into analysis-ready format
    Adds geographic, temporal, and impact analysis
    """
    logger = get_dagster_logger()
    
    if fema_disasters.empty:
        logger.warning("No FEMA disaster data to process")
        return pd.DataFrame()
    
    # Enhanced processing
    df = fema_disasters.copy()
    
    # Standardize temporal data
    df['declaration_date'] = pd.to_datetime(df['declarationDate'])
    df['incident_begin_date'] = pd.to_datetime(df['incidentBeginDate']) 
    df['incident_end_date'] = pd.to_datetime(df['incidentEndDate'])
    
    # Calculate federal fiscal year
    df['federal_fiscal_year'] = df['declaration_date'].apply(
        lambda x: x.year + 1 if x.month >= 10 else x.year
    )
    
    # Add geographic regions
    regional_mapping = {
        'CO': 'WEST_CENTRAL', 'WY': 'WEST_CENTRAL', 'MT': 'WEST_CENTRAL',
        'ND': 'WEST_CENTRAL', 'SD': 'WEST_CENTRAL', 'NE': 'WEST_CENTRAL',
        'KS': 'WEST_CENTRAL', 'OK': 'WEST_CENTRAL', 'TX': 'WEST_CENTRAL', 'NM': 'WEST_CENTRAL',
        'WA': 'WEST', 'OR': 'WEST', 'CA': 'WEST', 'NV': 'WEST', 'ID': 'WEST', 
        'UT': 'WEST', 'AZ': 'WEST', 'AK': 'WEST', 'HI': 'WEST'
    }
    df['geographic_region'] = df['state'].map(regional_mapping).fillna('OTHER')
    
    # Severity scoring based on incident type
    severity_mapping = {
        'Hurricane': 4, 'Major Disaster': 4, 'Tornado': 3, 'Flood': 3,
        'Fire': 3, 'Severe Storm': 2, 'Winter Storm': 2, 'Drought': 2,
        'Earthquake': 4, 'Volcanic Eruption': 4, 'Tsunami': 4
    }
    df['severity_score'] = df['incidentType'].map(severity_mapping).fillna(1)
    
    # Calculate impact duration
    df['impact_duration_days'] = (
        df['incident_end_date'] - df['incident_begin_date']
    ).dt.days
    
    # Seasonal analysis
    df['season'] = df['declaration_date'].dt.month.map({
        12: 'WINTER', 1: 'WINTER', 2: 'WINTER',
        3: 'SPRING', 4: 'SPRING', 5: 'SPRING',
        6: 'SUMMER', 7: 'SUMMER', 8: 'SUMMER',
        9: 'FALL', 10: 'FALL', 11: 'FALL'
    })
    
    # Data quality flags
    df['data_completeness_score'] = (
        df[['disasterNumber', 'state', 'declarationType', 'incidentType']].notna().sum(axis=1) / 4 * 100
    )
    
    # Federal compliance metadata
    df['data_classification'] = 'PUBLIC'
    df['processing_timestamp'] = datetime.now()
    df['retention_date'] = datetime.now() + timedelta(days=2555)  # 7 years
    
    # Store processed data
    if len(df) > 0:
        starrocks.bulk_insert('processed_fema_disasters', df.to_dict('records'))
    
    logger.info(f"Processed {len(df)} FEMA disaster records")
    
    context.add_output_metadata({
        "records_processed": len(df),
        "avg_severity_score": MetadataValue.float(df['severity_score'].mean()),
        "geographic_regions": len(df['geographic_region'].unique()),
        "processing_time": datetime.now().isoformat(),
        "data_quality_avg": MetadataValue.float(df['data_completeness_score'].mean()),
    })
    
    return df


@asset(
    ins={
        "noaa_weather": AssetIn("noaa_weather_data"),
    },
    description="Processed NOAA weather alerts with impact analysis",
    group_name="processed_data",
    compute_kind="data_transformation",
)
def processed_weather_alerts(
    context,
    noaa_weather: pd.DataFrame,
    starrocks: StarRocksResource,
) -> pd.DataFrame:
    """
    Transform raw NOAA weather alerts into analysis-ready format
    Adds priority scoring and geographic analysis
    """
    logger = get_dagster_logger()
    
    if noaa_weather.empty:
        logger.warning("No NOAA weather data to process")
        return pd.DataFrame()
    
    df = noaa_weather.copy()
    
    # Standardize temporal data
    df['effective_time'] = pd.to_datetime(df['effective'])
    df['expires_time'] = pd.to_datetime(df['expires'])
    
    # Priority scoring for emergency management
    severity_scores = {'Extreme': 4, 'Severe': 3, 'Moderate': 2, 'Minor': 1}
    urgency_scores = {'Immediate': 4, 'Expected': 3, 'Future': 2, 'Past': 1}
    certainty_scores = {'Observed': 4, 'Likely': 3, 'Possible': 2, 'Unlikely': 1}
    
    df['severity_score'] = df['severity'].map(severity_scores).fillna(0)
    df['urgency_score'] = df['urgency'].map(urgency_scores).fillna(0)
    df['certainty_score'] = df['certainty'].map(certainty_scores).fillna(0)
    
    # Composite priority score
    df['priority_score'] = (
        df['severity_score'] * 0.5 + 
        df['urgency_score'] * 0.3 + 
        df['certainty_score'] * 0.2
    )
    
    # Alert status
    current_time = datetime.now()
    df['alert_status'] = df.apply(lambda row: 
        'ACTIVE' if pd.notna(row['expires_time']) and row['expires_time'] > current_time 
        else 'EXPIRED' if pd.notna(row['expires_time'])
        else 'ONGOING', axis=1
    )
    
    # Extract primary state from area description
    df['primary_state'] = df['area_desc'].str.extract(r'\b([A-Z]{2})\b').iloc[:, 0]
    
    # Calculate alert duration
    df['alert_duration_hours'] = (
        df['expires_time'] - df['effective_time']
    ).dt.total_seconds() / 3600
    
    # Public safety categorization
    df['public_safety_level'] = df.apply(lambda row:
        'CRITICAL' if row['severity'] == 'Extreme' and row['urgency'] == 'Immediate'
        else 'HIGH' if row['severity'] in ['Extreme', 'Severe'] and row['urgency'] in ['Immediate', 'Expected']
        else 'MEDIUM' if row['severity'] in ['Severe', 'Moderate']
        else 'LOW', axis=1
    )
    
    # Federal compliance metadata
    df['data_classification'] = 'PUBLIC'
    df['processing_timestamp'] = datetime.now()
    
    # Store processed data
    if len(df) > 0:
        starrocks.bulk_insert('processed_weather_alerts', df.to_dict('records'))
    
    logger.info(f"Processed {len(df)} weather alerts")
    
    context.add_output_metadata({
        "records_processed": len(df),
        "active_alerts": len(df[df['alert_status'] == 'ACTIVE']),
        "critical_alerts": len(df[df['public_safety_level'] == 'CRITICAL']),
        "avg_priority_score": MetadataValue.float(df['priority_score'].mean()),
        "states_affected": len(df['primary_state'].dropna().unique()),
    })
    
    return df


@asset(
    ins={
        "coagmet_data": AssetIn("coagmet_data"),
        "usda_data": AssetIn("usda_data"),
    },
    description="Processed agricultural data combining CoAgMet and USDA sources",
    group_name="processed_data", 
    compute_kind="data_transformation",
)
def processed_agricultural_data(
    context,
    coagmet_data: pd.DataFrame,
    usda_data: pd.DataFrame,
    starrocks: StarRocksResource,
) -> pd.DataFrame:
    """
    Combine and process agricultural data from multiple sources
    Creates unified agricultural monitoring dataset
    """
    logger = get_dagster_logger()
    
    processed_records = []
    
    # Process CoAgMet weather station data
    if not coagmet_data.empty:
        coagmet_processed = coagmet_data.copy()
        coagmet_processed['data_type'] = 'WEATHER_STATION'
        coagmet_processed['processing_timestamp'] = datetime.now()
        processed_records.extend(coagmet_processed.to_dict('records'))
    
    # Process USDA agricultural data  
    if not usda_data.empty:
        usda_processed = usda_data.copy()
        usda_processed['data_type'] = 'AGRICULTURAL_PROGRAM'
        usda_processed['processing_timestamp'] = datetime.now()
        processed_records.extend(usda_processed.to_dict('records'))
    
    if not processed_records:
        logger.warning("No agricultural data to process")
        return pd.DataFrame()
    
    # Combine all agricultural data
    df = pd.DataFrame(processed_records)
    
    # Add federal compliance metadata
    df['data_classification'] = 'PUBLIC'
    df['retention_date'] = datetime.now() + timedelta(days=1095)  # 3 years for ag data
    
    # Store processed data
    starrocks.bulk_insert('processed_agricultural_data', processed_records)
    
    logger.info(f"Processed {len(df)} agricultural records")
    
    context.add_output_metadata({
        "records_processed": len(df),
        "coagmet_records": len(coagmet_data) if not coagmet_data.empty else 0,
        "usda_records": len(usda_data) if not usda_data.empty else 0,
        "processing_time": datetime.now().isoformat(),
    })
    
    return df


@multi_asset(
    outs={
        "unified_emergency_events": AssetOut(
            description="Unified view of all emergency events",
            group_name="processed_data"
        ),
        "data_quality_metrics": AssetOut(
            description="Data quality metrics across all sources",
            group_name="monitoring"
        ),
    },
    ins={
        "processed_disasters": AssetIn("processed_disasters"),
        "processed_weather": AssetIn("processed_weather_alerts"),
        "processed_agricultural": AssetIn("processed_agricultural_data"),
    },
    compute_kind="data_aggregation",
)
def unified_emergency_data_and_metrics(
    context,
    processed_disasters: pd.DataFrame,
    processed_weather: pd.DataFrame,
    processed_agricultural: pd.DataFrame,
    starrocks: StarRocksResource,
) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Create unified emergency events view and calculate data quality metrics
    """
    logger = get_dagster_logger()
    
    # Create unified emergency events
    unified_events = []
    
    # Add disaster events
    if not processed_disasters.empty:
        disaster_events = processed_disasters.copy()
        disaster_events['event_source'] = 'FEMA_DISASTER'
        disaster_events['event_id'] = disaster_events['disasterNumber']
        disaster_events['event_date'] = disaster_events['declaration_date']
        disaster_events['event_type'] = disaster_events['incidentType']
        unified_events.append(disaster_events[['event_id', 'event_source', 'event_date', 'event_type', 'severity_score']])
    
    # Add weather events
    if not processed_weather.empty:
        weather_events = processed_weather.copy()
        weather_events['event_source'] = 'NOAA_WEATHER'
        weather_events['event_id'] = weather_events['alert_id']
        weather_events['event_date'] = weather_events['effective_time']
        weather_events['event_type'] = weather_events['event']
        unified_events.append(weather_events[['event_id', 'event_source', 'event_date', 'event_type', 'severity_score']])
    
    # Combine events
    if unified_events:
        unified_df = pd.concat(unified_events, ignore_index=True)
        unified_df['processing_timestamp'] = datetime.now()
        unified_df['data_classification'] = 'PUBLIC'
        
        # Store unified events
        starrocks.bulk_insert('unified_emergency_events', unified_df.to_dict('records'))
    else:
        unified_df = pd.DataFrame()
    
    # Calculate data quality metrics
    quality_metrics = {
        "timestamp": datetime.now().isoformat(),
        "sources_processed": 0,
        "total_events": len(unified_df) if not unified_df.empty else 0,
        "data_sources": {},
    }
    
    # FEMA data quality
    if not processed_disasters.empty:
        quality_metrics["sources_processed"] += 1
        quality_metrics["data_sources"]["fema"] = {
            "record_count": len(processed_disasters),
            "completeness_avg": processed_disasters['data_completeness_score'].mean(),
            "latest_event": processed_disasters['declaration_date'].max().isoformat() if pd.notna(processed_disasters['declaration_date'].max()) else None,
            "data_freshness_hours": (datetime.now() - processed_disasters['ingestion_timestamp'].max()).total_seconds() / 3600,
            "quality_status": "GOOD" if processed_disasters['data_completeness_score'].mean() > 90 else "POOR"
        }
    
    # NOAA data quality
    if not processed_weather.empty:
        quality_metrics["sources_processed"] += 1
        quality_metrics["data_sources"]["noaa"] = {
            "record_count": len(processed_weather),
            "active_alerts": len(processed_weather[processed_weather['alert_status'] == 'ACTIVE']),
            "critical_alerts": len(processed_weather[processed_weather['public_safety_level'] == 'CRITICAL']),
            "latest_alert": processed_weather['effective_time'].max().isoformat() if pd.notna(processed_weather['effective_time'].max()) else None,
            "data_freshness_hours": (datetime.now() - processed_weather['ingestion_timestamp'].max()).total_seconds() / 3600,
            "quality_status": "GOOD"
        }
    
    # Agricultural data quality
    if not processed_agricultural.empty:
        quality_metrics["sources_processed"] += 1
        quality_metrics["data_sources"]["agricultural"] = {
            "record_count": len(processed_agricultural),
            "data_types": processed_agricultural['data_type'].unique().tolist(),
            "latest_update": processed_agricultural['processing_timestamp'].max().isoformat(),
            "quality_status": "GOOD"
        }
    
    # Overall quality assessment
    quality_metrics["overall_status"] = "HEALTHY" if quality_metrics["sources_processed"] >= 2 else "DEGRADED"
    
    logger.info(f"Created unified view with {len(unified_df)} events from {quality_metrics['sources_processed']} sources")
    
    # Add metadata
    context.add_output_metadata({
        "unified_emergency_events": {
            "total_events": len(unified_df),
            "data_sources": quality_metrics["sources_processed"],
            "processing_time": datetime.now().isoformat(),
        },
        "data_quality_metrics": {
            "overall_status": quality_metrics["overall_status"],
            "sources_healthy": quality_metrics["sources_processed"],
            "last_updated": quality_metrics["timestamp"],
        }
    })
    
    return unified_df, quality_metrics