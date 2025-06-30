"""
Assets module for Emergency Management Data Pipeline
Modular asset definitions supporting multiple data sources
"""

from .raw_data import (
    fema_disaster_data,
    noaa_weather_data,
    coagmet_data,
    usda_data,
    custom_source_data,
)

from .processed_data import (
    processed_disasters,
    processed_weather_alerts,
    processed_agricultural_data,
    unified_emergency_events,
    data_quality_metrics,
)

from .ml_assets import (
    disaster_prediction_features,
    weather_impact_model,
    agricultural_risk_scores,
    emergency_response_recommendations,
)

# Group assets by category for modular loading
raw_data_assets = [
    fema_disaster_data,
    noaa_weather_data,
    coagmet_data,
    usda_data,
    custom_source_data,
]

processed_data_assets = [
    processed_disasters,
    processed_weather_alerts,
    processed_agricultural_data,
    unified_emergency_events,
    data_quality_metrics,
]

ml_assets = [
    disaster_prediction_features,
    weather_impact_model,
    agricultural_risk_scores,
    emergency_response_recommendations,
]

# All assets for easy import
all_assets = [
    *raw_data_assets,
    *processed_data_assets, 
    *ml_assets,
]

__all__ = [
    # Individual assets
    "fema_disaster_data",
    "noaa_weather_data", 
    "coagmet_data",
    "usda_data",
    "custom_source_data",
    "processed_disasters",
    "processed_weather_alerts",
    "processed_agricultural_data",
    "unified_emergency_events",
    "data_quality_metrics",
    "disaster_prediction_features",
    "weather_impact_model",
    "agricultural_risk_scores",
    "emergency_response_recommendations",
    
    # Asset groups
    "raw_data_assets",
    "processed_data_assets",
    "ml_assets",
    "all_assets",
]