"""
Emergency Management Pipeline Assets
Comprehensive data assets for federal emergency data processing
"""

# Import all asset collections
from .emergency_scrapers import emergency_scraper_assets
from .raw_data import (
    fema_disaster_data,
    noaa_weather_data,
    coagmet_weather_data,
    usda_agricultural_data,
    custom_source_data,
)
from .processed_data import (
    processed_disasters,
    processed_weather_alerts,
    processed_agricultural_data,
    unified_emergency_data_and_metrics,
)
from .public_assets import public_api_assets
from .ml_assets import (
    disaster_prediction_features,
    disaster_risk_model,
    weather_impact_model,
    model_performance_metrics,
)

# Group raw data assets
raw_data_assets = [
    fema_disaster_data,
    noaa_weather_data,
    coagmet_weather_data,
    usda_agricultural_data,
    custom_source_data,
]

# Group processed data assets
processed_data_assets = [
    processed_disasters,
    processed_weather_alerts,
    processed_agricultural_data,
    unified_emergency_data_and_metrics,
]

# Group ML assets
ml_pipeline_assets = [
    disaster_prediction_features,
    disaster_risk_model,
    weather_impact_model,
    model_performance_metrics,
]

# All assets for easy import
all_emergency_assets = [
    *emergency_scraper_assets,    # New comprehensive scrapers
    *raw_data_assets,            # Existing raw data ingestion
    *processed_data_assets,      # Data transformations
    *public_api_assets,          # Public API optimized views
    *ml_pipeline_assets,         # Machine learning pipeline
]

# Export everything for main definitions
__all__ = [
    # Asset collections
    "emergency_scraper_assets",
    "raw_data_assets", 
    "processed_data_assets",
    "public_api_assets",
    "ml_pipeline_assets",
    "all_emergency_assets",
    
    # Individual raw data assets
    "fema_disaster_data",
    "noaa_weather_data", 
    "coagmet_weather_data",
    "usda_agricultural_data",
    "custom_source_data",
    
    # Individual processed data assets
    "processed_disasters",
    "processed_weather_alerts",
    "processed_agricultural_data",
    "unified_emergency_data_and_metrics",
    
    # Individual ML assets
    "disaster_prediction_features",
    "disaster_risk_model",
    "weather_impact_model",
    "model_performance_metrics",
]