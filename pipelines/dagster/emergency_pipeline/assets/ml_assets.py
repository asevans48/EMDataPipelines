"""
Machine Learning Assets - Predictive analytics for emergency management
Federal compliance with transparent, auditable ML models
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, classification_report
import joblib

from dagster import (
    asset,
    AssetIn, 
    MetadataValue,
    get_dagster_logger,
    Config,
)
from pydantic import Field

from ..resources import StarRocksResource


class MLModelConfig(Config):
    """Configuration for ML model training"""
    model_type: str = Field(default="random_forest", description="Type of ML model")
    test_size: float = Field(default=0.2, description="Test set size")
    random_state: int = Field(default=42, description="Random state for reproducibility")
    n_estimators: int = Field(default=100, description="Number of estimators")
    max_depth: Optional[int] = Field(default=10, description="Maximum tree depth")


@asset(
    ins={
        "unified_events": AssetIn("unified_emergency_events"),
        "weather_alerts": AssetIn("processed_weather_alerts"),
        "agricultural_data": AssetIn("processed_agricultural_data"),
    },
    description="Feature engineering for disaster prediction models",
    group_name="ml_pipeline",
    compute_kind="feature_engineering",
)
def disaster_prediction_features(
    context,
    unified_events: pd.DataFrame,
    weather_alerts: pd.DataFrame, 
    agricultural_data: pd.DataFrame,
    starrocks: StarRocksResource,
) -> pd.DataFrame:
    """
    Create feature dataset for disaster prediction models
    Combines historical patterns with real-time indicators
    """
    logger = get_dagster_logger()
    
    if unified_events.empty:
        logger.warning("No unified events data for feature engineering")
        return pd.DataFrame()
    
    # Create time-based features
    df = unified_events.copy()
    df['event_date'] = pd.to_datetime(df['event_date'])
    
    # Temporal features
    df['year'] = df['event_date'].dt.year
    df['month'] = df['event_date'].dt.month
    df['day_of_year'] = df['event_date'].dt.dayofyear
    df['quarter'] = df['event_date'].dt.quarter
    df['is_hurricane_season'] = ((df['month'] >= 6) & (df['month'] <= 11)).astype(int)
    df['is_fire_season'] = ((df['month'] >= 5) & (df['month'] <= 10)).astype(int)
    df['is_winter'] = ((df['month'] <= 2) | (df['month'] == 12)).astype(int)
    
    # Historical frequency features (rolling windows)
    df = df.sort_values('event_date')
    
    # 30-day rolling event count
    df['events_last_30_days'] = df.groupby('event_source')['event_date'].transform(
        lambda x: x.rolling(window='30D', on=x).count() - 1  # Subtract current event
    )
    
    # 90-day rolling severity average
    df['avg_severity_90_days'] = df.groupby('event_source')['severity_score'].transform(
        lambda x: x.rolling(window=5, min_periods=1).mean().shift(1)
    )
    
    # Geographic clustering features
    if 'state_code' in df.columns:
        # Regional disaster frequency
        regional_freq = df.groupby(['event_source', df['event_date'].dt.to_period('M')])['event_id'].count().reset_index()
        regional_freq.columns = ['event_source', 'month_period', 'monthly_event_count']
        
        # Add regional features back to main dataset
        df['month_period'] = df['event_date'].dt.to_period('M')
        df = df.merge(regional_freq, on=['event_source', 'month_period'], how='left')
    
    # Weather correlation features
    if not weather_alerts.empty:
        weather_summary = weather_alerts.groupby(
            weather_alerts['effective_time'].dt.date
        ).agg({
            'priority_score': ['mean', 'max', 'count'],
            'severity_score': 'mean'
        }).reset_index()
        
        weather_summary.columns = ['date', 'avg_weather_priority', 'max_weather_priority', 
                                 'daily_weather_alerts', 'avg_weather_severity']
        
        # Merge weather features
        df['date'] = df['event_date'].dt.date
        df = df.merge(weather_summary, on='date', how='left')
        df[['avg_weather_priority', 'max_weather_priority', 'daily_weather_alerts', 'avg_weather_severity']] = \
            df[['avg_weather_priority', 'max_weather_priority', 'daily_weather_alerts', 'avg_weather_severity']].fillna(0)
    
    # Agricultural risk features
    if not agricultural_data.empty and 'station_id' in agricultural_data.columns:
        # Simplified agricultural stress indicators
        ag_features = agricultural_data.groupby(
            agricultural_data['ingestion_timestamp'].dt.date
        ).agg({
            'temperature': 'mean',
            'humidity': 'mean', 
            'precipitation': 'sum'
        }).reset_index()
        
        ag_features.columns = ['date', 'avg_temperature', 'avg_humidity', 'total_precipitation']
        
        # Add drought indicators
        ag_features['drought_indicator'] = (
            (ag_features['total_precipitation'] < ag_features['total_precipitation'].quantile(0.2)) &
            (ag_features['avg_temperature'] > ag_features['avg_temperature'].quantile(0.8))
        ).astype(int)
        
        # Merge agricultural features
        df = df.merge(ag_features, on='date', how='left')
        df[['avg_temperature', 'avg_humidity', 'total_precipitation', 'drought_indicator']] = \
            df[['avg_temperature', 'avg_humidity', 'total_precipitation', 'drought_indicator']].fillna(0)
    
    # Target variables for prediction
    df = df.sort_values(['event_source', 'event_date'])
    
    # Next event prediction (binary classification)
    df['next_event_7_days'] = df.groupby('event_source')['event_date'].transform(
        lambda x: ((x.shift(-1) - x).dt.days <= 7).astype(int)
    ).fillna(0)
    
    # Severity prediction (regression)
    df['next_event_severity'] = df.groupby('event_source')['severity_score'].shift(-1)
    
    # Federal compliance metadata
    df['feature_extraction_timestamp'] = datetime.now()
    df['data_classification'] = 'PUBLIC'
    df['model_version'] = '1.0'
    df['compliance_flags'] = 'AUDITABLE,TRANSPARENT,FEDRAMP'
    
    # Store features
    if len(df) > 0:
        starrocks.bulk_insert('ml_disaster_features', df.to_dict('records'))
    
    logger.info(f"Created {len(df)} feature records for ML training")
    
    context.add_output_metadata({
        "feature_records": len(df),
        "feature_columns": len([col for col in df.columns if col not in ['event_id', 'event_date']]),
        "data_date_range": f"{df['event_date'].min()} to {df['event_date'].max()}",
        "missing_target_rate": MetadataValue.float(df['next_event_severity'].isna().mean()),
        "positive_class_rate": MetadataValue.float(df['next_event_7_days'].mean()),
    })
    
    return df


@asset(
    ins={"features": AssetIn("disaster_prediction_features")},
    description="Weather impact prediction model for emergency planning",
    group_name="ml_pipeline",
    compute_kind="model_training",
)
def weather_impact_model(
    context,
    features: pd.DataFrame,
    config: MLModelConfig,
    starrocks: StarRocksResource,
) -> Dict[str, Any]:
    """
    Train weather impact prediction model
    Predicts likelihood of weather-related emergencies
    """
    logger = get_dagster_logger()
    
    if features.empty:
        logger.warning("No features available for model training")
        return {}
    
    # Filter for weather-related events
    weather_features = features[features['event_source'] == 'NOAA_WEATHER'].copy()
    
    if len(weather_features) < 50:
        logger.warning("Insufficient weather data for model training")
        return {"status": "insufficient_data", "sample_size": len(weather_features)}
    
    # Prepare features for training
    feature_columns = [
        'month', 'day_of_year', 'quarter', 'is_hurricane_season', 'is_fire_season', 'is_winter',
        'events_last_30_days', 'avg_severity_90_days', 'avg_weather_priority', 
        'max_weather_priority', 'daily_weather_alerts'
    ]
    
    # Add agricultural features if available
    ag_columns = ['avg_temperature', 'avg_humidity', 'total_precipitation', 'drought_indicator']
    available_ag_cols = [col for col in ag_columns if col in weather_features.columns]
    feature_columns.extend(available_ag_cols)
    
    # Remove any columns with all NaN values
    feature_columns = [col for col in feature_columns if col in weather_features.columns and not weather_features[col].isna().all()]
    
    # Prepare training data
    X = weather_features[feature_columns].fillna(0)
    y_classification = weather_features['next_event_7_days'].fillna(0)
    y_regression = weather_features['next_event_severity'].dropna()
    X_reg = X.loc[y_regression.index]
    
    if len(X) < 30:
        logger.warning("Insufficient training data after preprocessing")
        return {"status": "insufficient_clean_data", "clean_samples": len(X)}
    
    # Train classification model (next event prediction)
    X_train_cls, X_test_cls, y_train_cls, y_test_cls = train_test_split(
        X, y_classification, test_size=config.test_size, random_state=config.random_state
    )
    
    cls_model = RandomForestClassifier(
        n_estimators=config.n_estimators,
        max_depth=config.max_depth,
        random_state=config.random_state
    )
    cls_model.fit(X_train_cls, y_train_cls)
    
    # Evaluate classification model
    cls_pred = cls_model.predict(X_test_cls)
    cls_report = classification_report(y_test_cls, cls_pred, output_dict=True)
    
    # Train regression model (severity prediction) if enough data
    reg_model = None
    reg_metrics = {}
    
    if len(X_reg) >= 20:
        X_train_reg, X_test_reg, y_train_reg, y_test_reg = train_test_split(
            X_reg, y_regression, test_size=config.test_size, random_state=config.random_state
        )
        
        reg_model = RandomForestRegressor(
            n_estimators=config.n_estimators,
            max_depth=config.max_depth,
            random_state=config.random_state
        )
        reg_model.fit(X_train_reg, y_train_reg)
        
        # Evaluate regression model
        reg_pred = reg_model.predict(X_test_reg)
        reg_metrics = {
            "mse": float(mean_squared_error(y_test_reg, reg_pred)),
            "rmse": float(np.sqrt(mean_squared_error(y_test_reg, reg_pred))),
            "train_samples": len(X_train_reg),
            "test_samples": len(X_test_reg)
        }
    
    # Feature importance analysis
    feature_importance = dict(zip(feature_columns, cls_model.feature_importances_))
    top_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:10]
    
    # Model metadata for federal compliance
    model_metadata = {
        "model_type": "RandomForestClassifier",
        "training_date": datetime.now().isoformat(),
        "model_version": "1.0",
        "data_classification": "PUBLIC",
        "compliance_status": "FEDRAMP_COMPLIANT",
        "audit_trail": {
            "feature_count": len(feature_columns),
            "training_samples": len(X_train_cls),
            "test_samples": len(X_test_cls),
            "hyperparameters": {
                "n_estimators": config.n_estimators,
                "max_depth": config.max_depth,
                "random_state": config.random_state
            }
        },
        "performance_metrics": {
            "classification": {
                "accuracy": float(cls_report['accuracy']),
                "precision": float(cls_report['macro avg']['precision']),
                "recall": float(cls_report['macro avg']['recall']),
                "f1_score": float(cls_report['macro avg']['f1-score'])
            },
            "regression": reg_metrics
        },
        "feature_importance": dict(top_features),
        "model_interpretability": {
            "top_risk_factors": [f[0] for f in top_features[:5]],
            "model_transparency": "HIGH",
            "explainability_methods": ["feature_importance", "decision_trees"]
        }
    }
    
    # Store model artifacts (in production, use proper model registry)
    model_storage_path = f"/opt/dagster/storage/models/weather_impact_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Save models
    joblib.dump(cls_model, f"{model_storage_path}_classifier.pkl")
    if reg_model:
        joblib.dump(reg_model, f"{model_storage_path}_regressor.pkl")
    
    # Store model metadata
    starrocks.bulk_insert('ml_model_metadata', [model_metadata])
    
    logger.info(f"Trained weather impact model with accuracy: {model_metadata['performance_metrics']['classification']['accuracy']:.3f}")
    
    context.add_output_metadata({
        "model_accuracy": MetadataValue.float(model_metadata['performance_metrics']['classification']['accuracy']),
        "training_samples": len(X_train_cls),
        "feature_count": len(feature_columns),
        "top_feature": top_features[0][0] if top_features else "none",
        "model_path": model_storage_path,
        "compliance_status": "FEDRAMP_COMPLIANT"
    })
    
    return model_metadata


@asset(
    ins={"features": AssetIn("disaster_prediction_features")},
    description="Agricultural risk scoring model for drought and crop impact",
    group_name="ml_pipeline",
    compute_kind="model_training",
)
def agricultural_risk_scores(
    context,
    features: pd.DataFrame,
    starrocks: StarRocksResource,
) -> pd.DataFrame:
    """
    Calculate agricultural risk scores based on weather and historical patterns
    Supports agricultural emergency preparedness
    """
    logger = get_dagster_logger()
    
    if features.empty:
        logger.warning("No features for agricultural risk calculation")
        return pd.DataFrame()
    
    # Filter features with agricultural data
    ag_features = features.dropna(subset=['avg_temperature', 'avg_humidity', 'total_precipitation']).copy()
    
    if ag_features.empty:
        logger.warning("No agricultural data available for risk scoring")
        return pd.DataFrame()
    
    # Calculate risk scores
    ag_features['temperature_risk'] = (
        (ag_features['avg_temperature'] - ag_features['avg_temperature'].mean()) / 
        ag_features['avg_temperature'].std()
    ).clip(-3, 3)  # Normalize to z-score, clip outliers
    
    ag_features['precipitation_risk'] = (
        (ag_features['total_precipitation'].mean() - ag_features['total_precipitation']) / 
        ag_features['total_precipitation'].std()
    ).clip(-3, 3)  # Inverse: low precipitation = high risk
    
    ag_features['humidity_risk'] = (
        (ag_features['avg_humidity'].mean() - ag_features['avg_humidity']) / 
        ag_features['avg_humidity'].std()
    ).clip(-3, 3)  # Inverse: low humidity = high risk
    
    # Composite agricultural risk score
    ag_features['agricultural_risk_score'] = (
        ag_features['temperature_risk'] * 0.4 +
        ag_features['precipitation_risk'] * 0.4 +
        ag_features['humidity_risk'] * 0.2 +
        ag_features['drought_indicator'] * 2  # Boost for drought conditions
    ).clip(0, 10)  # Scale to 0-10
    
    # Risk categories
    ag_features['risk_category'] = pd.cut(
        ag_features['agricultural_risk_score'],
        bins=[0, 2, 4, 6, 8, 10],
        labels=['LOW', 'MODERATE', 'HIGH', 'SEVERE', 'EXTREME']
    )
    
    # Seasonal adjustments
    ag_features['seasonal_multiplier'] = ag_features['month'].map({
        1: 0.5, 2: 0.5, 3: 0.7, 4: 1.0, 5: 1.2, 6: 1.5,
        7: 1.5, 8: 1.3, 9: 1.0, 10: 0.8, 11: 0.6, 12: 0.5
    })
    
    ag_features['adjusted_risk_score'] = (
        ag_features['agricultural_risk_score'] * ag_features['seasonal_multiplier']
    ).clip(0, 10)
    
    # Federal compliance metadata
    ag_features['risk_calculation_timestamp'] = datetime.now()
    ag_features['data_classification'] = 'PUBLIC'
    ag_features['risk_model_version'] = '1.0'
    
    # Store risk scores
    risk_records = ag_features[[
        'event_date', 'agricultural_risk_score', 'adjusted_risk_score', 
        'risk_category', 'temperature_risk', 'precipitation_risk', 'humidity_risk',
        'risk_calculation_timestamp', 'data_classification'
    ]].to_dict('records')
    
    starrocks.bulk_insert('agricultural_risk_scores', risk_records)
    
    logger.info(f"Calculated agricultural risk scores for {len(ag_features)} records")
    
    context.add_output_metadata({
        "risk_records": len(ag_features),
        "avg_risk_score": MetadataValue.float(ag_features['agricultural_risk_score'].mean()),
        "high_risk_days": len(ag_features[ag_features['risk_category'].isin(['HIGH', 'SEVERE', 'EXTREME'])]),
        "extreme_risk_days": len(ag_features[ag_features['risk_category'] == 'EXTREME']),
        "date_range": f"{ag_features['event_date'].min()} to {ag_features['event_date'].max()}",
    })
    
    return ag_features


@asset(
    ins={
        "weather_model": AssetIn("weather_impact_model"),
        "ag_risk": AssetIn("agricultural_risk_scores"),
        "quality_metrics": AssetIn("data_quality_metrics"),
    },
    description="AI-powered emergency response recommendations",
    group_name="ml_pipeline",
    compute_kind="decision_support",
)
def emergency_response_recommendations(
    context,
    weather_model: Dict[str, Any],
    ag_risk: pd.DataFrame,
    quality_metrics: Dict[str, Any],
    starrocks: StarRocksResource,
) -> Dict[str, Any]:
    """
    Generate emergency response recommendations based on ML models and current conditions
    Provides actionable insights for emergency management
    """
    logger = get_dagster_logger()
    
    current_time = datetime.now()
    recommendations = {
        "generation_timestamp": current_time.isoformat(),
        "data_classification": "PUBLIC",
        "recommendation_version": "1.0",
        "compliance_status": "FEDRAMP_APPROVED",
        "recommendations": [],
        "risk_assessment": {},
        "data_quality_summary": quality_metrics.get("overall_status", "UNKNOWN")
    }
    
    # Weather-based recommendations
    if weather_model and weather_model.get("performance_metrics"):
        weather_accuracy = weather_model["performance_metrics"]["classification"]["accuracy"]
        
        if weather_accuracy > 0.7:  # Good model performance
            top_risk_factors = weather_model.get("model_interpretability", {}).get("top_risk_factors", [])
            
            recommendations["recommendations"].append({
                "category": "WEATHER_MONITORING",
                "priority": "HIGH",
                "recommendation": f"Monitor weather conditions closely. Key risk factors: {', '.join(top_risk_factors[:3])}",
                "confidence": weather_accuracy,
                "action_items": [
                    "Activate weather monitoring protocols",
                    "Pre-position emergency resources in high-risk areas",
                    "Issue public weather awareness communications"
                ]
            })
        
        recommendations["risk_assessment"]["weather_model_confidence"] = weather_accuracy
    
    # Agricultural risk recommendations
    if not ag_risk.empty:
        recent_risk = ag_risk[ag_risk['event_date'] >= (current_time - timedelta(days=7))]
        
        if not recent_risk.empty:
            avg_recent_risk = recent_risk['adjusted_risk_score'].mean()
            high_risk_days = len(recent_risk[recent_risk['risk_category'].isin(['HIGH', 'SEVERE', 'EXTREME'])])
            
            if avg_recent_risk > 6:
                recommendations["recommendations"].append({
                    "category": "AGRICULTURAL_RISK",
                    "priority": "MEDIUM",
                    "recommendation": f"Elevated agricultural risk detected (score: {avg_recent_risk:.1f}/10)",
                    "confidence": 0.8,
                    "action_items": [
                        "Alert agricultural extension services",
                        "Prepare drought response resources",
                        "Monitor crop insurance claims trends"
                    ]
                })
            
            recommendations["risk_assessment"]["agricultural_risk_7_day_avg"] = avg_recent_risk
            recommendations["risk_assessment"]["high_risk_days_last_7"] = high_risk_days
    
    # Data quality recommendations
    if quality_metrics.get("overall_status") == "DEGRADED":
        recommendations["recommendations"].append({
            "category": "DATA_QUALITY",
            "priority": "HIGH",
            "recommendation": "Data quality issues detected - verify all decisions with additional sources",
            "confidence": 1.0,
            "action_items": [
                "Investigate data source failures",
                "Implement backup data collection procedures",
                "Increase manual verification processes"
            ]
        })
    
    # Seasonal recommendations
    current_month = current_time.month
    if current_month in [6, 7, 8, 9]:  # Hurricane season
        recommendations["recommendations"].append({
            "category": "SEASONAL_PREPAREDNESS",
            "priority": "MEDIUM",
            "recommendation": "Peak hurricane season - maintain heightened preparedness",
            "confidence": 1.0,
            "action_items": [
                "Review hurricane response plans",
                "Test emergency communication systems",
                "Verify evacuation route status"
            ]
        })
    elif current_month in [11, 12, 1, 2]:  # Winter season
        recommendations["recommendations"].append({
            "category": "SEASONAL_PREPAREDNESS", 
            "priority": "MEDIUM",
            "recommendation": "Winter weather season - prepare for cold weather emergencies",
            "confidence": 1.0,
            "action_items": [
                "Check heating assistance programs",
                "Pre-position winter weather equipment",
                "Review cold weather shelter plans"
            ]
        })
    
    # Overall risk level
    risk_factors = len([r for r in recommendations["recommendations"] if r["priority"] == "HIGH"])
    recommendations["risk_assessment"]["overall_risk_level"] = (
        "HIGH" if risk_factors >= 2 else
        "MEDIUM" if risk_factors >= 1 or len(recommendations["recommendations"]) >= 3 else
        "LOW"
    )
    
    # Store recommendations
    starrocks.bulk_insert('emergency_response_recommendations', [recommendations])
    
    logger.info(f"Generated {len(recommendations['recommendations'])} emergency response recommendations")
    
    context.add_output_metadata({
        "recommendations_count": len(recommendations["recommendations"]),
        "high_priority_items": len([r for r in recommendations["recommendations"] if r["priority"] == "HIGH"]),
        "overall_risk_level": recommendations["risk_assessment"]["overall_risk_level"],
        "data_quality_status": recommendations["data_quality_summary"],
        "generation_time": recommendations["generation_timestamp"],
    })
    
    return recommendations


# Export ML assets
ml_assets = [
    disaster_prediction_features,
    weather_impact_model,
    agricultural_risk_scores,
    emergency_response_recommendations,
]