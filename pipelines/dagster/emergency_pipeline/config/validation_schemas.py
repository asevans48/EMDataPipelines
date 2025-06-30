"""
Validation Schemas for Emergency Management Data Sources
Defines expected data schemas and validation rules for each source
"""

from typing import Dict, Any, List

# FEMA Disaster Declarations Schema
FEMA_SCHEMA = {
    "required_columns": [
        "disaster_number",
        "state", 
        "declaration_date",
        "incident_type",
        "declaration_type"
    ],
    "column_types": {
        "disaster_number": "string",
        "state": "string",
        "declaration_date": "datetime",
        "incident_begin_date": "datetime", 
        "incident_end_date": "datetime",
        "incident_type": "string",
        "declaration_type": "string",
        "title": "string",
        "designated_area": "string",
        "fy_declared": "numeric"
    },
    "nullable_columns": [
        "incident_end_date",
        "title",
        "designated_area"
    ],
    "pattern_validations": {
        "disaster_number": r"^\d{4,5}$",
        "state": r"^[A-Z]{2}$",
        "declaration_type": r"^(DR|EM|FM)$"
    },
    "value_constraints": {
        "incident_type": [
            "Biological", "Chemical", "Coastal Storm", "Dam/Levee Break", 
            "Drought", "Earthquake", "Fire", "Fishing Losses", "Flood",
            "Freezing", "Hurricane", "Mud/Landslide", "Other", "Severe Ice Storm",
            "Severe Storm(s)", "Snow", "Straight-line Winds", "Terrorism",
            "Tornado", "Toxic Substances", "Tsunami", "Typhoon", "Volcano",
            "Winter Storm"
        ],
        "declaration_type": ["DR", "EM", "FM"]  # Disaster, Emergency, Fire Management
    },
    "business_rules": [
        {
            "name": "incident_date_order",
            "description": "Incident begin date must be before or equal to end date",
            "validation": "incident_begin_date <= incident_end_date"
        },
        {
            "name": "declaration_after_incident",
            "description": "Declaration date should be after incident begin date",
            "validation": "declaration_date >= incident_begin_date"
        },
        {
            "name": "disaster_number_format",
            "description": "Disaster number should be 4-5 digits",
            "validation": "disaster_number matches '^\\d{4,5}$'"
        }
    ]
}

# NOAA Weather Alerts Schema
NOAA_SCHEMA = {
    "required_columns": [
        "alert_id",
        "event",
        "severity",
        "effective"
    ],
    "column_types": {
        "alert_id": "string",
        "event": "string",
        "severity": "string",
        "urgency": "string", 
        "certainty": "string",
        "headline": "string",
        "description": "string",
        "instruction": "string",
        "effective": "datetime",
        "expires": "datetime",
        "onset": "datetime",
        "area_desc": "string"
    },
    "nullable_columns": [
        "expires",
        "onset", 
        "instruction",
        "description"
    ],
    "pattern_validations": {
        "alert_id": r"^[A-Z0-9\-\.]+$",
        "severity": r"^(Extreme|Severe|Moderate|Minor)$",
        "urgency": r"^(Immediate|Expected|Future|Past)$",
        "certainty": r"^(Observed|Likely|Possible|Unlikely)$"
    },
    "value_constraints": {
        "severity": ["Extreme", "Severe", "Moderate", "Minor"],
        "urgency": ["Immediate", "Expected", "Future", "Past"],
        "certainty": ["Observed", "Likely", "Possible", "Unlikely"],
        "event": [
            "Tornado Warning", "Tornado Watch", "Severe Thunderstorm Warning",
            "Severe Thunderstorm Watch", "Flash Flood Warning", "Flood Warning",
            "Winter Storm Warning", "Winter Weather Advisory", "Blizzard Warning",
            "High Wind Warning", "Hurricane Warning", "Hurricane Watch",
            "Heat Warning", "Excessive Heat Warning", "Fire Weather Watch",
            "Red Flag Warning", "Air Quality Alert"
        ]
    },
    "business_rules": [
        {
            "name": "effective_before_expires",
            "description": "Effective time must be before expiration time",
            "validation": "effective <= expires"
        },
        {
            "name": "severity_urgency_consistency",
            "description": "Extreme severity should have Immediate urgency",
            "validation": "severity != 'Extreme' OR urgency = 'Immediate'"
        },
        {
            "name": "alert_freshness",
            "description": "Alerts should not be older than 7 days",
            "validation": "effective >= current_date - interval 7 day"
        }
    ]
}

# CoAgMet Weather Station Schema
COAGMET_SCHEMA = {
    "required_columns": [
        "station_id",
        "timestamp",
        "temperature"
    ],
    "column_types": {
        "station_id": "string",
        "station_name": "string",
        "timestamp": "datetime",
        "temperature": "numeric",
        "temperature_celsius": "numeric",
        "temperature_fahrenheit": "numeric",
        "humidity": "numeric",
        "wind_speed": "numeric",
        "wind_direction": "numeric",
        "precipitation": "numeric",
        "solar_radiation": "numeric",
        "latitude": "numeric",
        "longitude": "numeric",
        "elevation": "numeric"
    },
    "nullable_columns": [
        "wind_direction",
        "solar_radiation",
        "elevation",
        "station_name"
    ],
    "pattern_validations": {
        "station_id": r"^[A-Z0-9]+$"
    },
    "value_constraints": {
        "temperature_celsius": {"min": -50, "max": 60},
        "temperature_fahrenheit": {"min": -58, "max": 140},
        "humidity": {"min": 0, "max": 100},
        "wind_speed": {"min": 0, "max": 200},  # km/h
        "wind_direction": {"min": 0, "max": 360},
        "precipitation": {"min": 0, "max": 500},  # mm
        "latitude": {"min": 36.5, "max": 41.5},  # Colorado bounds
        "longitude": {"min": -109.5, "max": -102.0}
    },
    "business_rules": [
        {
            "name": "temperature_consistency",
            "description": "Celsius and Fahrenheit temperatures should be consistent",
            "validation": "abs(temperature_fahrenheit - (temperature_celsius * 9/5 + 32)) < 1"
        },
        {
            "name": "colorado_location",
            "description": "CoAgMet stations should be in Colorado",
            "validation": "latitude between 36.5 and 41.5 AND longitude between -109.5 and -102.0"
        },
        {
            "name": "reasonable_precipitation",
            "description": "Daily precipitation should be reasonable",
            "validation": "precipitation <= 300"  # 300mm is very high but possible
        }
    ]
}

# USDA Agricultural Data Schema  
USDA_SCHEMA = {
    "required_columns": [
        "commodity",
        "state_alpha", 
        "year",
        "value"
    ],
    "column_types": {
        "commodity": "string",
        "commodity_desc": "string",
        "state_alpha": "string",
        "state_name": "string", 
        "county_code": "string",
        "county_name": "string",
        "year": "numeric",
        "reference_period_desc": "string",
        "statisticcat_desc": "string",
        "unit_desc": "string",
        "value": "numeric",
        "cv_percent": "numeric"
    },
    "nullable_columns": [
        "county_code",
        "county_name",
        "cv_percent",
        "reference_period_desc"
    ],
    "pattern_validations": {
        "state_alpha": r"^[A-Z]{2}$",
        "year": r"^\d{4}$",
        "county_code": r"^\d{3}$"
    },
    "value_constraints": {
        "year": {"min": 1990, "max": 2030},
        "value": {"min": 0, "max": 999999999},
        "cv_percent": {"min": 0, "max": 100}
    },
    "business_rules": [
        {
            "name": "year_validity",
            "description": "Year should be reasonable for agricultural data",
            "validation": "year between 1990 and year(current_date) + 2"
        },
        {
            "name": "positive_values",
            "description": "Agricultural values should be non-negative",
            "validation": "value >= 0"
        },
        {
            "name": "state_county_consistency",
            "description": "County should belong to the specified state",
            "validation": "county_code is null OR state_alpha is not null"
        }
    ]
}

# Schema mapping by source
SOURCE_SCHEMAS = {
    "fema": FEMA_SCHEMA,
    "FEMA_OpenFEMA": FEMA_SCHEMA,
    "noaa": NOAA_SCHEMA, 
    "NOAA_Weather_API": NOAA_SCHEMA,
    "coagmet": COAGMET_SCHEMA,
    "CoAgMet": COAGMET_SCHEMA,
    "usda": USDA_SCHEMA,
    "USDA_RMA": USDA_SCHEMA,
    "USDA_NASS": USDA_SCHEMA
}

# Common validation patterns
COMMON_PATTERNS = {
    "state_code": r"^[A-Z]{2}$",
    "zip_code": r"^\d{5}(-\d{4})?$",
    "phone_number": r"^\d{3}-\d{3}-\d{4}$",
    "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    "date_iso": r"^\d{4}-\d{2}-\d{2}$",
    "datetime_iso": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
    "disaster_number": r"^\d{4,5}$",
    "fips_code": r"^\d{5}$"
}

# Common value constraints
COMMON_CONSTRAINTS = {
    "us_states": [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
        "DC", "PR", "VI", "GU", "AS", "MP"
    ],
    "data_classifications": ["PUBLIC", "INTERNAL", "RESTRICTED", "CONFIDENTIAL"],
    "boolean_values": [True, False, "true", "false", "True", "False", 1, 0, "1", "0"]
}


def get_schema_for_source(source_name: str) -> Dict[str, Any]:
    """Get validation schema for a data source"""
    return SOURCE_SCHEMAS.get(source_name, {})


def validate_schema_compliance(data_dict: Dict[str, Any], schema: Dict[str, Any]) -> List[str]:
    """Validate a data record against a schema"""
    issues = []
    
    # Check required columns
    required_columns = schema.get("required_columns", [])
    for column in required_columns:
        if column not in data_dict or data_dict[column] is None:
            issues.append(f"Required column '{column}' is missing or null")
    
    # Check pattern validations
    pattern_validations = schema.get("pattern_validations", {})
    for column, pattern in pattern_validations.items():
        if column in data_dict and data_dict[column] is not None:
            import re
            if not re.match(pattern, str(data_dict[column])):
                issues.append(f"Column '{column}' value '{data_dict[column]}' does not match pattern '{pattern}'")
    
    # Check value constraints
    value_constraints = schema.get("value_constraints", {})
    for column, constraint in value_constraints.items():
        if column in data_dict and data_dict[column] is not None:
            value = data_dict[column]
            
            if isinstance(constraint, list):
                # Enumerated values
                if value not in constraint:
                    issues.append(f"Column '{column}' value '{value}' not in allowed values: {constraint}")
            elif isinstance(constraint, dict):
                # Range constraints
                if "min" in constraint and value < constraint["min"]:
                    issues.append(f"Column '{column}' value {value} below minimum {constraint['min']}")
                if "max" in constraint and value > constraint["max"]:
                    issues.append(f"Column '{column}' value {value} above maximum {constraint['max']}")
    
    return issues


def get_schema_summary(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Get a summary of a schema definition"""
    return {
        "required_columns_count": len(schema.get("required_columns", [])),
        "total_columns_defined": len(schema.get("column_types", {})),
        "nullable_columns_count": len(schema.get("nullable_columns", [])),
        "pattern_validations_count": len(schema.get("pattern_validations", {})),
        "value_constraints_count": len(schema.get("value_constraints", {})),
        "business_rules_count": len(schema.get("business_rules", [])),
        "required_columns": schema.get("required_columns", []),
        "data_types": list(set(schema.get("column_types", {}).values()))
    }


def create_custom_schema(
    required_columns: List[str],
    column_types: Dict[str, str] = None,
    pattern_validations: Dict[str, str] = None,
    value_constraints: Dict[str, Any] = None,
    business_rules: List[Dict[str, str]] = None
) -> Dict[str, Any]:
    """Create a custom schema definition"""
    
    schema = {
        "required_columns": required_columns,
        "column_types": column_types or {},
        "nullable_columns": [],
        "pattern_validations": pattern_validations or {},
        "value_constraints": value_constraints or {},
        "business_rules": business_rules or []
    }
    
    return schema


def merge_schemas(base_schema: Dict[str, Any], override_schema: Dict[str, Any]) -> Dict[str, Any]:
    """Merge two schemas, with override taking precedence"""
    
    merged = base_schema.copy()
    
    # Merge lists by extending
    for list_key in ["required_columns", "nullable_columns", "business_rules"]:
        if list_key in override_schema:
            merged[list_key] = merged.get(list_key, []) + override_schema[list_key]
    
    # Merge dictionaries by updating
    for dict_key in ["column_types", "pattern_validations", "value_constraints"]:
        if dict_key in override_schema:
            merged[dict_key] = {**merged.get(dict_key, {}), **override_schema[dict_key]}
    
    return merged


def get_all_schemas() -> Dict[str, Dict[str, Any]]:
    """Get all available schemas"""
    return SOURCE_SCHEMAS.copy()


def validate_all_sources_schemas() -> Dict[str, List[str]]:
    """Validate all source schemas for internal consistency"""
    validation_results = {}
    
    for source_name, schema in SOURCE_SCHEMAS.items():
        issues = []
        
        # Check that required columns have types defined
        required_columns = schema.get("required_columns", [])
        column_types = schema.get("column_types", {})
        
        for column in required_columns:
            if column not in column_types:
                issues.append(f"Required column '{column}' has no type definition")
        
        # Check that pattern validation columns have string types
        pattern_validations = schema.get("pattern_validations", {})
        for column in pattern_validations.keys():
            if column in column_types and column_types[column] != "string":
                issues.append(f"Column '{column}' has pattern validation but is not string type")
        
        # Check that business rules reference defined columns
        business_rules = schema.get("business_rules", [])
        all_columns = set(column_types.keys())
        
        for rule in business_rules:
            rule_text = rule.get("validation", "")
            # Simple check for column references (could be more sophisticated)
            for column in all_columns:
                if column in rule_text and column not in column_types:
                    issues.append(f"Business rule references undefined column '{column}'")
        
        validation_results[source_name] = issues
    
    return validation_results