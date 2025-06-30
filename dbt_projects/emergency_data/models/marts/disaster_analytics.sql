
-- models/marts/disaster_analytics.sql
{{ config(
    materialized='table',
    engine='OLAP',
    duplicate_key=['analytics_id'],
    distributed_by=['state_code'],
    properties={
        "replication_num": "3",
        "storage_format": "DEFAULT",
        "compression": "LZ4"
    },
    partition_by='analysis_date'
) }}

WITH disaster_history AS (
    SELECT
        event_id,
        event_type,
        event_subtype,
        event_category,
        state_code,
        state_name,
        event_date,
        event_start_date,
        event_end_date,
        event_duration_days,
        risk_level,
        event_season,
        federal_fiscal_year,
        is_long_duration_event,
        is_high_impact_event
    FROM {{ ref('emergency_events') }}
    WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3650 DAY)  -- Last 10 years
),

agricultural_losses AS (
    SELECT
        state_code,
        county_code,
        program_year,
        commodity_name,
        SUM(indemnity_amount_usd) AS total_indemnity,
        SUM(liability_amount_usd) AS total_liability,
        AVG(loss_ratio) AS avg_loss_ratio,
        COUNT(CASE WHEN loss_category = 'HIGH_LOSS' THEN 1 END) AS high_loss_policies
    FROM {{ ref('stg_usda_data') }}
    WHERE program_year >= YEAR(CURRENT_DATE()) - 10
    GROUP BY state_code, county_code, program_year, commodity_name
),

state_disaster_trends AS (
    SELECT
        state_code,
        state_name,
        YEAR(event_date) AS disaster_year,
        event_category,
        
        -- Event counts
        COUNT(*) AS event_count,
        COUNT(CASE WHEN risk_level IN ('HIGH', 'CRITICAL') THEN 1 END) AS high_risk_events,
        COUNT(CASE WHEN is_long_duration_event THEN 1 END) AS long_duration_events,
        
        -- Temporal analysis
        AVG(event_duration_days) AS avg_event_duration,
        MAX(event_duration_days) AS max_event_duration,
        
        -- Seasonal distribution
        COUNT(CASE WHEN event_season = 'WINTER' THEN 1 END) AS winter_events,
        COUNT(CASE WHEN event_season = 'SPRING' THEN 1 END) AS spring_events,
        COUNT(CASE WHEN event_season = 'SUMMER' THEN 1 END) AS summer_events,
        COUNT(CASE WHEN event_season = 'FALL' THEN 1 END) AS fall_events
        
    FROM disaster_history
    GROUP BY state_code, state_name, YEAR(event_date), event_category
),

comprehensive_analytics AS (
    SELECT
        -- Generate analytics ID
        MD5(CONCAT(sdt.state_code, '_', sdt.disaster_year, '_', sdt.event_category)) AS analytics_id,
        
        -- Basic dimensions
        sdt.state_code,
        sdt.state_name,
        sdt.disaster_year,
        sdt.event_category,
        CURRENT_DATE() AS analysis_date,
        
        -- Event statistics
        sdt.event_count,
        sdt.high_risk_events,
        sdt.long_duration_events,
        sdt.avg_event_duration,
        sdt.max_event_duration,
        
        -- Seasonal analysis
        sdt.winter_events,
        sdt.spring_events,
        sdt.summer_events,
        sdt.fall_events,
        
        -- Risk calculations
        ROUND(sdt.high_risk_events / sdt.event_count * 100, 2) AS high_risk_event_percentage,
        ROUND(sdt.long_duration_events / sdt.event_count * 100, 2) AS long_duration_percentage,
        
        -- Trend analysis (comparing to previous year)
        LAG(sdt.event_count, 1) OVER (
            PARTITION BY sdt.state_code, sdt.event_category 
            ORDER BY sdt.disaster_year
        ) AS prev_year_event_count,
        
        -- Agricultural correlation (where available)
        al.total_indemnity,
        al.total_liability,
        al.avg_loss_ratio,
        al.high_loss_policies,
        
        -- Risk scoring
        CASE 
            WHEN sdt.event_count > 10 AND sdt.high_risk_events > 5 THEN 'VERY_HIGH'
            WHEN sdt.event_count > 5 AND sdt.high_risk_events > 2 THEN 'HIGH'
            WHEN sdt.event_count > 2 THEN 'MODERATE'
            ELSE 'LOW'
        END AS annual_disaster_risk_rating,
        
        -- Federal compliance
        {{ get_data_classification('PUBLIC') }} AS data_classification,
        {{ get_retention_date('emergency_events') }} AS retention_date,
        CURRENT_TIMESTAMP() AS analytics_generated_at
        
    FROM state_disaster_trends sdt
    LEFT JOIN agricultural_losses al ON 
        sdt.state_code = al.state_code 
        AND sdt.disaster_year = al.program_year
),

final_analytics AS (
    SELECT
        *,
        
        -- Year-over-year change calculation
        CASE 
            WHEN prev_year_event_count IS NOT NULL AND prev_year_event_count > 0 THEN
                ROUND((event_count - prev_year_event_count) / prev_year_event_count * 100, 2)
            ELSE NULL
        END AS yoy_event_count_change_percent,
        
        -- Trend classification
        CASE 
            WHEN prev_year_event_count IS NOT NULL THEN
                CASE 
                    WHEN event_count > prev_year_event_count * 1.2 THEN 'INCREASING'
                    WHEN event_count < prev_year_event_count * 0.8 THEN 'DECREASING'
                    ELSE 'STABLE'
                END
            ELSE 'INSUFFICIENT_DATA'
        END AS disaster_trend_direction,
        
        -- Preparedness recommendations
        CASE 
            WHEN annual_disaster_risk_rating IN ('HIGH', 'VERY_HIGH') THEN 
                'Enhance emergency preparedness and early warning systems'
            WHEN annual_disaster_risk_rating = 'MODERATE' THEN 
                'Maintain current preparedness levels with targeted improvements'
            ELSE 
                'Standard emergency preparedness protocols sufficient'
        END AS preparedness_recommendation
        
    FROM comprehensive_analytics
)

SELECT * FROM final_analytics