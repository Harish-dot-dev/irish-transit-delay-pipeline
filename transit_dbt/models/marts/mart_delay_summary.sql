-- Overall KPI summary — single row
-- Powers the header cards in Power BI dashboard

WITH base AS (
    SELECT * FROM {{ ref('stg_transit_cleaned') }}
)

SELECT
    COUNT(*)                                        AS total_records,
    COUNT(DISTINCT trip_id)                         AS total_trips,
    COUNT(DISTINCT route_id)                        AS total_routes,
    COUNT(DISTINCT stop_id)                         AS total_stops,
    ROUND(AVG(delay_minutes), 2)                    AS overall_avg_delay_minutes,
    SUM(CASE WHEN is_delayed     THEN 1 ELSE 0 END) AS total_delayed,
    SUM(CASE WHEN delay_category = 'severe'   THEN 1 ELSE 0 END) AS total_severe,
    SUM(CASE WHEN delay_category = 'moderate' THEN 1 ELSE 0 END) AS total_moderate,
    SUM(CASE WHEN delay_category = 'minor'    THEN 1 ELSE 0 END) AS total_minor,
    ROUND(
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    )                                               AS overall_delay_rate_pct
FROM base