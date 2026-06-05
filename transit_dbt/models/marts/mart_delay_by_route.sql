-- Which routes have the worst delays?
-- Aggregates by route_id — one row per route
-- Used for the route ranking chart in Power BI

WITH base AS (
    SELECT * FROM {{ ref('stg_transit_cleaned') }}
)

SELECT
    route_id,
    COUNT(*)                                    AS total_stops,
    SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) AS delayed_stops,
    ROUND(AVG(delay_minutes), 2)                AS avg_delay_minutes,
    ROUND(MAX(delay_minutes), 2)                AS max_delay_minutes,
    ROUND(
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    )                                           AS delay_rate_pct,
    SUM(CASE WHEN delay_category = 'severe'   THEN 1 ELSE 0 END) AS severe_count,
    SUM(CASE WHEN delay_category = 'moderate' THEN 1 ELSE 0 END) AS moderate_count,
    SUM(CASE WHEN delay_category = 'minor'    THEN 1 ELSE 0 END) AS minor_count
FROM base
GROUP BY route_id
ORDER BY avg_delay_minutes DESC