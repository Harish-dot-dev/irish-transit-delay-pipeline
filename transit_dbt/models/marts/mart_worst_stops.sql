-- Which individual stops have the most severe delays?
-- One row per stop_id
-- Used for the stop-level drill-down in Power BI

WITH base AS (
    SELECT * FROM {{ ref('stg_transit_cleaned') }}
)

SELECT
    stop_id,
    route_id,
    COUNT(*)                                    AS total_observations,
    ROUND(AVG(delay_minutes), 2)                AS avg_delay_minutes,
    ROUND(MAX(delay_minutes), 2)                AS max_delay_minutes,
    SUM(CASE WHEN delay_category = 'severe'   THEN 1 ELSE 0 END) AS severe_count,
    ROUND(
        SUM(CASE WHEN delay_category = 'severe' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    )                                           AS severe_rate_pct
FROM base
GROUP BY stop_id, route_id
HAVING COUNT(*) >= 10  -- only stops with enough observations
ORDER BY avg_delay_minutes DESC
LIMIT 100