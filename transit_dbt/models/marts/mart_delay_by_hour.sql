-- What time of day has the most delays?
-- Extracts hour from ingested_at — one row per hour (0-23)
-- Used for the time-of-day heatmap in Power BI

WITH base AS (
    SELECT * FROM {{ ref('stg_transit_cleaned') }}
)

SELECT
    HOUR(ingested_at)                           AS hour_of_day,
    COUNT(*)                                    AS total_stops,
    SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) AS delayed_stops,
    ROUND(AVG(delay_minutes), 2)                AS avg_delay_minutes,
    ROUND(
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    )                                           AS delay_rate_pct
FROM base
GROUP BY HOUR(ingested_at)
ORDER BY hour_of_day