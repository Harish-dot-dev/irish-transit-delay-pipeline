-- Staging model — sits between Silver source and Gold marts
-- Filters nulls, casts types, renames for consistency
-- This is the single source of truth all mart models build from

WITH source AS (
    SELECT *
    FROM transit_pipeline.silver.transit_cleaned
    WHERE
        trip_id  IS NOT NULL
        AND trip_id != ''
        AND stop_id  IS NOT NULL
        AND route_id IS NOT NULL
)

SELECT
    trip_id,
    route_id,
    stop_id,
    stop_sequence,
    delay_seconds,
    delay_minutes,
    delay_category,
    is_delayed::BOOLEAN         AS is_delayed,
    ingested_at::TIMESTAMP_NTZ  AS ingested_at,
    processed_at::TIMESTAMP_NTZ AS processed_at
FROM source