-- mart_summary.sql
-- Aggregated mart: counts and averages per category
{{ config(materialized='table') }}

SELECT
    category,
    COUNT(*)                   AS record_count,
    AVG(value)                 AS avg_value,
    MIN(value)                 AS min_value,
    MAX(value)                 AS max_value,
    SUM(value)                 AS total_value,
    MIN(created_at)            AS earliest_record,
    MAX(created_at)            AS latest_record,
    CURRENT_TIMESTAMP          AS mart_refreshed_at
FROM {{ ref('stg_dataset') }}
WHERE category IS NOT NULL
GROUP BY category
ORDER BY record_count DESC
