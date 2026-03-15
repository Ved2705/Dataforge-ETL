-- stg_dataset.sql
-- Staging model: clean column names, cast types, filter nulls
{{ config(materialized='view') }}

SELECT
    LOWER(TRIM(CAST(id AS VARCHAR)))          AS record_id,
    TRIM(CAST(name AS VARCHAR))               AS name,
    CAST(created_at AS TIMESTAMP)             AS created_at,
    NULLIF(TRIM(CAST(category AS VARCHAR)),'') AS category,
    CAST(value AS NUMERIC)                    AS value,
    CURRENT_TIMESTAMP                         AS dbt_loaded_at
FROM {{ source('raw', 'uploaded_dataset') }}
WHERE id IS NOT NULL
