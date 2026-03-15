-- models/staging/stg_raw_dataset.sql
-- Staging model: cast types, rename, filter nulls

WITH source AS (
    SELECT * FROM {{ source('raw', 'dataset') }}
),

staged AS (
    SELECT
        -- rename and cast all columns
        {% for col in var('columns', []) %}
        TRIM(CAST({{ col }} AS VARCHAR)) AS {{ col | lower | replace(' ', '_') }}
        {% if not loop.last %},{% endif %}
        {% endfor %}
    FROM source
    WHERE 1=1
)

SELECT * FROM staged
