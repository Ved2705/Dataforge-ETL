-- models/marts/mart_dataset_summary.sql
-- Mart model: aggregated summary per dataset run

WITH pipeline_runs AS (
    SELECT
        pr.run_id,
        pr.started_at,
        pr.finished_at,
        pr.duration_s,
        pr.status,
        pr.input_rows,
        pr.output_rows,
        pr.rows_removed,
        d.original_name AS dataset_name,
        d.file_format,
        u.username
    FROM pipeline_runs pr
    JOIN datasets d ON d.id = pr.dataset_id
    JOIN users u    ON u.id = pr.user_id
),

quality_summary AS (
    SELECT
        qc.run_id_fk,
        qc.success_pct,
        qc.passed,
        qc.failed,
        qc.total_expectations
    FROM quality_checks qc
),

final AS (
    SELECT
        pr.run_id,
        pr.username,
        pr.dataset_name,
        pr.file_format,
        pr.started_at,
        pr.duration_s,
        pr.status,
        pr.input_rows,
        pr.output_rows,
        pr.rows_removed,
        ROUND(100.0 * pr.rows_removed / NULLIF(pr.input_rows, 0), 2) AS pct_rows_removed,
        qs.success_pct   AS quality_success_pct,
        qs.passed        AS quality_checks_passed,
        qs.failed        AS quality_checks_failed,
        qs.total_expectations
    FROM pipeline_runs pr
    LEFT JOIN quality_summary qs ON qs.run_id_fk = pr.run_id
)

SELECT * FROM final
ORDER BY started_at DESC
