"""
api/pipeline.py — ETL pipeline run routes
"""

from __future__ import annotations
import os, uuid, time
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify, send_file, current_app
from flask_login import login_required, current_user
from backend.models import (
    db, Dataset, PipelineRun, PipelineTask,
    QualityCheck, Transformation, CleanedFile
)
from backend.etl.pipeline import (
    extract, profile, transform, load,
    run_quality_checks, generate_dbt_model,
    trigger_airflow_dag, get_airflow_dag_status,
    compute_correlation, build_preview, detect_duplicates,
    get_spark
)

pipeline_bp = Blueprint("pipeline", __name__)

DEFAULT_STEPS = [
    {"type": "snake_case_columns",  "config": {}},
    {"type": "drop_duplicates",     "config": {}},
    {"type": "drop_missing",        "config": {"threshold_pct": 80}},
    {"type": "fill_missing",        "config": {"strategy": "median"}},
    {"type": "remove_outliers",     "config": {"method": "iqr", "threshold": 1.5}},
]


@pipeline_bp.post("/run")
@login_required
def run_pipeline():
    """
    POST /api/pipeline/run
    Body: {
      "ds_id": "...",
      "steps": [...],           # optional, defaults to DEFAULT_STEPS
      "use_spark": false,
      "trigger_airflow": false,
      "generate_dbt": false
    }
    """
    body    = request.get_json(force=True)
    ds_id   = body.get("ds_id")
    steps   = body.get("steps") or DEFAULT_STEPS
    use_spark       = body.get("use_spark", False)
    trigger_airflow = body.get("trigger_airflow", False)
    gen_dbt         = body.get("generate_dbt", False)

    ds = Dataset.query.filter_by(ds_id=ds_id, user_id=current_user.id).first()
    if not ds:
        return jsonify({"error": "Dataset not found"}), 404

    run_id  = str(uuid.uuid4())
    run = PipelineRun(
        run_id=run_id,
        dataset_id=ds.id,
        user_id=current_user.id,
        dag_id=f"dataforge_etl_{ds_id}",
        pipeline_config={"steps": steps, "use_spark": use_spark},
        status="running",
        input_rows=ds.row_count,
    )
    db.session.add(run)
    db.session.commit()

    task_results = []
    overall_ok   = True

    try:
        wall_start = time.time()

        # ── TASK 1: EXTRACT ────────────────────────────
        t = _start_task(run, "extract", "pyspark" if use_spark else "pandas", 1)
        try:
            df, meta = extract(ds.storage_path, ds.original_name, use_spark=use_spark)
            _finish_task(t, "success", result=meta,
                         logs=f"Extracted {meta['rows']} rows × {meta['cols']} cols via {meta['engine']}")
            task_results.append(t.to_dict())
        except Exception as e:
            _finish_task(t, "failed", logs=str(e))
            task_results.append(t.to_dict())
            raise

        # ── TASK 2: PROFILE ───────────────────────────
        t = _start_task(run, "profile", "pandas", 2)
        profile_data = profile(df)
        dupes        = detect_duplicates(df)
        corr         = compute_correlation(df, profile_data)
        _finish_task(t, "success",
                     result={"cols_profiled": len(df.columns), "duplicates": dupes},
                     logs=f"Profiled {len(df.columns)} columns. {dupes['duplicate_rows']} duplicates found.")
        task_results.append(t.to_dict())

        # ── TASK 3: QUALITY (Great Expectations) ──────
        t = _start_task(run, "quality_check", "great_expectations", 3)
        quality = run_quality_checks(df, profile_data)
        qc = QualityCheck(
            dataset_id=ds.id,
            run_id_fk=run.id,
            suite_name=quality["suite"],
            success=quality["success"],
            total_expectations=quality["total"],
            passed=quality["passed"],
            failed=quality["failed"],
            success_pct=quality["success_pct"],
            results_json=quality,
        )
        db.session.add(qc)
        _finish_task(t, "success", result=quality,
                     logs=f"GE engine: {quality['engine']}. {quality['passed']}/{quality['total']} checks passed ({quality['success_pct']}%)")
        task_results.append(t.to_dict())

        # ── TASK 4: TRANSFORM ─────────────────────────
        t = _start_task(run, "transform", "pyspark" if use_spark else "pandas", 4)
        df_clean, transform_log = transform(df, steps)
        _finish_task(t, "success",
                     result={"steps_run": len(transform_log), "log": transform_log},
                     logs="\n".join(
                         f"[{e['status'].upper()}] {e['step']}: {e.get('rows_before','')}→{e.get('rows_after','')}"
                         for e in transform_log
                     ))
        task_results.append(t.to_dict())

        # Persist transform records
        for step_log in transform_log:
            if step_log.get("status") == "success":
                txn = Transformation(
                    dataset_id=ds.id,
                    run_id_fk=run.id,
                    transform_type=step_log["step"],
                    tool="pyspark" if use_spark else "pandas",
                    config_json=next((s["config"] for s in steps if s["type"]==step_log["step"]), {}),
                    input_rows=step_log.get("rows_before"),
                    output_rows=step_log.get("rows_after"),
                    status="success",
                )
                db.session.add(txn)

        # ── TASK 5: DBT MODEL GENERATION ──────────────
        dbt_result = None
        if gen_dbt:
            t = _start_task(run, "dbt_generate", "dbt", 5)
            dbt_result = generate_dbt_model(df_clean, ds.original_name.split(".")[0], transform_log)
            _finish_task(t, "success", result=dbt_result,
                         logs=f"Generated dbt model: {dbt_result['model_name']}")
            task_results.append(t.to_dict())

        # ── TASK 6: LOAD ───────────────────────────────
        t = _start_task(run, "load", "pandas", 6)
        load_result = load(df_clean, current_app.config["OUTPUT_FOLDER"], ds.ds_id)
        cf = CleanedFile(
            upload_id=run.id,
            filename=load_result["csv_path"],
            row_count=load_result["rows"],
            col_count=load_result["cols"],
            removed_rows=(ds.row_count or 0) - load_result["rows"],
        )
        db.session.add(cf)
        _finish_task(t, "success", result=load_result,
                     logs=f"CSV: {load_result['csv_path']} ({load_result['csv_size_kb']} KB). "
                          f"Parquet: {'yes' if load_result['parquet_path'] else 'no'}")
        task_results.append(t.to_dict())

        # ── TASK 7: AIRFLOW TRIGGER ───────────────────
        airflow_result = None
        if trigger_airflow:
            t = _start_task(run, "airflow_trigger", "airflow", 7)
            airflow_result = trigger_airflow_dag(
                dag_id=f"dataforge_etl_{ds_id}",
                conf={"ds_id": ds_id, "run_id": run_id, "output": load_result["csv_path"]},
            )
            run.airflow_run_id = airflow_result.get("run_id")
            _finish_task(t, "success" if airflow_result["success"] else "failed",
                         result=airflow_result,
                         logs=f"Airflow DAG triggered. run_id={airflow_result.get('run_id')}")
            task_results.append(t.to_dict())

        # ── FINALIZE RUN ──────────────────────────────
        duration = round(time.time() - wall_start, 2)
        run.status      = "success"
        run.finished_at = datetime.now(timezone.utc)
        run.duration_s  = duration
        run.output_rows = load_result["rows"]
        run.rows_removed= (ds.row_count or 0) - load_result["rows"]
        run.cols_added  = 0

        ds.status = "cleaned"
        db.session.commit()

        return jsonify({
            "success":       True,
            "run_id":        run_id,
            "duration_s":    duration,
            "input_rows":    ds.row_count,
            "output_rows":   load_result["rows"],
            "rows_removed":  run.rows_removed,
            "tasks":         task_results,
            "quality":       quality,
            "transform_log": transform_log,
            "dbt":           dbt_result,
            "airflow":       airflow_result,
            "download_csv":  f"/api/pipeline/download/{run_id}",
            "download_parquet": f"/api/pipeline/download/{run_id}?format=parquet",
            "profile":       profile_data,
            "correlation":   corr,
            "preview":       build_preview(df_clean),
        })

    except Exception as e:
        run.status    = "failed"
        run.error_msg = str(e)
        run.finished_at = datetime.now(timezone.utc)
        db.session.commit()
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e), "run_id": run_id}), 500


@pipeline_bp.get("/download/<run_id>")
@login_required
def download_output(run_id):
    fmt = request.args.get("format", "csv")
    run = PipelineRun.query.filter_by(run_id=run_id, user_id=current_user.id).first_or_404()
    cf  = CleanedFile.query.filter_by(upload_id=run.id).first_or_404()

    if fmt == "parquet":
        pq_path = cf.filename.replace("_cleaned.csv", "_cleaned.parquet")
        if os.path.exists(pq_path):
            return send_file(pq_path, as_attachment=True,
                             download_name=f"cleaned_{run_id}.parquet")

    return send_file(cf.filename, as_attachment=True,
                     download_name=f"cleaned_{run_id}.csv")


@pipeline_bp.get("/dag-status/<run_id>")
@login_required
def dag_status(run_id):
    run = PipelineRun.query.filter_by(run_id=run_id, user_id=current_user.id).first_or_404()
    if not run.airflow_run_id:
        return jsonify({"status": "not_triggered"})
    result = get_airflow_dag_status(run.dag_id, run.airflow_run_id)
    return jsonify(result)


@pipeline_bp.get("/spark-status")
def spark_status():
    spark = get_spark()
    if spark:
        return jsonify({"available": True, "master": spark.conf.get("spark.master"),
                        "app_name": spark.conf.get("spark.app.name")})
    return jsonify({"available": False})


# ── helpers ──────────────────────────────────────────
def _start_task(run, name, tool, order) -> PipelineTask:
    from backend.models import PipelineTask
    t = PipelineTask(run_id_fk=run.id, task_name=name, tool=tool,
                     order_idx=order, status="running")
    db.session.add(t)
    db.session.commit()
    return t

def _finish_task(t, status, result=None, logs=None):
    from datetime import datetime, timezone
    t.status     = status
    t.finished_at= datetime.now(timezone.utc)
    t.duration_s = (t.finished_at - t.started_at).total_seconds()
    t.result_json= result
    t.logs       = logs
    db.session.commit()
