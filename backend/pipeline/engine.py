"""
engine.py — ETL Pipeline Executor
Runs each pipeline step in sequence, logs to DB, coordinates
Spark transforms, Great Expectations DQ checks, dbt runs,
and Airflow DAG triggers.
"""
import os, uuid, json, traceback
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import numpy as np

from backend.pipeline.profiler   import profile_dataframe, analyze_missing, detect_duplicates, detect_outliers, compute_correlation
from backend.pipeline.transforms import apply_transform
from backend.pipeline.dq         import run_great_expectations
from backend.pipeline.spark_ops  import run_spark_transform
from backend.pipeline.dbt_runner import run_dbt_model
from backend.pipeline.airflow_client import trigger_airflow_dag


def run_pipeline(pipeline, dataset, run, db, output_folder: str):
    """
    Execute all steps of a pipeline, update the PipelineRun record live.
    """
    from backend.models import RunLog, DQResult

    def log(level, step, msg):
        entry = RunLog(run_id=run.id, level=level, step=step, message=msg)
        db.session.add(entry)
        db.session.commit()
        print(f"[{level}] [{step}] {msg}")

    steps    = pipeline.steps_json or []
    summary  = {}
    df       = None

    try:
        # ── STEP: EXTRACT ──────────────────────────────
        log("INFO", "extract", f"Loading dataset '{dataset.name}' from {dataset.storage_path}")
        df = _load_dataset(dataset)
        run.rows_in = len(df)
        db.session.commit()
        log("INFO", "extract", f"Loaded {len(df)} rows × {len(df.columns)} columns")

        # ── STEP: PROFILE ──────────────────────────────
        log("INFO", "profile", "Profiling columns …")
        profile   = profile_dataframe(df)
        missing   = analyze_missing(df)
        dupes     = detect_duplicates(df)
        outliers  = detect_outliers(df, profile)
        corr      = compute_correlation(df, profile)
        summary["profile"]     = profile
        summary["missing"]     = missing
        summary["duplicates"]  = dupes
        summary["outliers"]    = outliers
        summary["correlation"] = corr
        log("INFO", "profile", f"Profile complete — {len(profile)} columns profiled")

        # ── PIPELINE-DEFINED STEPS ─────────────────────
        for idx, step in enumerate(steps):
            stype  = step.get("type", "")
            scfg   = step.get("config", {})
            sname  = step.get("name", f"step_{idx+1}")

            log("INFO", sname, f"Starting step [{stype}]")

            if stype == "transform":
                df, t_log = apply_transform(df, scfg)
                summary[f"transform_{idx}"] = t_log
                log("INFO", sname, f"Transform complete — {len(df)} rows out")

            elif stype == "spark_transform":
                df, s_log = run_spark_transform(df, scfg)
                summary[f"spark_{idx}"] = s_log
                log("INFO", sname, "Spark transform complete")

            elif stype == "dq_check":
                dq_result = run_great_expectations(df, scfg)
                pct       = dq_result["success_pct"]
                summary[f"dq_{idx}"] = dq_result
                level = "INFO" if dq_result["success"] else "WARN"
                log(level, sname, f"DQ check: {pct:.1f}% expectations passed")

                # persist DQResult
                dqr = DQResult(
                    run_id           = run.id,
                    success          = dq_result["success"],
                    total_checks     = dq_result["total"],
                    passed_checks    = dq_result["passed"],
                    failed_checks    = dq_result["failed"],
                    success_pct      = pct,
                    expectations_json= dq_result["expectations"],
                )
                db.session.add(dqr)
                db.session.commit()

            elif stype == "dbt":
                dbt_log = run_dbt_model(scfg)
                summary[f"dbt_{idx}"] = dbt_log
                log("INFO", sname, f"dbt run complete — {dbt_log.get('status')}")

            elif stype == "airflow_trigger":
                resp = trigger_airflow_dag(scfg.get("dag_id"), scfg)
                summary[f"airflow_{idx}"] = resp
                log("INFO", sname, f"Airflow DAG triggered: {scfg.get('dag_id')}")

            else:
                log("WARN", sname, f"Unknown step type '{stype}' — skipping")

        # ── STEP: LOAD (write output) ──────────────────
        log("INFO", "load", "Writing cleaned output …")
        out_filename = f"output_{run.id}_{uuid.uuid4().hex[:6]}.csv"
        out_path     = os.path.join(output_folder, out_filename)
        df.to_csv(out_path, index=False)
        run.output_path = out_path
        run.rows_out    = len(df)
        log("INFO", "load", f"Output written → {out_filename} ({len(df)} rows)")

        # ── FINISH ─────────────────────────────────────
        run.status       = "success"
        run.finished_at  = datetime.now(timezone.utc)
        run.summary_json = summary
        db.session.commit()
        log("INFO", "engine", "Pipeline completed successfully ✓")

    except Exception as exc:
        tb = traceback.format_exc()
        log("ERROR", "engine", f"Pipeline FAILED: {exc}\n{tb}")
        run.status      = "failed"
        run.finished_at = datetime.now(timezone.utc)
        db.session.commit()
        raise


def _load_dataset(dataset) -> pd.DataFrame:
    path = dataset.storage_path
    ext  = Path(path).suffix.lower()
    if ext == ".csv":
        return pd.read_csv(path, on_bad_lines="skip")
    elif ext in (".xlsx", ".xls"):
        return pd.read_excel(path)
    elif ext == ".json":
        import json as _json
        with open(path) as f:
            data = _json.load(f)
        return pd.DataFrame(data if isinstance(data, list) else [data])
    elif ext in (".tsv", ".txt"):
        return pd.read_csv(path, sep=None, engine="python", on_bad_lines="skip")
    raise ValueError(f"Unsupported format: {ext}")
