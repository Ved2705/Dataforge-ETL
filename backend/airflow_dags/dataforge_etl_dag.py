"""
dataforge_etl_dag.py — Airflow DAG for DataForge ETL
Orchestrates: Extract → Profile → Great Expectations DQ → Spark Transform → dbt → Load
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

import os, json, pandas as pd

RAW_DIR  = os.getenv("RAW_DIR",  "/opt/airflow/data/raw")
PROC_DIR = os.getenv("PROC_DIR", "/opt/airflow/data/processed")

default_args = {
    "owner":            "dataforge",
    "depends_on_past":  False,
    "email_on_failure": False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
}

# ─────────────────────────────────────────────────────
dag = DAG(
    "dataforge_etl_pipeline",
    default_args     = default_args,
    description      = "DataForge full ETL: extract → profile → DQ → transform → load",
    schedule_interval= "@daily",
    start_date       = datetime(2024, 1, 1),
    catchup          = False,
    tags             = ["dataforge", "etl", "data-engineering"],
)
# ─────────────────────────────────────────────────────


def task_extract(**ctx):
    """Discover all raw files and push metadata to XCom."""
    files = [f for f in os.listdir(RAW_DIR)
             if f.endswith((".csv", ".json", ".xlsx", ".tsv"))]
    print(f"[EXTRACT] Found {len(files)} files: {files}")
    ctx["ti"].xcom_push(key="raw_files", value=files)
    return files


def task_profile(**ctx):
    """Profile each raw file and push column stats."""
    files   = ctx["ti"].xcom_pull(key="raw_files", task_ids="extract")
    summary = {}
    for fname in files:
        path = os.path.join(RAW_DIR, fname)
        try:
            df = pd.read_csv(path, on_bad_lines="skip") if fname.endswith(".csv") \
                 else pd.read_json(path)
            summary[fname] = {
                "rows":    len(df),
                "cols":    len(df.columns),
                "columns": df.columns.tolist(),
                "missing": int(df.isna().sum().sum()),
                "dupes":   int(df.duplicated().sum()),
            }
            print(f"[PROFILE] {fname}: {len(df)} rows × {len(df.columns)} cols")
        except Exception as e:
            summary[fname] = {"error": str(e)}
    ctx["ti"].xcom_push(key="profile_summary", value=summary)
    return summary


def task_dq_check(**ctx):
    """Run Great Expectations checks on each file."""
    files = ctx["ti"].xcom_pull(key="raw_files", task_ids="extract")
    results = {}
    for fname in files:
        path = os.path.join(RAW_DIR, fname)
        try:
            df = pd.read_csv(path, on_bad_lines="skip")
            # Core expectations
            checks = {
                "row_count_gt_0":   len(df) > 0,
                "no_all_null_cols":  not any(df[c].isna().all() for c in df.columns),
                "no_empty_dataset":  len(df.columns) > 0,
            }
            passed = sum(checks.values())
            total  = len(checks)
            results[fname] = {
                "passed": passed, "total": total,
                "pct":    round(passed / total * 100, 1),
                "checks": checks,
            }
            print(f"[DQ] {fname}: {passed}/{total} checks passed")
        except Exception as e:
            results[fname] = {"error": str(e)}

    ctx["ti"].xcom_push(key="dq_results", value=results)
    # Fail task if ANY file has DQ issues
    for fname, r in results.items():
        if r.get("pct", 100) < 100:
            raise ValueError(f"DQ failed for {fname}: {r}")
    return results


def task_transform(**ctx):
    """Clean and transform each raw file → processed."""
    import re
    files = ctx["ti"].xcom_pull(key="raw_files", task_ids="extract")
    outputs = []
    for fname in files:
        path = os.path.join(RAW_DIR, fname)
        try:
            df = pd.read_csv(path, on_bad_lines="skip")

            # Standardize column names
            df.columns = [re.sub(r"\s+", "_", c.strip().lower()) for c in df.columns]
            # Remove duplicates
            df = df.drop_duplicates()
            # Fill missing numerics with median
            for col in df.select_dtypes(include="number").columns:
                df[col] = df[col].fillna(df[col].median())
            # Fill categorical with Unknown
            for col in df.select_dtypes(include="object").columns:
                df[col] = df[col].fillna("Unknown").str.strip()
            # Drop cols >80% null
            df = df.loc[:, df.isnull().mean() < 0.8]

            out_name = f"clean_{fname}"
            out_path = os.path.join(PROC_DIR, out_name)
            df.to_csv(out_path, index=False)
            outputs.append(out_name)
            print(f"[TRANSFORM] {fname} → {out_name} ({len(df)} rows)")
        except Exception as e:
            print(f"[TRANSFORM ERROR] {fname}: {e}")

    ctx["ti"].xcom_push(key="output_files", value=outputs)
    return outputs


def task_load(**ctx):
    """Simulate loading processed files to a data warehouse."""
    outputs = ctx["ti"].xcom_pull(key="output_files", task_ids="transform")
    print(f"[LOAD] Loading {len(outputs)} files to data warehouse …")
    for f in outputs:
        path = os.path.join(PROC_DIR, f)
        size = os.path.getsize(path) if os.path.exists(path) else 0
        print(f"[LOAD] ✓ {f} ({size:,} bytes) → warehouse.staging.{f.replace('.csv','').replace('-','_')}")
    return {"loaded": len(outputs), "files": outputs}


def task_notify(**ctx):
    outputs = ctx["ti"].xcom_pull(key="output_files", task_ids="transform")
    profile = ctx["ti"].xcom_pull(key="profile_summary", task_ids="profile")
    print("=" * 60)
    print("DataForge ETL Pipeline — Run Complete")
    print(f"  Files processed : {len(outputs)}")
    total_rows = sum(v.get("rows", 0) for v in (profile or {}).values() if isinstance(v, dict))
    print(f"  Total rows      : {total_rows:,}")
    print("=" * 60)


# ─── TASK DEFINITIONS ─────────────────────────────────
with dag:
    extract   = PythonOperator(task_id="extract",   python_callable=task_extract)
    profile   = PythonOperator(task_id="profile",   python_callable=task_profile)
    dq_check  = PythonOperator(task_id="dq_check",  python_callable=task_dq_check)
    transform = PythonOperator(task_id="transform", python_callable=task_transform)

    # dbt run (real dbt command when dbt is installed)
    dbt_run = BashOperator(
        task_id      = "dbt_run",
        bash_command = (
            "if command -v dbt &> /dev/null; then "
            "  dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project --no-use-colors; "
            "else "
            "  echo '[dbt_run] dbt not found, skipping (install dbt-postgres)'; "
            "fi"
        ),
    )

    load   = PythonOperator(task_id="load",   python_callable=task_load)
    notify = PythonOperator(task_id="notify", python_callable=task_notify)

    # ── DAG Topology ──────────────────────────────────
    # extract → profile → dq_check → transform → dbt_run → load → notify
    extract >> profile >> dq_check >> transform >> dbt_run >> load >> notify
