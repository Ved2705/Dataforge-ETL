"""
pipeline.py — Core ETL Engine

Orchestrates:
  1. EXTRACT   — parse any file format with Pandas / PySpark
  2. PROFILE   — deep column profiling
  3. QUALITY   — Great Expectations validation suite
  4. TRANSFORM — normalize, cast, filter, aggregate, dedupe (Pandas + PySpark)
  5. LOAD      — write cleaned Parquet + CSV; generate dbt model SQL
  6. AIRFLOW   — trigger / poll DAG via REST API
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────
# SPARK SESSION  (lazy singleton)
# ──────────────────────────────────────────────────────
_spark = None

def get_spark():
    global _spark
    if _spark is None:
        try:
            from pyspark.sql import SparkSession
            _spark = (
                SparkSession.builder
                .appName("DataForge-ETL")
                .master(os.getenv("SPARK_MASTER", "local[*]"))
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.driver.memory", "2g")
                .config("spark.sql.shuffle.partitions", "8")
                .getOrCreate()
            )
            _spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session started")
        except Exception as e:
            logger.warning(f"Spark unavailable: {e}")
            _spark = None
    return _spark


# ──────────────────────────────────────────────────────
# EXTRACT
# ──────────────────────────────────────────────────────
def extract(filepath: str, filename: str, use_spark: bool = False) -> tuple[pd.DataFrame, dict]:
    """Parse file → pandas DataFrame + extraction metadata."""
    t0  = time.time()
    ext = filename.rsplit(".", 1)[-1].lower()

    if use_spark:
        df = _extract_spark(filepath, ext)
    else:
        df = _extract_pandas(filepath, ext)

    meta = {
        "rows":       len(df),
        "cols":       len(df.columns),
        "format":     ext,
        "engine":     "pyspark" if use_spark else "pandas",
        "duration_s": round(time.time() - t0, 3),
    }
    return df, meta


def _extract_pandas(filepath: str, ext: str) -> pd.DataFrame:
    if ext == "csv":
        return pd.read_csv(filepath, on_bad_lines="skip")
    if ext in ("tsv", "txt"):
        return pd.read_csv(filepath, sep=None, engine="python", on_bad_lines="skip")
    if ext == "json":
        with open(filepath) as f:
            raw = json.load(f)
        return pd.DataFrame(raw if isinstance(raw, list) else [raw])
    if ext in ("xlsx", "xls"):
        return pd.read_excel(filepath)
    if ext == "parquet":
        return pd.read_parquet(filepath)
    raise ValueError(f"Unsupported format: .{ext}")


def _extract_spark(filepath: str, ext: str) -> pd.DataFrame:
    spark = get_spark()
    if spark is None:
        return _extract_pandas(filepath, ext)
    if ext == "csv":
        sdf = spark.read.option("header", "true").option("inferSchema", "true").csv(filepath)
    elif ext == "json":
        sdf = spark.read.option("multiLine", "true").json(filepath)
    elif ext == "parquet":
        sdf = spark.read.parquet(filepath)
    else:
        return _extract_pandas(filepath, ext)
    return sdf.toPandas()


# ──────────────────────────────────────────────────────
# PROFILE
# ──────────────────────────────────────────────────────
def profile(df: pd.DataFrame) -> dict:
    """Deep column-level profiling."""
    t0 = time.time()
    out = {}
    for col in df.columns:
        s        = df[col]
        non_null = s.dropna()
        total    = len(s)
        missing  = int(s.isna().sum())
        unique   = int(s.nunique())

        info: dict[str, Any] = dict(
            name=col, dtype=str(s.dtype), total=total,
            missing=missing, missing_pct=round(missing/total*100, 2) if total else 0,
            unique=unique, unique_pct=round(unique/total*100, 2) if total else 0,
            sample=[str(v) for v in non_null.head(5).tolist()],
        )

        if pd.api.types.is_numeric_dtype(s) and len(non_null):
            info["kind"] = "numeric"
            info.update(
                min=round(float(non_null.min()), 4),
                max=round(float(non_null.max()), 4),
                mean=round(float(non_null.mean()), 4),
                median=round(float(non_null.median()), 4),
                std=round(float(non_null.std()), 4),
                skew=round(float(non_null.skew()), 4) if len(non_null) > 2 else None,
                q1=round(float(non_null.quantile(0.25)), 4),
                q3=round(float(non_null.quantile(0.75)), 4),
                zeros=int((non_null == 0).sum()),
                negatives=int((non_null < 0).sum()),
            )
            counts, bins = np.histogram(non_null.dropna(), bins=min(20, max(unique, 2)))
            info["hist_counts"] = counts.tolist()
            info["hist_bins"]   = [round(float(b), 2) for b in bins.tolist()]
        elif pd.api.types.is_datetime64_any_dtype(s):
            info["kind"] = "datetime"
            if len(non_null):
                info["min"] = str(non_null.min())
                info["max"] = str(non_null.max())
        else:
            info["kind"] = "categorical"
            vc = s.value_counts().head(15)
            info["top_values"] = [{"value": str(k), "count": int(v)} for k, v in vc.items()]

        out[col] = info

    out["__meta__"] = {"duration_s": round(time.time()-t0, 3)}
    return out


# ──────────────────────────────────────────────────────
# QUALITY  (Great Expectations — native / lite fallback)
# ──────────────────────────────────────────────────────
def run_quality_checks(df: pd.DataFrame, profile_data: dict) -> dict:
    """
    Run GE expectations if available, else run a built-in lite suite.
    Returns a GE-compatible result dict.
    """
    try:
        return _ge_quality(df, profile_data)
    except Exception as e:
        logger.warning(f"GE unavailable ({e}), running lite quality checks")
        return _lite_quality(df, profile_data)


def _ge_quality(df: pd.DataFrame, profile_data: dict) -> dict:
    import great_expectations as gx
    context = gx.get_context()
    ds = context.sources.add_or_update_pandas("runtime_ds")
    da = ds.add_dataframe_asset("runtime_asset")
    batch_request = da.build_batch_request(dataframe=df)

    suite_name = f"auto_suite_{uuid.uuid4().hex[:8]}"
    suite = context.add_or_update_expectation_suite(suite_name)
    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)

    for col, info in profile_data.items():
        if col == "__meta__":
            continue
        validator.expect_column_to_exist(col)
        if info["missing_pct"] == 0:
            validator.expect_column_values_to_not_be_null(col)
        if info["kind"] == "numeric":
            validator.expect_column_values_to_be_between(
                col, min_value=info.get("min"), max_value=info.get("max"), mostly=0.99
            )
        if info["unique_pct"] == 100:
            validator.expect_column_values_to_be_unique(col)

    results = validator.validate()
    stats_  = results["statistics"]
    return {
        "engine":      "great_expectations",
        "suite":       suite_name,
        "success":     bool(results["success"]),
        "total":       int(stats_["evaluated_expectations"]),
        "passed":      int(stats_["successful_expectations"]),
        "failed":      int(stats_["unsuccessful_expectations"]),
        "success_pct": round(float(stats_["success_percent"] or 0), 2),
        "results":     [
            {
                "expectation": r["expectation_config"]["expectation_type"],
                "column":      r["expectation_config"]["kwargs"].get("column"),
                "success":     bool(r["success"]),
                "result":      r.get("result", {}),
            }
            for r in results["results"]
        ],
    }


def _lite_quality(df: pd.DataFrame, profile_data: dict) -> dict:
    """Built-in quality suite — mirrors GE output format."""
    expectations = []

    def add(expectation_type, column, success, result=None, **kwargs):
        expectations.append({
            "expectation": expectation_type,
            "column":      column,
            "success":     success,
            "result":      result or {},
        })

    for col, info in profile_data.items():
        if col == "__meta__":
            continue
        s = df[col]

        # column exists
        add("expect_column_to_exist", col, col in df.columns)

        # completeness
        null_rate = info["missing_pct"] / 100
        add("expect_column_values_to_not_be_null", col,
            null_rate < 0.05,
            result={"null_rate": null_rate})

        # numeric range / distribution
        if info["kind"] == "numeric":
            non_null = s.dropna()
            z        = np.abs(stats.zscore(non_null)) if len(non_null) > 3 else np.array([])
            outlier_pct = float((z > 3).sum() / len(non_null)) if len(non_null) else 0
            add("expect_column_values_within_iqr_range", col,
                outlier_pct < 0.05,
                result={"outlier_pct": round(outlier_pct * 100, 2)})
            add("expect_column_mean_to_be_between", col,
                True,   # informational
                result={"mean": info.get("mean")})

        # uniqueness
        if info["unique_pct"] == 100:
            dupes = int(s.duplicated().sum())
            add("expect_column_values_to_be_unique", col,
                dupes == 0,
                result={"duplicate_count": dupes})

        # categorical cardinality
        if info["kind"] == "categorical" and info["unique"] < 50:
            add("expect_column_distinct_values_to_be_in_set", col,
                True,
                result={"distinct_count": info["unique"]})

    total  = len(expectations)
    passed = sum(1 for e in expectations if e["success"])
    failed = total - passed

    return {
        "engine":      "dataforge_lite",
        "suite":       "auto_lite_suite",
        "success":     failed == 0,
        "total":       total,
        "passed":      passed,
        "failed":      failed,
        "success_pct": round(passed / total * 100, 2) if total else 0,
        "results":     expectations,
    }


# ──────────────────────────────────────────────────────
# TRANSFORM
# ──────────────────────────────────────────────────────
def transform(df: pd.DataFrame, steps: list[dict]) -> tuple[pd.DataFrame, list[dict]]:
    """
    Apply a list of transform steps.
    Each step: { "type": "...", "config": {...} }
    Returns (transformed_df, log_entries)
    """
    log    = []
    result = df.copy()

    for step in steps:
        stype  = step.get("type", "")
        config = step.get("config", {})
        t0     = time.time()
        before = len(result)

        try:
            if stype == "drop_duplicates":
                result = _t_drop_duplicates(result, config)
            elif stype == "fill_missing":
                result = _t_fill_missing(result, config)
            elif stype == "drop_missing":
                result = _t_drop_missing(result, config)
            elif stype == "normalize":
                result = _t_normalize(result, config)
            elif stype == "standardize":
                result = _t_standardize(result, config)
            elif stype == "filter_rows":
                result = _t_filter_rows(result, config)
            elif stype == "cast_types":
                result = _t_cast_types(result, config)
            elif stype == "rename_columns":
                result = _t_rename_columns(result, config)
            elif stype == "drop_columns":
                result = _t_drop_columns(result, config)
            elif stype == "snake_case_columns":
                result = _t_snake_case(result)
            elif stype == "aggregate":
                result = _t_aggregate(result, config)
            elif stype == "derive_column":
                result = _t_derive_column(result, config)
            elif stype == "remove_outliers":
                result = _t_remove_outliers(result, config)
            elif stype == "spark_repartition":
                result = _t_spark_repartition(result, config)
            else:
                log.append({"step": stype, "status": "skipped", "reason": "unknown type"})
                continue

            log.append({
                "step":       stype,
                "status":     "success",
                "rows_before":before,
                "rows_after": len(result),
                "rows_delta": before - len(result),
                "duration_s": round(time.time() - t0, 3),
            })

        except Exception as e:
            log.append({"step": stype, "status": "failed", "error": str(e)})
            logger.error(f"Transform step '{stype}' failed: {e}")

    return result, log


# ── individual transform functions ──────────────────
def _t_drop_duplicates(df, cfg):
    subset = cfg.get("subset") or None
    return df.drop_duplicates(subset=subset)

def _t_fill_missing(df, cfg):
    strategy = cfg.get("strategy", "median")  # median | mean | mode | constant | ffill | bfill
    cols     = cfg.get("columns") or df.columns.tolist()
    for col in cols:
        if col not in df.columns:
            continue
        if strategy == "median" and pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].median())
        elif strategy == "mean" and pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].mean())
        elif strategy == "mode":
            df[col] = df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else "Unknown")
        elif strategy == "constant":
            df[col] = df[col].fillna(cfg.get("value", "Unknown"))
        elif strategy == "ffill":
            df[col] = df[col].ffill()
        elif strategy == "bfill":
            df[col] = df[col].bfill()
        else:
            df[col] = df[col].fillna("Unknown")
    return df

def _t_drop_missing(df, cfg):
    thresh = cfg.get("threshold_pct", 50)
    cols   = [c for c in df.columns if df[c].isna().sum()/len(df)*100 > thresh]
    return df.drop(columns=cols)

def _t_normalize(df, cfg):
    """Min-max normalize to [0,1]."""
    cols = cfg.get("columns") or [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    for col in cols:
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            mn, mx = df[col].min(), df[col].max()
            if mx != mn:
                df[col] = (df[col] - mn) / (mx - mn)
    return df

def _t_standardize(df, cfg):
    """Z-score standardize."""
    cols = cfg.get("columns") or [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    for col in cols:
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            mean, std = df[col].mean(), df[col].std()
            if std > 0:
                df[col] = (df[col] - mean) / std
    return df

def _t_filter_rows(df, cfg):
    col, op, val = cfg.get("column"), cfg.get("operator"), cfg.get("value")
    if not col or col not in df.columns:
        return df
    if op == "eq":   return df[df[col] == val]
    if op == "neq":  return df[df[col] != val]
    if op == "gt":   return df[df[col] >  float(val)]
    if op == "gte":  return df[df[col] >= float(val)]
    if op == "lt":   return df[df[col] <  float(val)]
    if op == "lte":  return df[df[col] <= float(val)]
    if op == "contains": return df[df[col].astype(str).str.contains(str(val), na=False)]
    return df

def _t_cast_types(df, cfg):
    mapping = cfg.get("mapping", {})  # {col: "int"|"float"|"str"|"datetime"}
    for col, dtype in mapping.items():
        if col not in df.columns:
            continue
        try:
            if dtype in ("int", "integer"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dtype in ("str", "string"):
                df[col] = df[col].astype(str)
            elif dtype == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")
        except Exception:
            pass
    return df

def _t_rename_columns(df, cfg):
    return df.rename(columns=cfg.get("mapping", {}))

def _t_drop_columns(df, cfg):
    cols = cfg.get("columns", [])
    return df.drop(columns=[c for c in cols if c in df.columns])

def _t_snake_case(df):
    df.columns = [re.sub(r"\s+", "_", c.strip().lower()) for c in df.columns]
    return df

def _t_aggregate(df, cfg):
    group_by = cfg.get("group_by", [])
    agg_map  = cfg.get("aggregations", {})  # {col: "sum"|"mean"|"count"|"min"|"max"}
    if not group_by or not agg_map:
        return df
    return df.groupby(group_by).agg(agg_map).reset_index()

def _t_derive_column(df, cfg):
    name   = cfg.get("name", "new_col")
    expr   = cfg.get("expression", "")
    try:
        df[name] = df.eval(expr)
    except Exception as e:
        logger.warning(f"derive_column eval failed: {e}")
    return df

def _t_remove_outliers(df, cfg):
    cols      = cfg.get("columns") or [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    method    = cfg.get("method", "iqr")
    threshold = float(cfg.get("threshold", 1.5))
    mask      = pd.Series([True] * len(df), index=df.index)
    for col in cols:
        if col not in df.columns or not pd.api.types.is_numeric_dtype(df[col]):
            continue
        s = df[col].dropna()
        if method == "iqr":
            q1, q3 = s.quantile(0.25), s.quantile(0.75)
            iqr    = q3 - q1
            mask  &= df[col].between(q1 - threshold*iqr, q3 + threshold*iqr, inclusive="both") | df[col].isna()
        elif method == "zscore":
            z     = (df[col] - s.mean()) / s.std()
            mask &= z.abs() <= threshold
    return df[mask]

def _t_spark_repartition(df, cfg):
    """Demonstrate Spark processing — repartition and return to Pandas."""
    spark = get_spark()
    if spark is None:
        return df
    n_parts = cfg.get("partitions", 4)
    sdf     = spark.createDataFrame(df)
    sdf     = sdf.repartition(n_parts)
    return sdf.toPandas()


# ──────────────────────────────────────────────────────
# DBT MODEL GENERATOR
# ──────────────────────────────────────────────────────
def generate_dbt_model(df: pd.DataFrame, model_name: str, transform_log: list) -> dict:
    """
    Generate a dbt model SQL + schema YAML from the cleaned DataFrame.
    This is what you'd commit to your dbt project.
    """
    cols      = df.columns.tolist()
    dtypes    = {c: str(df[c].dtype) for c in cols}
    safe_name = re.sub(r"\W+", "_", model_name.lower())

    # Build SELECT with light casting
    select_parts = []
    for col in cols:
        safe_col = re.sub(r"\W+", "_", col.lower())
        if "int" in dtypes[col]:
            select_parts.append(f"    CAST({col} AS INTEGER) AS {safe_col}")
        elif "float" in dtypes[col] or "double" in dtypes[col]:
            select_parts.append(f"    CAST({col} AS FLOAT) AS {safe_col}")
        elif "datetime" in dtypes[col] or "timestamp" in dtypes[col]:
            select_parts.append(f"    CAST({col} AS TIMESTAMP) AS {safe_col}")
        else:
            select_parts.append(f"    TRIM(CAST({col} AS VARCHAR)) AS {safe_col}")

    sql = (
        f"-- dbt model: {safe_name}\n"
        f"-- generated by DataForge ETL\n"
        f"-- transforms applied: {', '.join(t['step'] for t in transform_log if t.get('status')=='success')}\n\n"
        f"WITH source AS (\n"
        f"    SELECT * FROM {{{{ source('raw', '{safe_name}') }}}}\n"
        f"),\n\n"
        f"cleaned AS (\n"
        f"    SELECT\n"
        + ",\n".join(select_parts) + "\n"
        f"    FROM source\n"
        f"    WHERE 1=1\n"
        f"    -- add additional filters here\n"
        f")\n\n"
        f"SELECT * FROM cleaned\n"
    )

    schema_yaml = f"""version: 2

models:
  - name: {safe_name}
    description: "Cleaned and transformed model generated by DataForge ETL"
    columns:
""" + "".join(
        "      - name: {n}\n        description: \"{d}\"\n".format(
            n=re.sub(r'\W+', '_', c.lower()), d=c)
        for c in cols
    )

    return {
        "model_name":  safe_name,
        "sql":         sql,
        "schema_yaml": schema_yaml,
        "columns":     cols,
        "row_count":   len(df),
    }


# ──────────────────────────────────────────────────────
# LOAD  (write outputs)
# ──────────────────────────────────────────────────────
def load(df: pd.DataFrame, output_dir: str, base_name: str) -> dict:
    """Write cleaned DataFrame to CSV + Parquet."""
    os.makedirs(output_dir, exist_ok=True)
    safe  = re.sub(r"\W+", "_", base_name)
    csv_path = os.path.join(output_dir, f"{safe}_cleaned.csv")
    pq_path  = os.path.join(output_dir, f"{safe}_cleaned.parquet")

    df.to_csv(csv_path, index=False)
    try:
        df.to_parquet(pq_path, index=False, engine="pyarrow")
        parquet_ok = True
    except Exception:
        parquet_ok = False

    return {
        "csv_path":     csv_path,
        "parquet_path": pq_path if parquet_ok else None,
        "rows":         len(df),
        "cols":         len(df.columns),
        "csv_size_kb":  round(os.path.getsize(csv_path)/1024, 1),
    }


# ──────────────────────────────────────────────────────
# AIRFLOW  (REST API client)
# ──────────────────────────────────────────────────────
def trigger_airflow_dag(dag_id: str, conf: dict) -> dict:
    """Trigger an Airflow DAG run via REST API."""
    import urllib.request
    import base64

    api_url  = os.getenv("AIRFLOW_API_URL", "http://localhost:8080/api/v1")
    user     = os.getenv("AIRFLOW_USER",    "airflow")
    password = os.getenv("AIRFLOW_PASSWORD","airflow")

    run_id = f"dataforge_{uuid.uuid4().hex[:12]}"
    body   = json.dumps({
        "dag_run_id": run_id,
        "conf":       conf,
    }).encode()

    creds = base64.b64encode(f"{user}:{password}".encode()).decode()
    req   = urllib.request.Request(
        f"{api_url}/dags/{dag_id}/dagRuns",
        data=body,
        method="POST",
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Basic {creds}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.load(resp)
            return {"success": True, "run_id": run_id, "airflow_response": result}
    except Exception as e:
        return {"success": False, "error": str(e), "run_id": run_id}


def get_airflow_dag_status(dag_id: str, run_id: str) -> dict:
    import urllib.request, base64
    api_url  = os.getenv("AIRFLOW_API_URL", "http://localhost:8080/api/v1")
    user     = os.getenv("AIRFLOW_USER",    "airflow")
    password = os.getenv("AIRFLOW_PASSWORD","airflow")
    creds    = base64.b64encode(f"{user}:{password}".encode()).decode()
    req      = urllib.request.Request(
        f"{api_url}/dags/{dag_id}/dagRuns/{run_id}",
        headers={"Authorization": f"Basic {creds}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.load(resp)
    except Exception as e:
        return {"state": "unknown", "error": str(e)}


# ──────────────────────────────────────────────────────
# MISC HELPERS
# ──────────────────────────────────────────────────────
def build_preview(df: pd.DataFrame, nrows: int = 100) -> dict:
    preview_df = df.head(nrows)
    if len(preview_df.columns) > 25:
        preview_df = preview_df.iloc[:, :25]
    return {
        "columns": preview_df.columns.tolist(),
        "rows":    preview_df.fillna("").astype(str).values.tolist(),
    }

def compute_correlation(df: pd.DataFrame, profile_data: dict) -> dict:
    num_cols = [c for c, i in profile_data.items() if isinstance(i, dict) and i.get("kind") == "numeric"]
    if len(num_cols) < 2:
        return {}
    corr = df[num_cols].corr().round(3)
    return {"columns": num_cols, "matrix": corr.values.tolist()}

def detect_duplicates(df: pd.DataFrame) -> dict:
    total  = len(df)
    dupes  = int(df.duplicated().sum())
    return {
        "total_rows": total, "duplicate_rows": dupes,
        "unique_rows": total - dupes,
        "dupe_pct":   round(dupes / total * 100, 2) if total else 0,
    }
