"""profiler.py — Column profiling, missing values, duplicates, outliers, correlation."""
import numpy as np
import pandas as pd
from scipy import stats


def profile_dataframe(df: pd.DataFrame) -> dict:
    profile = {}
    for col in df.columns:
        s        = df[col]
        non_null = s.dropna()
        total    = len(s)
        missing  = int(s.isna().sum())
        unique   = int(s.nunique())

        info = dict(
            name        = col,
            dtype       = str(s.dtype),
            total       = total,
            missing     = missing,
            missing_pct = round(missing / total * 100, 2) if total else 0,
            unique      = unique,
            unique_pct  = round(unique / total * 100, 2) if total else 0,
            sample      = [str(v) for v in non_null.head(5).tolist()],
        )

        if pd.api.types.is_numeric_dtype(s) and len(non_null):
            info["kind"] = "numeric"
            info.update(
                min    = round(float(non_null.min()), 4),
                max    = round(float(non_null.max()), 4),
                mean   = round(float(non_null.mean()), 4),
                median = round(float(non_null.median()), 4),
                std    = round(float(non_null.std()), 4),
                skew   = round(float(non_null.skew()), 4) if len(non_null) > 2 else None,
                q1     = round(float(non_null.quantile(0.25)), 4),
                q3     = round(float(non_null.quantile(0.75)), 4),
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
            vc = s.value_counts().head(10)
            info["top_values"] = [{"value": str(k), "count": int(v)} for k, v in vc.items()]

        profile[col] = info
    return profile


def analyze_missing(df: pd.DataFrame) -> dict:
    return {
        col: dict(count=int(df[col].isna().sum()),
                  pct=round(df[col].isna().sum() / len(df) * 100, 2))
        for col in df.columns
    }


def detect_duplicates(df: pd.DataFrame) -> dict:
    total = len(df)
    dupes = int(df.duplicated().sum())
    return dict(total_rows=total, duplicate_rows=dupes,
                unique_rows=total - dupes,
                dupe_pct=round(dupes / total * 100, 2) if total else 0)


def detect_outliers(df: pd.DataFrame, profile: dict) -> dict:
    results = {}
    for col, info in profile.items():
        if info.get("kind") != "numeric":
            continue
        s = df[col].dropna()
        if len(s) < 10:
            continue
        q1, q3 = s.quantile(0.25), s.quantile(0.75)
        iqr     = q3 - q1
        lo, hi  = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        iqr_out = int(((s < lo) | (s > hi)).sum())
        z_out   = int((np.abs(stats.zscore(s)) > 3).sum())
        if iqr_out or z_out:
            results[col] = dict(iqr_outliers=iqr_out, zscore_outliers=z_out,
                                iqr_lo=round(float(lo), 4), iqr_hi=round(float(hi), 4),
                                pct_flagged=round(iqr_out / len(s) * 100, 2))
    return results


def compute_correlation(df: pd.DataFrame, profile: dict) -> dict:
    num_cols = [c for c, i in profile.items() if i.get("kind") == "numeric"]
    if len(num_cols) < 2:
        return {}
    corr = df[num_cols].corr().round(3)
    return dict(columns=num_cols, matrix=corr.values.tolist())
