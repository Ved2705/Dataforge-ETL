"""
transforms.py — Pandas-based ETL transformations
Supports: clean, normalize, filter, aggregate, rename,
          cast, deduplicate, fill_missing, drop_columns, derive
"""
import re
import pandas as pd
import numpy as np


def apply_transform(df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, list]:
    """
    Apply a list of transform operations defined in config["operations"].
    Returns (transformed_df, log_list).
    """
    ops = config.get("operations", [])
    log = []

    for op in ops:
        kind = op.get("op", "")
        try:
            df, msg = _dispatch(df, kind, op)
            log.append({"op": kind, "status": "ok", "msg": msg})
        except Exception as e:
            log.append({"op": kind, "status": "error", "msg": str(e)})

    return df, log


def _dispatch(df, kind, op):
    handlers = {
        "deduplicate":    _deduplicate,
        "drop_nulls":     _drop_nulls,
        "fill_missing":   _fill_missing,
        "drop_columns":   _drop_columns,
        "rename_columns": _rename_columns,
        "cast":           _cast,
        "filter_rows":    _filter_rows,
        "normalize":      _normalize,
        "standardize":    _standardize,
        "aggregate":      _aggregate,
        "derive_column":  _derive_column,
        "trim_strings":   _trim_strings,
        "snake_case_cols":_snake_case_cols,
        "drop_high_null": _drop_high_null,
    }
    fn = handlers.get(kind)
    if not fn:
        return df, f"Unknown op '{kind}' — skipped"
    return fn(df, op)


def _deduplicate(df, op):
    subset = op.get("subset")
    before = len(df)
    df = df.drop_duplicates(subset=subset)
    return df, f"Removed {before - len(df)} duplicate rows"


def _drop_nulls(df, op):
    cols = op.get("columns")
    before = len(df)
    df = df.dropna(subset=cols) if cols else df.dropna()
    return df, f"Dropped {before - len(df)} rows with nulls"


def _fill_missing(df, op):
    strategy = op.get("strategy", "median")   # median | mean | mode | constant
    value    = op.get("value")
    cols     = op.get("columns", df.columns.tolist())
    filled   = 0
    for col in cols:
        if col not in df.columns:
            continue
        n = df[col].isna().sum()
        if n == 0:
            continue
        if strategy == "median" and pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].median())
        elif strategy == "mean" and pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].mean())
        elif strategy == "mode":
            df[col] = df[col].fillna(df[col].mode()[0])
        elif strategy == "constant":
            df[col] = df[col].fillna(value if value is not None else "Unknown")
        else:
            df[col] = df[col].fillna("Unknown")
        filled += int(n)
    return df, f"Filled {filled} missing values using strategy='{strategy}'"


def _drop_columns(df, op):
    cols = op.get("columns", [])
    cols = [c for c in cols if c in df.columns]
    df = df.drop(columns=cols)
    return df, f"Dropped columns: {cols}"


def _rename_columns(df, op):
    mapping = op.get("mapping", {})
    df = df.rename(columns=mapping)
    return df, f"Renamed {len(mapping)} columns"


def _cast(df, op):
    casts = op.get("casts", {})   # {col: dtype}
    for col, dtype in casts.items():
        if col not in df.columns:
            continue
        if dtype in ("int", "int64"):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif dtype in ("float", "float64"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif dtype == "datetime":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif dtype == "string":
            df[col] = df[col].astype(str)
        elif dtype == "bool":
            df[col] = df[col].astype(bool)
    return df, f"Cast {len(casts)} columns"


def _filter_rows(df, op):
    col  = op.get("column")
    cond = op.get("condition")   # gt | lt | eq | neq | contains | not_null
    val  = op.get("value")
    before = len(df)
    if col not in df.columns:
        return df, f"Column '{col}' not found"
    if cond == "gt":      df = df[df[col] > val]
    elif cond == "lt":    df = df[df[col] < val]
    elif cond == "gte":   df = df[df[col] >= val]
    elif cond == "lte":   df = df[df[col] <= val]
    elif cond == "eq":    df = df[df[col] == val]
    elif cond == "neq":   df = df[df[col] != val]
    elif cond == "contains":   df = df[df[col].astype(str).str.contains(str(val), na=False)]
    elif cond == "not_null":   df = df[df[col].notna()]
    elif cond == "is_null":    df = df[df[col].isna()]
    return df, f"Filtered {before - len(df)} rows out (condition: {col} {cond} {val})"


def _normalize(df, op):
    """Min-max normalization → [0, 1]"""
    cols = op.get("columns", [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])])
    for col in cols:
        if col not in df.columns: continue
        mn, mx = df[col].min(), df[col].max()
        if mx != mn:
            df[col] = (df[col] - mn) / (mx - mn)
    return df, f"Min-max normalized {len(cols)} columns"


def _standardize(df, op):
    """Z-score standardization → mean=0, std=1"""
    cols = op.get("columns", [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])])
    for col in cols:
        if col not in df.columns: continue
        m, s = df[col].mean(), df[col].std()
        if s and s != 0:
            df[col] = (df[col] - m) / s
    return df, f"Z-score standardized {len(cols)} columns"


def _aggregate(df, op):
    group_by = op.get("group_by", [])
    agg_map  = op.get("aggregations", {})   # {col: [mean, sum, count, ...]}
    if not group_by or not agg_map:
        return df, "aggregate: missing group_by or aggregations"
    df = df.groupby(group_by).agg(agg_map).reset_index()
    df.columns = ["_".join(c).strip("_") for c in df.columns.values]
    return df, f"Aggregated by {group_by}"


def _derive_column(df, op):
    name = op.get("name")
    expr = op.get("expression")   # simple pandas eval expression
    if name and expr:
        df[name] = df.eval(expr)
    return df, f"Derived column '{name}' = {expr}"


def _trim_strings(df, op):
    cols = op.get("columns", df.select_dtypes(include="object").columns.tolist())
    for col in cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    return df, f"Trimmed whitespace in {len(cols)} string columns"


def _snake_case_cols(df, op):
    df.columns = [re.sub(r"\s+", "_", c.strip().lower()) for c in df.columns]
    return df, "Renamed all columns to snake_case"


def _drop_high_null(df, op):
    threshold = op.get("threshold", 0.8)
    before    = df.shape[1]
    null_pct  = df.isnull().mean()
    drop_cols = null_pct[null_pct > threshold].index.tolist()
    df        = df.drop(columns=drop_cols)
    return df, f"Dropped {len(drop_cols)} columns with >{threshold*100:.0f}% nulls"
