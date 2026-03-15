"""
dq.py — Great Expectations data quality checks
Runs a configurable suite of expectations against a DataFrame.
Falls back to a pandas-native implementation when GE is not installed
or when the API is incompatible.
"""
import pandas as pd
import numpy as np


def run_great_expectations(df: pd.DataFrame, config: dict) -> dict:
    """
    Run DQ checks. Tries GE first; falls back to native checks.
    config = {
        "suite": [
            {"expectation": "expect_column_to_exist",              "kwargs": {"column": "salary"}},
            {"expectation": "expect_column_values_to_not_be_null", "kwargs": {"column": "id"}},
            {"expectation": "expect_column_values_to_be_between",  "kwargs": {"column": "age","min_value":0,"max_value":120}},
            {"expectation": "expect_column_values_to_be_unique",   "kwargs": {"column": "id"}},
            {"expectation": "expect_column_values_to_match_regex", "kwargs": {"column": "email","regex":"^[^@]+@[^@]+"}},
            {"expectation": "expect_table_row_count_to_be_between","kwargs": {"min_value":1}},
        ]
    }
    """
    suite = config.get("suite", _default_suite(df))

    try:
        return _run_ge(df, suite)
    except Exception:
        # GE unavailable or API mismatch — use native pandas fallback
        return _run_native(df, suite)


# ─────────────────────────────────────────────────────
# GREAT EXPECTATIONS (v1.0+ compatible)
# ─────────────────────────────────────────────────────
def _run_ge(df: pd.DataFrame, suite: list) -> dict:
    import great_expectations as gx

    # v1.0+ uses DataSources / Validators, not ge.from_pandas()
    # We use an in-memory ephemeral DataContext
    context = gx.get_context(mode="ephemeral")

    datasource = context.sources.add_pandas("pandas_source")
    asset      = datasource.add_dataframe_asset("dataframe_asset")
    batch_req  = asset.build_batch_request(dataframe=df)

    expectation_suite_name = "dataforge_suite"
    context.add_or_update_expectation_suite(expectation_suite_name)

    validator = context.get_validator(
        batch_request=batch_req,
        expectation_suite_name=expectation_suite_name,
    )

    results = []
    for check in suite:
        exp_type = check["expectation"]
        kwargs   = check.get("kwargs", {})
        try:
            fn     = getattr(validator, exp_type)
            result = fn(**kwargs)
            results.append({
                "expectation": exp_type,
                "kwargs":      kwargs,
                "success":     bool(result["success"]),
                "result":      result.get("result", {}),
            })
        except AttributeError:
            results.append({
                "expectation": exp_type,
                "kwargs":      kwargs,
                "success":     False,
                "result":      {"error": "Unknown expectation"},
            })
        except Exception as e:
            results.append({
                "expectation": exp_type,
                "kwargs":      kwargs,
                "success":     False,
                "result":      {"error": str(e)},
            })

    return _aggregate_results(results)


# ─────────────────────────────────────────────────────
# NATIVE PANDAS FALLBACK
# ─────────────────────────────────────────────────────
def _run_native(df: pd.DataFrame, suite: list) -> dict:
    results = []

    for check in suite:
        exp  = check["expectation"]
        kw   = check.get("kwargs", {})
        col  = kw.get("column")
        ok   = False
        info = {}

        try:
            if exp == "expect_column_to_exist":
                ok   = col in df.columns
                info = {"found": ok}

            elif exp == "expect_column_values_to_not_be_null":
                if col in df.columns:
                    null_count = int(df[col].isna().sum())
                    ok   = null_count == 0
                    info = {"null_count": null_count}

            elif exp == "expect_column_values_to_be_unique":
                if col in df.columns:
                    dup  = int(df[col].duplicated().sum())
                    ok   = dup == 0
                    info = {"duplicate_count": dup}

            elif exp == "expect_column_values_to_be_between":
                if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                    lo  = kw.get("min_value", -np.inf)
                    hi  = kw.get("max_value",  np.inf)
                    out = int(((df[col] < lo) | (df[col] > hi)).sum())
                    ok  = out == 0
                    info = {"out_of_range": out, "min": lo, "max": hi}

            elif exp == "expect_column_values_to_match_regex":
                if col in df.columns:
                    regex   = kw.get("regex", ".*")
                    failing = int((~df[col].astype(str).str.match(regex).fillna(False)).sum())
                    ok      = failing == 0
                    info    = {"non_matching": failing}

            elif exp == "expect_column_values_to_be_in_set":
                if col in df.columns:
                    allowed = set(kw.get("value_set", []))
                    bad     = int((~df[col].isin(allowed)).sum())
                    ok      = bad == 0
                    info    = {"not_in_set": bad}

            elif exp == "expect_table_row_count_to_be_between":
                lo   = kw.get("min_value", 0)
                hi   = kw.get("max_value", float("inf"))
                ok   = lo <= len(df) <= hi
                info = {"row_count": len(df)}

            elif exp == "expect_column_mean_to_be_between":
                if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                    mean = float(df[col].mean())
                    lo   = kw.get("min_value", -np.inf)
                    hi   = kw.get("max_value",  np.inf)
                    ok   = lo <= mean <= hi
                    info = {"mean": round(mean, 4)}

            else:
                info = {"note": "expectation not implemented in fallback"}
                ok   = False

        except Exception as e:
            info = {"error": str(e)}
            ok   = False

        results.append({
            "expectation": exp,
            "kwargs":      kw,
            "success":     ok,
            "result":      info,
        })

    return _aggregate_results(results)


# ─────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────
def _aggregate_results(results: list) -> dict:
    total  = len(results)
    passed = sum(1 for r in results if r["success"])
    failed = total - passed
    pct    = round(passed / total * 100, 2) if total else 0
    return dict(
        success      = failed == 0,
        total        = total,
        passed       = passed,
        failed       = failed,
        success_pct  = pct,
        expectations = results,
    )


def _default_suite(df: pd.DataFrame) -> list:
    """Auto-generate a basic suite for any DataFrame."""
    suite = [{
        "expectation": "expect_table_row_count_to_be_between",
        "kwargs": {"min_value": 1},
    }]
    for col in df.columns:
        suite.append({
            "expectation": "expect_column_to_exist",
            "kwargs": {"column": col},
        })
        null_rate = df[col].isna().mean()
        if null_rate < 0.05:
            suite.append({
                "expectation": "expect_column_values_to_not_be_null",
                "kwargs": {"column": col},
            })
        if pd.api.types.is_numeric_dtype(df[col]):
            q1  = float(df[col].quantile(0.01))
            q99 = float(df[col].quantile(0.99))
            suite.append({
                "expectation": "expect_column_values_to_be_between",
                "kwargs": {
                    "column":    col,
                    "min_value": round(q1,  2),
                    "max_value": round(q99, 2),
                },
            })
    return suite