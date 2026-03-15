"""
api/dq_rules.py — Data Quality Rules Builder
CRUD for rule sets + run validation against datasets
"""
import os, re as regex_mod
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify
from flask_login import login_required, current_user
from backend.models import db, Dataset, DQRuleSet

dq_rules_bp = Blueprint("dq_rules", __name__)


def _load_df(ds):
    import pandas as pd
    ext = (ds.file_format or ds.name.rsplit(".", 1)[-1]).lower()
    p = ds.storage_path
    if ext == "csv":       return pd.read_csv(p, on_bad_lines="skip")
    if ext in ("xlsx","xls"): return pd.read_excel(p)
    if ext == "parquet":   return pd.read_parquet(p)
    if ext == "json":      return pd.read_json(p)
    if ext in ("tsv","txt"): return pd.read_csv(p, sep="\t", on_bad_lines="skip")
    return pd.read_csv(p, sep=None, engine="python", on_bad_lines="skip")


# ── CRUD ─────────────────────────────────────────────

@dq_rules_bp.route("", methods=["POST"])
@login_required
def create_ruleset():
    try:
        body = request.get_json(force=True)
        rs = DQRuleSet(
            user_id    = current_user.id,
            name       = body.get("name", "Untitled Rule Set"),
            dataset_id = body.get("dataset_id"),
            rules_json = body.get("rules", []),
        )
        db.session.add(rs)
        db.session.commit()
        return jsonify({"success": True, "ruleset": rs.to_dict()}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dq_rules_bp.route("", methods=["GET"])
@login_required
def list_rulesets():
    try:
        rsets = DQRuleSet.query.filter_by(user_id=current_user.id).order_by(DQRuleSet.created_at.desc()).all()
        return jsonify([r.to_dict() for r in rsets])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dq_rules_bp.route("/<int:rid>", methods=["GET"])
@login_required
def get_ruleset(rid):
    try:
        rs = DQRuleSet.query.filter_by(id=rid, user_id=current_user.id).first_or_404()
        return jsonify(rs.to_dict())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dq_rules_bp.route("/<int:rid>", methods=["PUT"])
@login_required
def update_ruleset(rid):
    try:
        rs = DQRuleSet.query.filter_by(id=rid, user_id=current_user.id).first_or_404()
        body = request.get_json(force=True)
        if "name" in body:       rs.name = body["name"]
        if "dataset_id" in body: rs.dataset_id = body["dataset_id"]
        if "rules" in body:      rs.rules_json = body["rules"]
        db.session.commit()
        return jsonify({"success": True, "ruleset": rs.to_dict()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dq_rules_bp.route("/<int:rid>", methods=["DELETE"])
@login_required
def delete_ruleset(rid):
    try:
        rs = DQRuleSet.query.filter_by(id=rid, user_id=current_user.id).first_or_404()
        db.session.delete(rs)
        db.session.commit()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── RUN VALIDATION ───────────────────────────────────

@dq_rules_bp.route("/<int:rid>/run", methods=["POST"])
@login_required
def run_ruleset(rid):
    try:
        import pandas as pd
        rs = DQRuleSet.query.filter_by(id=rid, user_id=current_user.id).first_or_404()
        body = request.get_json(force=True) or {}
        did = body.get("dataset_id") or rs.dataset_id
        if not did:
            return jsonify({"error": "No dataset specified"}), 400

        ds = Dataset.query.filter_by(id=did, user_id=current_user.id).first_or_404()
        if not ds.storage_path or not os.path.exists(ds.storage_path):
            return jsonify({"error": "Dataset file not found"}), 404

        df = _load_df(ds)
        rules = rs.rules_json or []
        results = []
        passed = 0
        failed = 0
        warnings = 0

        for rule in rules:
            r = _evaluate_rule(rule, df)
            results.append(r)
            if r["status"] == "passed":
                passed += 1
            elif r["status"] == "warning":
                warnings += 1
            else:
                failed += 1

        overall = "passed" if failed == 0 and warnings == 0 else "warning" if failed == 0 else "failed"

        # Update ruleset
        rs.last_run_at = datetime.now(timezone.utc)
        rs.last_run_status = overall
        db.session.commit()

        return jsonify({
            "success": True,
            "passed": passed,
            "failed": failed,
            "warnings": warnings,
            "overall_status": overall,
            "results": results,
            "dataset_name": ds.name,
            "total_rows": len(df),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _evaluate_rule(rule: dict, df) -> dict:
    """Evaluate a single DQ rule against a DataFrame."""
    import pandas as pd
    rtype   = rule.get("rule_type", "")
    col     = rule.get("column", "")
    params  = rule.get("params", {})
    severity = rule.get("severity", "error")
    rname   = rule.get("name", f"{rtype} on {col}")

    result = {
        "rule_name": rname, "column": col, "rule_type": rtype,
        "expected": "", "actual": "", "status": "passed",
        "failing_rows_count": 0, "failing_row_examples": [],
    }

    try:
        # ── Dataset-level rules ──
        if rtype == "row_count_min":
            min_rows = int(params.get("value", 0))
            result["expected"] = f">= {min_rows} rows"
            result["actual"] = f"{len(df)} rows"
            if len(df) < min_rows:
                result["status"] = "warning" if severity == "warning" else "failed"
            return result

        if rtype == "no_duplicates":
            dupes = df.duplicated().sum()
            result["expected"] = "0 duplicate rows"
            result["actual"] = f"{dupes} duplicate rows"
            if dupes > 0:
                result["status"] = "warning" if severity == "warning" else "failed"
                result["failing_rows_count"] = int(dupes)
                result["failing_row_examples"] = df[df.duplicated()].head(5).fillna("").astype(str).values.tolist()
            return result

        # ── Column-level rules ──
        if col not in df.columns:
            result["status"] = "failed"
            result["actual"] = f"Column '{col}' not found"
            return result

        series = df[col]

        if rtype == "not_null":
            nulls = series.isna().sum()
            result["expected"] = "0 nulls"
            result["actual"] = f"{nulls} nulls"
            if nulls > 0:
                result["status"] = "warning" if severity == "warning" else "failed"
                result["failing_rows_count"] = int(nulls)
                result["failing_row_examples"] = df[series.isna()].head(5).fillna("").astype(str).values.tolist()

        elif rtype == "null_threshold":
            threshold = float(params.get("value", 5))
            pct = round(series.isna().mean() * 100, 2)
            result["expected"] = f"null% < {threshold}%"
            result["actual"] = f"{pct}%"
            if pct >= threshold:
                result["status"] = "warning" if severity == "warning" else "failed"
                result["failing_rows_count"] = int(series.isna().sum())
            elif pct >= threshold * 0.8:
                result["status"] = "warning"

        elif rtype == "unique":
            dupes = series.duplicated().sum()
            result["expected"] = "all unique"
            result["actual"] = f"{dupes} duplicates"
            if dupes > 0:
                result["status"] = "warning" if severity == "warning" else "failed"
                result["failing_rows_count"] = int(dupes)
                result["failing_row_examples"] = df[series.duplicated(keep=False)].head(5).fillna("").astype(str).values.tolist()

        elif rtype == "min_value":
            min_val = float(params.get("value", 0))
            actual_min = series.dropna().min()
            result["expected"] = f">= {min_val}"
            result["actual"] = f"min = {actual_min}"
            mask = series.dropna() < min_val
            if mask.any():
                result["status"] = "warning" if severity == "warning" else "failed"
                result["failing_rows_count"] = int(mask.sum())
                result["failing_row_examples"] = df.loc[series < min_val].head(5).fillna("").astype(str).values.tolist()

        elif rtype == "max_value":
            max_val = float(params.get("value", 0))
            actual_max = series.dropna().max()
            result["expected"] = f"<= {max_val}"
            result["actual"] = f"max = {actual_max}"
            mask = series.dropna() > max_val
            if mask.any():
                result["status"] = "warning" if severity == "warning" else "failed"
                result["failing_rows_count"] = int(mask.sum())
                result["failing_row_examples"] = df.loc[series > max_val].head(5).fillna("").astype(str).values.tolist()

        elif rtype == "between":
            lo = float(params.get("min", 0))
            hi = float(params.get("max", 100))
            result["expected"] = f"between {lo} and {hi}"
            mask = ~series.dropna().between(lo, hi)
            result["actual"] = f"{mask.sum()} values out of range"
            if mask.any():
                result["status"] = "warning" if severity == "warning" else "failed"
                result["failing_rows_count"] = int(mask.sum())
                result["failing_row_examples"] = df.loc[~series.between(lo, hi)].head(5).fillna("").astype(str).values.tolist()

        elif rtype == "regex_match":
            pattern = params.get("pattern", ".*")
            result["expected"] = f"matches /{pattern}/"
            s = series.dropna().astype(str)
            mask = ~s.str.match(pattern, na=False)
            result["actual"] = f"{mask.sum()} non-matching"
            if mask.any():
                result["status"] = "warning" if severity == "warning" else "failed"
                result["failing_rows_count"] = int(mask.sum())
                result["failing_row_examples"] = df.loc[mask[mask].index].head(5).fillna("").astype(str).values.tolist()

        elif rtype == "allowed_values":
            allowed = params.get("values", [])
            if isinstance(allowed, str):
                allowed = [v.strip() for v in allowed.split(",")]
            s = series.dropna().astype(str)
            mask = ~s.isin(allowed)
            result["expected"] = f"in {allowed}"
            result["actual"] = f"{mask.sum()} disallowed"
            if mask.any():
                result["status"] = "warning" if severity == "warning" else "failed"
                result["failing_rows_count"] = int(mask.sum())
                result["failing_row_examples"] = df.loc[mask[mask].index].head(5).fillna("").astype(str).values.tolist()

        elif rtype == "min_length":
            min_len = int(params.get("value", 1))
            s = series.dropna().astype(str)
            mask = s.str.len() < min_len
            result["expected"] = f"length >= {min_len}"
            result["actual"] = f"{mask.sum()} too short"
            if mask.any():
                result["status"] = "warning" if severity == "warning" else "failed"
                result["failing_rows_count"] = int(mask.sum())
                result["failing_row_examples"] = df.loc[mask[mask].index].head(5).fillna("").astype(str).values.tolist()

        elif rtype == "max_length":
            max_len = int(params.get("value", 255))
            s = series.dropna().astype(str)
            mask = s.str.len() > max_len
            result["expected"] = f"length <= {max_len}"
            result["actual"] = f"{mask.sum()} too long"
            if mask.any():
                result["status"] = "warning" if severity == "warning" else "failed"
                result["failing_rows_count"] = int(mask.sum())
                result["failing_row_examples"] = df.loc[mask[mask].index].head(5).fillna("").astype(str).values.tolist()

        else:
            result["actual"] = f"Unknown rule type: {rtype}"
            result["status"] = "failed"

    except Exception as e:
        result["status"] = "failed"
        result["actual"] = f"Error: {e}"

    return result
