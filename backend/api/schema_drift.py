"""
api/schema_drift.py — Schema Drift Detection
POST /api/schema-drift/compare   — compare two datasets' schemas
GET  /api/schema-drift/history/<dataset_id> — drift history for a dataset
"""
import os, json
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify
from flask_login import login_required, current_user
from backend.models import db, Dataset

schema_drift_bp = Blueprint("schema_drift", __name__)


def _load_profile(ds):
    """Return profile dict from Dataset.profile_json, or build one from disk."""
    pj = ds.profile_json or {}
    if "profile" in pj and isinstance(pj["profile"], dict):
        return pj["profile"]
    if pj:
        return pj
    # Fallback: read from disk
    if not ds.storage_path or not os.path.exists(ds.storage_path):
        return {}
    import pandas as pd
    ext = (ds.file_format or ds.name.rsplit(".", 1)[-1]).lower()
    try:
        if ext == "csv":       df = pd.read_csv(ds.storage_path, on_bad_lines="skip")
        elif ext in ("xlsx","xls"): df = pd.read_excel(ds.storage_path)
        elif ext == "parquet": df = pd.read_parquet(ds.storage_path)
        elif ext == "json":   df = pd.read_json(ds.storage_path)
        else:                 df = pd.read_csv(ds.storage_path, sep=None, engine="python", on_bad_lines="skip")
    except Exception:
        return {}
    profile = {}
    for col in df.columns:
        info = {"dtype": str(df[col].dtype), "missing_pct": round(df[col].isna().mean() * 100, 2)}
        if pd.api.types.is_numeric_dtype(df[col]):
            info["kind"] = "numeric"
            info["min"] = float(df[col].min()) if not df[col].isna().all() else None
            info["max"] = float(df[col].max()) if not df[col].isna().all() else None
            info["mean"] = float(df[col].mean()) if not df[col].isna().all() else None
        else:
            info["kind"] = "categorical"
            info["unique"] = int(df[col].nunique())
        profile[col] = info
    return profile


# Store comparison history in-memory (persists per container restart)
_drift_history: list[dict] = []


@schema_drift_bp.route("/compare", methods=["POST"])
@login_required
def compare_schemas():
    try:
        body = request.get_json(force=True)
        bid = body.get("baseline_dataset_id")
        nid = body.get("new_dataset_id")
        if not bid or not nid:
            return jsonify({"error": "Both baseline and new dataset IDs are required"}), 400

        bds = Dataset.query.filter_by(id=bid, user_id=current_user.id).first_or_404()
        nds = Dataset.query.filter_by(id=nid, user_id=current_user.id).first_or_404()

        bp = _load_profile(bds)
        np_ = _load_profile(nds)

        b_cols = {k for k in bp if k != "__meta__"}
        n_cols = {k for k in np_ if k != "__meta__"}

        added = sorted(n_cols - b_cols)
        removed = sorted(b_cols - n_cols)
        common = sorted(b_cols & n_cols)

        type_changes = []
        nullable_changes = []
        range_changes = []

        for col in common:
            bi, ni = bp.get(col, {}), np_.get(col, {})
            # Type changes
            bd, nd = str(bi.get("dtype", "")), str(ni.get("dtype", ""))
            if bd and nd and bd != nd:
                type_changes.append({"column": col, "old_type": bd, "new_type": nd})
            # Nullable changes (>10% difference)
            bn = bi.get("missing_pct", 0) or 0
            nn = ni.get("missing_pct", 0) or 0
            if abs(nn - bn) > 10:
                nullable_changes.append({"column": col, "old_null_pct": round(bn, 2), "new_null_pct": round(nn, 2)})
            # Range changes for numeric columns
            if bi.get("kind") == "numeric" and ni.get("kind") == "numeric":
                old_min = bi.get("min")
                old_max = bi.get("max")
                new_min = ni.get("min")
                new_max = ni.get("max")
                if old_min is not None and new_min is not None:
                    old_range = (old_max or 0) - (old_min or 0)
                    if old_range > 0:
                        min_shift = abs((new_min or 0) - (old_min or 0)) / old_range
                        max_shift = abs((new_max or 0) - (old_max or 0)) / old_range
                        if min_shift > 0.2 or max_shift > 0.2:
                            range_changes.append({
                                "column": col,
                                "old_min": round(old_min, 4), "old_max": round(old_max, 4),
                                "new_min": round(new_min, 4), "new_max": round(new_max, 4),
                            })

        # Drift score: 0-100
        total_cols = max(len(b_cols | n_cols), 1)
        score = 0
        score += len(added) / total_cols * 25          # added columns
        score += len(removed) / total_cols * 30        # removed columns (more severe)
        score += len(type_changes) / total_cols * 25   # type changes
        score += len(nullable_changes) / total_cols * 10
        score += len(range_changes) / total_cols * 10
        score = min(round(score * 100 / 100, 1), 100)

        if score == 0:
            severity = "none"
        elif score <= 15:
            severity = "low"
        elif score <= 45:
            severity = "medium"
        else:
            severity = "critical"

        # Column comparison table
        all_cols = sorted(b_cols | n_cols)
        comparison = []
        for col in all_cols:
            bi = bp.get(col, {})
            ni = np_.get(col, {})
            comparison.append({
                "column": col,
                "in_baseline": col in b_cols,
                "in_new": col in n_cols,
                "baseline_dtype": str(bi.get("dtype", "—")),
                "new_dtype": str(ni.get("dtype", "—")),
                "baseline_null_pct": round(bi.get("missing_pct", 0) or 0, 2),
                "new_null_pct": round(ni.get("missing_pct", 0) or 0, 2),
                "baseline_min": bi.get("min"),
                "baseline_max": bi.get("max"),
                "new_min": ni.get("min"),
                "new_max": ni.get("max"),
                "dtype_changed": str(bi.get("dtype", "")) != str(ni.get("dtype", "")) if col in common else False,
            })

        result = {
            "success": True,
            "baseline": {"id": bds.id, "name": bds.name, "columns": len(b_cols)},
            "new":      {"id": nds.id, "name": nds.name, "columns": len(n_cols)},
            "added_columns": added,
            "removed_columns": removed,
            "type_changes": type_changes,
            "nullable_changes": nullable_changes,
            "range_changes": range_changes,
            "drift_score": score,
            "severity": severity,
            "comparison": comparison,
            "compared_at": datetime.now(timezone.utc).isoformat(),
        }

        # Store in history
        _drift_history.append({
            "baseline_id": bds.id, "new_id": nds.id,
            "baseline_name": bds.name, "new_name": nds.name,
            "drift_score": score, "severity": severity,
            "compared_at": result["compared_at"],
            "user_id": current_user.id,
        })

        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@schema_drift_bp.route("/history/<int:dataset_id>", methods=["GET"])
@login_required
def drift_history(dataset_id):
    try:
        ds = Dataset.query.filter_by(id=dataset_id, user_id=current_user.id).first_or_404()
        relevant = [h for h in _drift_history
                    if h["user_id"] == current_user.id
                    and (h["baseline_id"] == dataset_id or h["new_id"] == dataset_id)]
        relevant.sort(key=lambda x: x["compared_at"], reverse=True)
        return jsonify({"dataset_id": dataset_id, "dataset_name": ds.name, "history": relevant})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
