"""
api/join.py — Visual Dataset Join / Merge
POST /api/join/preview  — preview first 50 rows of merged result
POST /api/join/execute  — full merge, save CSV, create Dataset record
"""
import os, uuid
from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required, current_user
from backend.models import db, Dataset

join_bp = Blueprint("join", __name__)


def _load_df(ds):
    """Load a Dataset into a pandas DataFrame."""
    import pandas as pd
    ext = (ds.file_format or ds.name.rsplit(".", 1)[-1]).lower()
    p = ds.storage_path
    if ext == "csv":       return pd.read_csv(p, on_bad_lines="skip")
    if ext in ("xlsx","xls"): return pd.read_excel(p)
    if ext == "parquet":   return pd.read_parquet(p)
    if ext == "json":      return pd.read_json(p)
    if ext in ("tsv","txt"): return pd.read_csv(p, sep="\t", on_bad_lines="skip")
    return pd.read_csv(p, sep=None, engine="python", on_bad_lines="skip")


def _parse_keys(body):
    """Extract left_key / right_key as lists (supports multi-key joins)."""
    lk = body.get("left_key", [])
    rk = body.get("right_key", [])
    if isinstance(lk, str): lk = [lk]
    if isinstance(rk, str): rk = [rk]
    return lk, rk


# ── columns for a dataset ────────────────────────────
@join_bp.route("/columns/<int:did>", methods=["GET"])
@login_required
def dataset_columns(did):
    try:
        ds = Dataset.query.filter_by(id=did, user_id=current_user.id).first_or_404()
        df = _load_df(ds)
        cols = [{"name": c, "dtype": str(df[c].dtype)} for c in df.columns]
        return jsonify({"columns": cols, "row_count": len(df)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── preview ──────────────────────────────────────────
@join_bp.route("/preview", methods=["POST"])
@login_required
def join_preview():
    try:
        body = request.get_json(force=True)
        lid  = body.get("left_dataset_id")
        rid  = body.get("right_dataset_id")
        jt   = body.get("join_type", "inner")
        lk, rk = _parse_keys(body)

        if not lid or not rid:
            return jsonify({"error": "Both left and right datasets are required"}), 400
        if not lk or not rk or len(lk) != len(rk):
            return jsonify({"error": "Matching left and right keys are required"}), 400
        if jt not in ("inner", "left", "right", "outer"):
            return jsonify({"error": f"Invalid join type: {jt}"}), 400

        lds = Dataset.query.filter_by(id=lid, user_id=current_user.id).first_or_404()
        rds = Dataset.query.filter_by(id=rid, user_id=current_user.id).first_or_404()
        ldf = _load_df(lds)
        rdf = _load_df(rds)

        merged = ldf.merge(rdf, left_on=lk, right_on=rk, how=jt, suffixes=("_left", "_right"))

        preview = merged.head(50).fillna("").astype(str)
        return jsonify({
            "success":     True,
            "join_type":   jt,
            "left_rows":   len(ldf),
            "right_rows":  len(rdf),
            "output_rows": len(merged),
            "output_cols": len(merged.columns),
            "new_columns": list(merged.columns),
            "preview": {
                "columns": preview.columns.tolist(),
                "rows":    preview.values.tolist(),
            },
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── execute ──────────────────────────────────────────
@join_bp.route("/execute", methods=["POST"])
@login_required
def join_execute():
    try:
        body = request.get_json(force=True)
        lid  = body.get("left_dataset_id")
        rid  = body.get("right_dataset_id")
        jt   = body.get("join_type", "inner")
        lk, rk = _parse_keys(body)

        if not lid or not rid:
            return jsonify({"error": "Both left and right datasets are required"}), 400
        if not lk or not rk or len(lk) != len(rk):
            return jsonify({"error": "Matching left and right keys are required"}), 400

        lds = Dataset.query.filter_by(id=lid, user_id=current_user.id).first_or_404()
        rds = Dataset.query.filter_by(id=rid, user_id=current_user.id).first_or_404()
        ldf = _load_df(lds)
        rdf = _load_df(rds)

        merged = ldf.merge(rdf, left_on=lk, right_on=rk, how=jt, suffixes=("_left", "_right"))

        # Save result
        uid      = uuid.uuid4().hex[:10]
        out_name = f"{uid}_joined.csv"
        out_dir  = current_app.config["OUTPUT_FOLDER"]
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, out_name)
        merged.to_csv(out_path, index=False)
        fsize = os.path.getsize(out_path)

        # Create Dataset record
        ds = Dataset(
            user_id      = current_user.id,
            name         = f"joined_{lds.name.split('.')[0]}_{rds.name.split('.')[0]}.csv",
            file_format  = "csv",
            file_size    = fsize,
            row_count    = len(merged),
            col_count    = len(merged.columns),
            storage_path = out_path,
        )
        db.session.add(ds)
        db.session.commit()

        return jsonify({
            "success":      True,
            "dataset_id":   ds.id,
            "row_count":    len(merged),
            "col_count":    len(merged.columns),
            "file_size":    fsize,
            "download_url": f"/api/datasets/{ds.id}/download",
            "dataset":      ds.to_dict(),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
