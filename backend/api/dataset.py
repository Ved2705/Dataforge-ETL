"""
api/dataset.py — Dataset upload, profile, and download routes
"""

from __future__ import annotations
import os, uuid, time
from flask import Blueprint, request, jsonify, send_file, current_app
from flask_login import login_required, current_user
from backend.models import db, Dataset
from backend.etl.pipeline import (
    extract, profile, build_preview,
    compute_correlation, detect_duplicates, run_quality_checks
)

dataset_bp = Blueprint("dataset", __name__)

ALLOWED = {"csv","json","xlsx","xls","tsv","txt","parquet"}


def _allowed(fname):
    return "." in fname and fname.rsplit(".",1)[-1].lower() in ALLOWED


@dataset_bp.post("/upload")
@login_required
def upload():
    if "file" not in request.files:
        return jsonify({"error": "No file field"}), 400
    f = request.files["file"]
    if not f.filename or not _allowed(f.filename):
        return jsonify({"error": f"Unsupported format. Allowed: {', '.join(ALLOWED)}"}), 400

    ds_id    = uuid.uuid4().hex[:12]
    ext      = f.filename.rsplit(".",1)[-1].lower()
    save_name= f"{ds_id}.{ext}"
    filepath = os.path.join(current_app.config["UPLOAD_FOLDER"], save_name)
    f.save(filepath)

    try:
        use_spark = request.form.get("use_spark", "false").lower() == "true"
        df, meta  = extract(filepath, f.filename, use_spark=use_spark)

        if df.empty:
            os.remove(filepath)
            return jsonify({"error": "File is empty"}), 400

        profile_data = profile(df)
        preview      = build_preview(df)
        corr         = compute_correlation(df, profile_data)
        dupes        = detect_duplicates(df)
        quality      = run_quality_checks(df, profile_data)

        # infer schema
        schema = {
            col: {
                "dtype": str(df[col].dtype),
                "kind":  profile_data[col]["kind"],
                "nullable": bool(df[col].isna().any()),
            }
            for col in df.columns
        }

        # persist to DB
        dataset = Dataset(
            user_id=current_user.id,
            ds_id=ds_id,
            name=f.filename,
            file_format=ext,
            file_size=os.path.getsize(filepath),
            row_count=len(df),
            col_count=len(df.columns),
            schema_json=schema,
            profile_json=profile_data,
            preview_json=preview,
            storage_path=filepath,
            status="profiled",
        )
        db.session.add(dataset)
        db.session.commit()

        return jsonify({
            "success":        True,
            "ds_id":          ds_id,
            "dataset_id":     dataset.id,
            "original_name":  f.filename,
            "shape":          {"rows": len(df), "cols": len(df.columns)},
            "extract_meta":   meta,
            "schema":         schema,
            "profile":        profile_data,
            "preview":        preview,
            "duplicates":     dupes,
            "correlation":    corr,
            "quality":        quality,
        })

    except Exception as e:
        if os.path.exists(filepath):
            os.remove(filepath)
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@dataset_bp.get("/")
@login_required
def list_datasets():
    datasets = (
        Dataset.query
        .filter_by(user_id=current_user.id)
        .order_by(Dataset.uploaded_at.desc())
        .limit(50).all()
    )
    return jsonify([d.to_dict() for d in datasets])


@dataset_bp.get("/<ds_id>")
@login_required
def get_dataset(ds_id):
    ds = Dataset.query.filter_by(ds_id=ds_id, user_id=current_user.id).first_or_404()
    return jsonify(ds.to_dict(full=True))


@dataset_bp.delete("/<ds_id>")
@login_required
def delete_dataset(ds_id):
    ds = Dataset.query.filter_by(ds_id=ds_id, user_id=current_user.id).first_or_404()
    if ds.storage_path and os.path.exists(ds.storage_path):
        os.remove(ds.storage_path)
    db.session.delete(ds)
    db.session.commit()
    return jsonify({"success": True})


@dataset_bp.get("/<ds_id>/download")
@login_required
def download_cleaned(ds_id):
    from backend.models import PipelineRun
    run = (
        PipelineRun.query
        .filter_by(dataset_id=Dataset.query.filter_by(ds_id=ds_id).first().id)
        .order_by(PipelineRun.started_at.desc())
        .first()
    )
    if not run:
        return jsonify({"error": "No pipeline run found"}), 404

    from backend.models import CleanedFile
    cf = CleanedFile.query.filter_by(upload_id=run.id).first()
    if not cf or not os.path.exists(cf.filename):
        return jsonify({"error": "Cleaned file not found"}), 404

    return send_file(cf.filename, as_attachment=True,
                     download_name=f"cleaned_{ds_id}.csv")
