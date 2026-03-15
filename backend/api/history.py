"""
api/history.py — Pipeline run history & stats
"""
from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user
from backend.models import PipelineRun, Dataset, QualityCheck, Transformation

history_bp = Blueprint("history", __name__)


@history_bp.get("/runs")
@login_required
def list_runs():
    runs = (PipelineRun.query
            .filter_by(user_id=current_user.id)
            .order_by(PipelineRun.started_at.desc())
            .limit(50).all())
    return jsonify([r.to_dict() for r in runs])


@history_bp.get("/runs/<run_id>")
@login_required
def get_run(run_id):
    run = PipelineRun.query.filter_by(
        run_id=run_id, user_id=current_user.id
    ).first_or_404()
    return jsonify(run.to_dict())


@history_bp.get("/quality")
@login_required
def quality_history():
    checks = (QualityCheck.query
              .join(Dataset)
              .filter(Dataset.user_id == current_user.id)
              .order_by(QualityCheck.checked_at.desc())
              .limit(30).all())
    return jsonify([c.to_dict() for c in checks])


@history_bp.get("/transformations")
@login_required
def transform_history():
    txns = (Transformation.query
            .join(Dataset)
            .filter(Dataset.user_id == current_user.id)
            .order_by(Transformation.created_at.desc())
            .limit(50).all())
    return jsonify([t.to_dict() for t in txns])


@history_bp.get("/stats")
@login_required
def dashboard_stats():
    from backend.models import db
    from sqlalchemy import func
    uid = current_user.id

    total_runs     = PipelineRun.query.filter_by(user_id=uid).count()
    success_runs   = PipelineRun.query.filter_by(user_id=uid, status="success").count()
    total_datasets = Dataset.query.filter_by(user_id=uid).count()
    total_rows     = db.session.query(
        func.sum(PipelineRun.input_rows)
    ).filter_by(user_id=uid).scalar() or 0
    rows_cleaned   = db.session.query(
        func.sum(PipelineRun.output_rows)
    ).filter_by(user_id=uid).scalar() or 0

    recent_runs = (PipelineRun.query
                   .filter_by(user_id=uid)
                   .order_by(PipelineRun.started_at.desc())
                   .limit(10).all())

    return jsonify({
        "total_runs":      total_runs,
        "success_runs":    success_runs,
        "failed_runs":     total_runs - success_runs,
        "success_rate":    round(success_runs/total_runs*100, 1) if total_runs else 0,
        "total_datasets":  total_datasets,
        "total_rows_processed": int(total_rows),
        "rows_cleaned":    int(rows_cleaned),
        "rows_removed":    int(total_rows - rows_cleaned),
        "recent_runs":     [r.to_dict() for r in recent_runs],
    })
