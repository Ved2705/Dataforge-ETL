"""
app.py — DataForge ETL Platform — Flask Backend
All API routes for: auth, datasets, pipelines, runs, logs, DQ, Airflow status
"""
import os, uuid, json, traceback, threading
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, request, jsonify, render_template, send_file, g, redirect, url_for
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
from flask_migrate import Migrate
from flask_cors import CORS
from dotenv import load_dotenv
from backend.api.ai import ai_bp
from backend.api.templates import templates_bp
from backend.api.scheduler import scheduler_bp
from backend.api.join import join_bp
from backend.api.schema_drift import schema_drift_bp
from backend.api.dq_rules import dq_rules_bp
from backend.api.dataset import dataset_bp

load_dotenv()

from backend.models import db, bcr, User, LoginSession, Dataset, Pipeline, PipelineRun, RunLog, DQResult, DQRuleSet
from backend.pipeline.profiler import profile_dataframe, analyze_missing, detect_duplicates, detect_outliers, compute_correlation
from backend.pipeline.transforms import apply_transform
from backend.pipeline.dq import run_great_expectations, _default_suite
from backend.pipeline.airflow_client import list_dags, list_dag_runs, trigger_airflow_dag

# ─── App setup ───────────────────────────────────────
app = Flask(__name__, template_folder="../frontend/templates",
                      static_folder="../frontend/static")
app.url_map.strict_slashes = False

app.config["SECRET_KEY"]              = os.getenv("SECRET_KEY", "dev-secret-change-me")
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", "sqlite:///dataforge.db")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["MAX_CONTENT_LENGTH"]      = int(os.getenv("MAX_CONTENT_LENGTH", 52428800))
app.config["UPLOAD_FOLDER"]           = os.getenv("UPLOAD_FOLDER", "data/raw")
app.config["OUTPUT_FOLDER"]           = os.getenv("OUTPUT_FOLDER", "data/processed")

os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
os.makedirs(app.config["OUTPUT_FOLDER"], exist_ok=True)

db.init_app(app)
bcr.init_app(app)
CORS(app)
Migrate(app, db)

login_manager = LoginManager(app)
login_manager.login_view = "index"

@login_manager.user_loader
def load_user(uid): return User.query.get(int(uid))

@login_manager.unauthorized_handler
def unauthorized():
    if request.path.startswith("/api/"):
        return jsonify({"error": "Login required"}), 401
    return redirect(url_for("index"))

ALLOWED = {"csv", "json", "xlsx", "xls", "tsv", "txt", "parquet"}

def allowed(filename):
    return "." in filename and filename.rsplit(".", 1)[-1].lower() in ALLOWED

# ─── Register Blueprints ─────────────────────────────
app.register_blueprint(ai_bp,          url_prefix="/api/ai")
app.register_blueprint(templates_bp,   url_prefix="/api/templates")
app.register_blueprint(scheduler_bp,   url_prefix="/api/scheduler")
app.register_blueprint(join_bp,        url_prefix="/api/join")
app.register_blueprint(schema_drift_bp, url_prefix="/api/schema-drift")
app.register_blueprint(dq_rules_bp,    url_prefix="/api/dq-rules")
app.register_blueprint(dataset_bp, url_prefix="/api/datasets")


# ─── DB init ─────────────────────────────────────────
with app.app_context():
    db.create_all()

# ═════════════════════════════════════════════════════
# FRONTEND
# ═════════════════════════════════════════════════════
@app.route("/")
def landing():
    return render_template("landing.html")

@app.route("/app")
def index():
    # Landing page "Sign In" uses ?fresh=1 to force a clean login
    if request.args.get("fresh") == "1" and current_user.is_authenticated:
        logout_user()
    return render_template("index.html")

# ═════════════════════════════════════════════════════
# AUTH
# ═════════════════════════════════════════════════════
@app.route("/api/auth/register", methods=["POST"])
def register():
    d = request.json or {}
    if not d.get("username") or not d.get("email") or not d.get("password"):
        return jsonify({"error": "username, email and password required"}), 400
    if User.query.filter((User.username == d["username"]) | (User.email == d["email"])).first():
        return jsonify({"error": "Username or email already exists"}), 409
    u = User(username=d["username"], email=d["email"])
    u.set_password(d["password"])
    db.session.add(u); db.session.commit()
    login_user(u)
    _record_login(u)
    return jsonify({"success": True, "user": u.to_dict()}), 201

@app.route("/api/auth/login", methods=["POST"])
def login():
    d = request.json or {}
    u = User.query.filter_by(username=d.get("username")).first()
    if not u or not u.check_password(d.get("password", "")):
        return jsonify({"error": "Invalid credentials"}), 401
    u.last_login = datetime.now(timezone.utc)
    db.session.commit()
    login_user(u)
    _record_login(u)
    return jsonify({"success": True, "user": u.to_dict()})

@app.route("/api/auth/logout", methods=["POST"])
@login_required
def logout():
    sess = LoginSession.query.filter_by(user_id=current_user.id, logged_out=None).first()
    if sess:
        sess.logged_out = datetime.now(timezone.utc)
        db.session.commit()
    logout_user()
    return jsonify({"success": True})

@app.route("/api/auth/me")
@login_required
def me():
    return jsonify(current_user.to_dict())

def _record_login(u):
    s = LoginSession(user_id=u.id,
                     ip_address=request.remote_addr,
                     user_agent=request.headers.get("User-Agent", "")[:300])
    db.session.add(s); db.session.commit()

# ═════════════════════════════════════════════════════
# DATASETS
# ═════════════════════════════════════════════════════
@app.route("/api/datasets", methods=["GET"])
@login_required
def list_datasets():
    ds = Dataset.query.filter_by(user_id=current_user.id).order_by(Dataset.uploaded_at.desc()).all()
    
    # Deduplicate by original_name or name
    seen = {}
    unique_ds = []
    for d in ds:
        key = (d.original_name or d.name or "").lower().strip()
        if key not in seen:
            seen[key] = True
            unique_ds.append(d)
    
    return jsonify([d.to_dict() for d in unique_ds])

@app.route("/api/datasets/upload", methods=["POST"])
@login_required
def upload_dataset():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    f = request.files["file"]
    if not f.filename or not allowed(f.filename):
        return jsonify({"error": f"Invalid file type. Allowed: {', '.join(ALLOWED)}"}), 400

    uid      = uuid.uuid4().hex[:10]
    ext      = f.filename.rsplit(".", 1)[-1].lower()
    savename = f"{uid}_{f.filename}"
    savepath = os.path.join(app.config["UPLOAD_FOLDER"], savename)
    f.save(savepath)
    fsize = os.path.getsize(savepath)

    try:
        import pandas as pd, time as _time
        t0 = _time.time()
        df = _load_file(savepath, ext)
        extract_dur = round(_time.time() - t0, 2)

        use_spark = request.form.get("use_spark", "false").lower() == "true"
        engine = "pyspark" if use_spark else "pandas"

        profile  = profile_dataframe(df)
        missing  = analyze_missing(df)
        dupes    = detect_duplicates(df)
        outliers = detect_outliers(df, profile)
        corr     = compute_correlation(df, profile)

        # preview (first 200 rows, max 25 cols)
        pv_df = df.head(200).fillna("").astype(str)
        if len(pv_df.columns) > 25:
            pv_df = pv_df.iloc[:, :25]
        preview = {"columns": pv_df.columns.tolist(), "rows": pv_df.values.tolist()}

        ds = Dataset(
            user_id      = current_user.id,
            name         = f.filename,
            file_format  = ext,
            file_size    = fsize,
            row_count    = len(df),
            col_count    = len(df.columns),
            storage_path = savepath,
            profile_json = {
                "profile": profile, "missing": missing,
                "duplicates": dupes, "outliers": outliers,
                "correlation": corr, "preview": preview,
            },
        )
        db.session.add(ds); db.session.commit()

        return jsonify({
            "success":      True,
            "ds_id":        uid,
            "dataset_id":   ds.id,
            "dataset":      ds.to_dict(),
            "shape":        {"rows": len(df), "cols": len(df.columns)},
            "extract_meta": {"engine": engine, "duration_s": extract_dur,
                             "rows": len(df), "cols": len(df.columns)},
            "profile":      profile,
            "missing":      missing,
            "duplicates":   dupes,
            "outliers":     outliers,
            "correlation":  corr,
            "preview":      preview,
        })

    except Exception as e:
        traceback.print_exc()
        if os.path.exists(savepath): os.remove(savepath)
        return jsonify({"error": str(e)}), 500

@app.route("/api/datasets/<int:did>", methods=["GET"])
@login_required
def get_dataset(did):
    ds = Dataset.query.filter_by(id=did, user_id=current_user.id).first_or_404()
    return jsonify(ds.to_dict())

@app.route("/api/datasets/<int:did>", methods=["DELETE"])
@login_required
def delete_dataset(did):
    ds = Dataset.query.filter_by(id=did, user_id=current_user.id).first_or_404()
    if ds.storage_path and os.path.exists(ds.storage_path):
        os.remove(ds.storage_path)
    db.session.delete(ds); db.session.commit()
    return jsonify({"success": True})

@app.route("/api/datasets/<int:did>/download", methods=["GET"])
@login_required
def download_dataset(did):
    ds = Dataset.query.filter_by(id=did, user_id=current_user.id).first_or_404()
    if not ds.storage_path or not os.path.exists(ds.storage_path):
        return jsonify({"error": "File not found"}), 404
    # Preserve original filename with correct extension
    return send_file(
        ds.storage_path,
        as_attachment=True,
        download_name=ds.name,
        mimetype="text/csv",
    )

@app.route("/api/datasets/<int:did>/dq", methods=["POST"])
@login_required
def quick_dq(did):
    ds   = Dataset.query.filter_by(id=did, user_id=current_user.id).first_or_404()
    ext  = ds.file_format
    df   = _load_file(ds.storage_path, ext)
    cfg  = request.json or {}
    suite= cfg.get("suite") or _default_suite(df)
    res  = run_great_expectations(df, {"suite": suite})
    return jsonify(res)

@app.route("/api/datasets/<int:did>/transform_preview", methods=["POST"])
@login_required
def transform_preview(did):
    ds  = Dataset.query.filter_by(id=did, user_id=current_user.id).first_or_404()
    df  = _load_file(ds.storage_path, ds.file_format)
    cfg = request.json or {}
    df2, log = apply_transform(df, cfg)
    pv  = df2.head(100).fillna("").astype(str)
    if len(pv.columns) > 20: pv = pv.iloc[:, :20]
    return jsonify({
        "rows_before": len(df), "rows_after": len(df2),
        "cols_before": len(df.columns), "cols_after": len(df2.columns),
        "log": log,
        "preview": {"columns": pv.columns.tolist(), "rows": pv.values.tolist()},
    })

# ═════════════════════════════════════════════════════
# PIPELINES
# ═════════════════════════════════════════════════════
@app.route("/api/pipelines", methods=["GET"])
@login_required
def list_pipelines():
    pls = Pipeline.query.filter_by(user_id=current_user.id).order_by(Pipeline.created_at.desc()).all()
    return jsonify([p.to_dict() for p in pls])

@app.route("/api/pipelines", methods=["POST"])
@login_required
def create_pipeline():
    d = request.json or {}
    if not d.get("name"):
        return jsonify({"error": "Pipeline name required"}), 400
    pl = Pipeline(user_id=current_user.id, name=d["name"],
                  description=d.get("description", ""),
                  steps_json=d.get("steps", []),
                  schedule=d.get("schedule"))
    db.session.add(pl); db.session.commit()
    return jsonify(pl.to_dict()), 201

@app.route("/api/pipelines/<int:pid>", methods=["GET"])
@login_required
def get_pipeline(pid):
    pl = Pipeline.query.filter_by(id=pid, user_id=current_user.id).first_or_404()
    return jsonify(pl.to_dict())

@app.route("/api/pipelines/<int:pid>", methods=["PUT"])
@login_required
def update_pipeline(pid):
    pl = Pipeline.query.filter_by(id=pid, user_id=current_user.id).first_or_404()
    d  = request.json or {}
    if "name"        in d: pl.name        = d["name"]
    if "description" in d: pl.description = d["description"]
    if "steps"       in d: pl.steps_json  = d["steps"]
    if "schedule"    in d: pl.schedule    = d["schedule"]
    db.session.commit()
    return jsonify(pl.to_dict())

@app.route("/api/pipelines/<int:pid>", methods=["DELETE"])
@login_required
def delete_pipeline(pid):
    pl = Pipeline.query.filter_by(id=pid, user_id=current_user.id).first_or_404()
    db.session.delete(pl); db.session.commit()
    return jsonify({"success": True})

# ═════════════════════════════════════════════════════
# PIPELINE RUNS  (new-style: via saved Pipeline record)
# ═════════════════════════════════════════════════════
@app.route("/api/pipelines/<int:pid>/run", methods=["POST"])
@login_required
def trigger_run(pid):
    from backend.pipeline.engine import run_pipeline as _run_pipeline
    pl = Pipeline.query.filter_by(id=pid, user_id=current_user.id).first_or_404()
    d  = request.json or {}

    dataset_id = d.get("dataset_id")
    ds = Dataset.query.filter_by(id=dataset_id, user_id=current_user.id).first() if dataset_id else \
         Dataset.query.filter_by(user_id=current_user.id).order_by(Dataset.uploaded_at.desc()).first()
    if not ds:
        return jsonify({"error": "No dataset found"}), 404

    run = PipelineRun(pipeline_id=pl.id, dataset_id=ds.id, trigger="manual")
    db.session.add(run); db.session.commit()

    def bg():
        with app.app_context():
            try:
                _run_pipeline(pl, ds, run, db, app.config["OUTPUT_FOLDER"])
            except Exception:
                pass

    threading.Thread(target=bg, daemon=True).start()
    return jsonify({"success": True, "run_id": run.id, "status": "queued"}), 202

@app.route("/api/runs/<int:rid>", methods=["GET"])
@login_required
def get_run(rid):
    run = PipelineRun.query.join(Pipeline).filter(
        PipelineRun.id == rid, Pipeline.user_id == current_user.id
    ).first_or_404()
    return jsonify({**run.to_dict(),
                    "logs": [l.to_dict() for l in run.logs],
                    "dq":   run.dq_result.to_dict() if run.dq_result else None})

@app.route("/api/runs/<int:rid>/logs", methods=["GET"])
@login_required
def get_run_logs(rid):
    run  = PipelineRun.query.join(Pipeline).filter(
        PipelineRun.id == rid, Pipeline.user_id == current_user.id
    ).first_or_404()
    since = request.args.get("since_id", 0, type=int)
    logs  = RunLog.query.filter(RunLog.run_id == rid, RunLog.id > since).all()
    return jsonify([l.to_dict() for l in logs])

@app.route("/api/pipelines/<int:pid>/runs", methods=["GET"])
@login_required
def pipeline_run_history(pid):
    pl   = Pipeline.query.filter_by(id=pid, user_id=current_user.id).first_or_404()
    runs = PipelineRun.query.filter_by(pipeline_id=pid).order_by(PipelineRun.started_at.desc()).limit(50).all()
    return jsonify([r.to_dict() for r in runs])

# ─── Download: new-style integer run ID ──────────────
@app.route("/api/runs/<int:rid>/download", methods=["GET"])
@login_required
def download_run_output(rid):
    run = PipelineRun.query.join(Pipeline).filter(
        PipelineRun.id == rid, Pipeline.user_id == current_user.id
    ).first_or_404()

    if not run.output_path or not os.path.exists(run.output_path):
        return jsonify({"error": "Output file not found"}), 404

    fmt = request.args.get("format", "csv").lower()

    if fmt == "parquet":
        try:
            import pandas as pd
            parquet_path = run.output_path.replace(".csv", ".parquet")
            if not os.path.exists(parquet_path):
                pd.read_csv(run.output_path).to_parquet(parquet_path, index=False)
            return send_file(
                parquet_path,
                as_attachment=True,
                download_name=f"dataforge_run_{rid}_cleaned.parquet",
                mimetype="application/octet-stream",
            )
        except Exception as e:
            return jsonify({"error": f"Parquet conversion failed: {e}"}), 500

    # Default: always force .csv filename + correct MIME type
    return send_file(
        run.output_path,
        as_attachment=True,
        download_name=f"dataforge_run_{rid}_cleaned.csv",
        mimetype="text/csv",
    )

# ═════════════════════════════════════════════════════
# PIPELINE RUN  (legacy: used by old main.js)
# ═════════════════════════════════════════════════════
@app.route("/api/pipeline/run", methods=["POST"])
@login_required
def run_pipeline_legacy():
    import time as _time
    from backend.etl.pipeline import (
        extract as etl_extract, profile as etl_profile,
        transform as etl_transform, load as etl_load,
        run_quality_checks, generate_dbt_model,
        trigger_airflow_dag as etl_trigger_airflow,
        compute_correlation, build_preview, detect_duplicates,
    )

    body  = request.get_json(force=True)
    ds_id = body.get("ds_id")
    steps = body.get("steps") or [
        {"type": "snake_case_columns",  "config": {}},
        {"type": "drop_duplicates",     "config": {}},
        {"type": "drop_missing",        "config": {"threshold_pct": 80}},
        {"type": "fill_missing",        "config": {"strategy": "median"}},
        {"type": "remove_outliers",     "config": {"method": "iqr", "threshold": 1.5}},
    ]
    use_spark       = body.get("use_spark", False)
    trigger_airflow = body.get("trigger_airflow", False)
    gen_dbt         = body.get("generate_dbt", False)

    ds = Dataset.query.filter_by(user_id=current_user.id).filter(
        Dataset.storage_path.ilike(f"%{ds_id}%")
    ).first()
    if not ds:
        return jsonify({"error": "Dataset not found"}), 404

    run_id     = str(uuid.uuid4())
    wall_start = _time.time()
    task_results = []

    try:
        df, meta = etl_extract(ds.storage_path, ds.name, use_spark=use_spark)
        task_results.append({"task_name": "extract", "status": "success",
                             "logs": f"Extracted {meta['rows']} rows × {meta['cols']} cols via {meta['engine']}"})

        profile_data = etl_profile(df)
        dupes = detect_duplicates(df)
        corr  = compute_correlation(df, profile_data)
        task_results.append({"task_name": "profile", "status": "success",
                             "logs": f"Profiled {len(df.columns)} columns. {dupes['duplicate_rows']} duplicates found."})

        quality = run_quality_checks(df, profile_data)
        task_results.append({"task_name": "quality_check", "status": "success",
                             "logs": f"{quality['engine']}: {quality['passed']}/{quality['total']} checks passed ({quality['success_pct']}%)"})

        df_clean, transform_log = etl_transform(df, steps)
        task_results.append({"task_name": "transform", "status": "success",
                             "logs": "\n".join(f"[{e['status'].upper()}] {e['step']}: {e.get('rows_before','')}→{e.get('rows_after','')}" for e in transform_log)})

        dbt_result = None
        if gen_dbt:
            dbt_result = generate_dbt_model(df_clean, ds.name.split(".")[0], transform_log)
            task_results.append({"task_name": "dbt_generate", "status": "success",
                                 "logs": f"Generated dbt model: {dbt_result['model_name']}"})

        load_result = etl_load(df_clean, app.config["OUTPUT_FOLDER"], ds_id)
        task_results.append({"task_name": "load", "status": "success",
                             "logs": f"CSV: {load_result['csv_path']} ({load_result['csv_size_kb']} KB)"})

        # Stash paths so /api/pipeline/download/<run_id> can find them
        if not hasattr(app, '_download_map'):
            app._download_map = {}
        app._download_map[f"{run_id}_csv"] = load_result["csv_path"]
        if load_result.get("parquet_path"):
            app._download_map[f"{run_id}_parquet"] = load_result["parquet_path"]

        airflow_result = None
        if trigger_airflow:
            airflow_result = etl_trigger_airflow(
                dag_id=f"dataforge_etl_{ds_id}",
                conf={"ds_id": ds_id, "run_id": run_id, "output": load_result["csv_path"]},
            )
            task_results.append({"task_name": "airflow_trigger",
                                 "status": "success" if airflow_result["success"] else "failed",
                                 "logs": f"Airflow DAG triggered. run_id={airflow_result.get('run_id')}"})

        duration = round(_time.time() - wall_start, 2)
        missing  = {col: {"count": int(df[col].isna().sum()),
                          "pct": round(df[col].isna().sum() / len(df) * 100, 2)}
                    for col in df.columns}

        db_run = PipelineRun(
            run_id=run_id,
            user_id=current_user.id,
            dataset_id=ds.id,
            status="success",
            trigger="manual",
            rows_in=len(df),
            rows_out=len(df_clean),
            duration_s=duration,
            output_path=load_result.get("csv_path"),
            finished_at=datetime.now(timezone.utc),
            pipeline_config={"ds_id": ds_id, "steps": [s.get("type") for s in steps]},
        )
        db.session.add(db_run)
        db.session.commit()

        return jsonify({
            "success":          True,
            "run_id":           run_id,
            "duration_s":       duration,
            "input_rows":       len(df),
            "output_rows":      len(df_clean),
            "rows_removed":     len(df) - len(df_clean),
            "tasks":            task_results,
            "quality":          quality,
            "transform_log":    transform_log,
            "dbt":              dbt_result,
            "airflow":          airflow_result,
            "missing":          missing,
            "download_csv":     f"/api/pipeline/download/{run_id}",
            "download_parquet": f"/api/pipeline/download/{run_id}?format=parquet",
            "profile":          profile_data,
            "correlation":      corr,
            "preview":          build_preview(df_clean),
        })

    except Exception as e:
        traceback.print_exc()
        try:
            db_run = PipelineRun(
                run_id=run_id,
                user_id=current_user.id,
                dataset_id=ds.id if ds else None,
                status="failed",
                trigger="manual",
                duration_s=round(_time.time() - wall_start, 2),
                finished_at=datetime.now(timezone.utc),
                pipeline_config={"ds_id": ds_id, "error": str(e)},
            )
            db.session.add(db_run)
            db.session.commit()
        except Exception:
            db.session.rollback()
        return jsonify({"error": str(e), "run_id": run_id}), 500


# ─── Download: legacy string run_id ──────────────────
@app.route("/api/pipeline/download/<run_id>", methods=["GET"])
@login_required
def download_pipeline_output(run_id):
    fmt     = request.args.get("format", "csv").lower()
    ext     = "parquet" if fmt == "parquet" else "csv"
    mime    = "application/octet-stream" if ext == "parquet" else "text/csv"
    dl_name = f"dataforge_{run_id[:8]}_cleaned.{ext}"

    # 1) In-memory map (populated right after the run completes)
    mapping = getattr(app, '_download_map', {})
    key = f"{run_id}_{fmt}"
    if key in mapping and os.path.exists(mapping[key]):
        return send_file(mapping[key], as_attachment=True,
                         download_name=dl_name, mimetype=mime)

    # 2) DB lookup by string run_id field
    try:
        db_run = PipelineRun.query.filter_by(run_id=run_id).first()
        if db_run and db_run.output_path and os.path.exists(db_run.output_path):
            path = db_run.output_path
            if ext == "parquet":
                parquet_path = path.replace(".csv", ".parquet")
                if not os.path.exists(parquet_path):
                    import pandas as pd
                    pd.read_csv(path).to_parquet(parquet_path, index=False)
                path = parquet_path
            return send_file(path, as_attachment=True,
                             download_name=dl_name, mimetype=mime)
    except Exception:
        pass

    # 3) Glob fallback — most recent cleaned file
    import glob
    pattern = os.path.join(app.config["OUTPUT_FOLDER"], f"*_cleaned.{ext}")
    files   = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    if files:
        return send_file(files[0], as_attachment=True,
                         download_name=dl_name, mimetype=mime)

    return jsonify({"error": "Output file not found. Run a pipeline first."}), 404


@app.route("/api/pipeline/download-latest", methods=["GET"])
@login_required
def download_latest_output():
    """Download the most recently created cleaned CSV or Parquet."""
    fmt  = request.args.get("format", "csv").lower()
    ext  = "parquet" if fmt == "parquet" else "csv"
    mime = "application/octet-stream" if ext == "parquet" else "text/csv"
    import glob
    pattern = os.path.join(app.config["OUTPUT_FOLDER"], f"*_cleaned.{ext}")
    files   = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    if files:
        return send_file(files[0], as_attachment=True,
                         download_name=os.path.basename(files[0]),
                         mimetype=mime)
    return jsonify({"error": "No cleaned files found. Run a pipeline first."}), 404


@app.route("/api/pipeline/spark-status", methods=["GET"])
def spark_status():
    from backend.etl.pipeline import get_spark
    spark = get_spark()
    if spark:
        return jsonify({"available": True,
                        "master":   spark.conf.get("spark.master"),
                        "app_name": spark.conf.get("spark.app.name")})
    return jsonify({"available": False})

# ═════════════════════════════════════════════════════
# AIRFLOW STATUS — # DISABLED
# ═════════════════════════════════════════════════════
@app.route("/api/airflow/dags", methods=["GET"])
@login_required
def airflow_dags():
    # DISABLED — return jsonify(list_dags())
    return jsonify({"status": "disabled"})

@app.route("/api/airflow/dags/<dag_id>/runs", methods=["GET"])
@login_required
def airflow_dag_runs(dag_id):
    # DISABLED — return jsonify(list_dag_runs(dag_id))
    return jsonify({"status": "disabled"})

@app.route("/api/airflow/dags/<dag_id>/trigger", methods=["POST"])
@login_required
def airflow_trigger(dag_id):
    # DISABLED — cfg = request.json or {}
    # DISABLED — return jsonify(trigger_airflow_dag(dag_id, cfg))
    return jsonify({"status": "disabled"})

# ═════════════════════════════════════════════════════
# DASHBOARD STATS
# ═════════════════════════════════════════════════════
@app.route("/api/dashboard", methods=["GET"])
@login_required
def dashboard():
    uid = current_user.id
    pipelines   = Pipeline.query.filter_by(user_id=uid).count()
    datasets    = Dataset.query.filter_by(user_id=uid).count()
    total_runs  = PipelineRun.query.join(Pipeline).filter(Pipeline.user_id == uid).count()
    success     = PipelineRun.query.join(Pipeline).filter(Pipeline.user_id == uid, PipelineRun.status == "success").count()
    failed      = PipelineRun.query.join(Pipeline).filter(Pipeline.user_id == uid, PipelineRun.status == "failed").count()
    recent_runs = PipelineRun.query.join(Pipeline).filter(Pipeline.user_id == uid) \
                    .order_by(PipelineRun.started_at.desc()).limit(10).all()
    return jsonify({
        "pipelines":    pipelines,
        "datasets":     datasets,
        "total_runs":   total_runs,
        "success_runs": success,
        "failed_runs":  failed,
        "success_rate": round(success / total_runs * 100, 1) if total_runs else 0,
        "recent_runs":  [r.to_dict() for r in recent_runs],
    })

# ═════════════════════════════════════════════════════
# HISTORY  (used by main.js loadHistory)
# ═════════════════════════════════════════════════════
@app.route("/api/history/stats", methods=["GET"])
@login_required
def history_stats():
    uid = current_user.id
    total_runs   = PipelineRun.query.outerjoin(Pipeline).filter(
        db.or_(PipelineRun.user_id == uid, Pipeline.user_id == uid)
    ).count()
    success_runs = PipelineRun.query.outerjoin(Pipeline).filter(
        db.or_(PipelineRun.user_id == uid, Pipeline.user_id == uid),
        PipelineRun.status == "success"
    ).count()
    total_rows_in = db.session.query(
        db.func.coalesce(db.func.sum(PipelineRun.rows_in), 0)
    ).outerjoin(Pipeline).filter(
        db.or_(PipelineRun.user_id == uid, Pipeline.user_id == uid)
    ).scalar()
    total_rows_out = db.session.query(
        db.func.coalesce(db.func.sum(PipelineRun.rows_out), 0)
    ).outerjoin(Pipeline).filter(
        db.or_(PipelineRun.user_id == uid, Pipeline.user_id == uid)
    ).scalar()
    return jsonify({
        "total_runs":           total_runs,
        "success_runs":         success_runs,
        "failed_runs":          total_runs - success_runs,
        "success_rate":         round(success_runs / total_runs * 100, 1) if total_runs else 0,
        "total_rows_processed": total_rows_in,
        "rows_cleaned":         total_rows_out,
    })

@app.route("/api/history/runs", methods=["GET"])
@login_required
def history_runs():
    uid  = current_user.id
    runs = PipelineRun.query.outerjoin(Pipeline).filter(
        db.or_(PipelineRun.user_id == uid, Pipeline.user_id == uid)
    ).order_by(PipelineRun.started_at.desc()).limit(50).all()
    return jsonify([r.to_dict() for r in runs])

@app.route("/api/auth/update", methods=["PUT"])
@login_required
def update_profile():
    d = request.json or {}
    if "full_name"  in d: current_user.full_name  = d["full_name"]
    if "email"      in d: current_user.email      = d["email"]
    if "job_title"  in d: current_user.job_title  = d["job_title"]
    if "company"    in d: current_user.company    = d["company"]
    if "location"   in d: current_user.location   = d["location"]
    if "password" in d and d.get("current_password"):
        if not current_user.check_password(d["current_password"]):
            return jsonify({"error": "Current password is incorrect"}), 400
        current_user.set_password(d["password"])
    db.session.commit()
    return jsonify({"success": True, "user": current_user.to_dict()})

# ═════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════
def _load_file(path, ext):
    import pandas as pd
    ext = ext.lower()
    if ext == "csv":             return pd.read_csv(path, on_bad_lines="skip")
    if ext in ("xlsx", "xls"):   return pd.read_excel(path)
    if ext == "parquet":         return pd.read_parquet(path)
    if ext == "json":
        import json as _j
        with open(path) as f: data = _j.load(f)
        return pd.DataFrame(data if isinstance(data, list) else [data])
    if ext in ("tsv", "txt"):    return pd.read_csv(path, sep="\t", on_bad_lines="skip")
    return pd.read_csv(path, sep=None, engine="python", on_bad_lines="skip")


if __name__ == "__main__":
    app.run(debug=True, port=5000)