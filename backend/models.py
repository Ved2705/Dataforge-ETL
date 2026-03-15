from datetime import datetime, timezone
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from flask_bcrypt import Bcrypt

db  = SQLAlchemy()
bcr = Bcrypt()

def now(): return datetime.now(timezone.utc)


class User(UserMixin, db.Model):
    __tablename__ = "users"
    id            = db.Column(db.Integer, primary_key=True)
    username      = db.Column(db.String(64),  unique=True, nullable=False, index=True)
    email         = db.Column(db.String(120), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(256), nullable=False)
    full_name     = db.Column(db.String(128), nullable=True)
    job_title     = db.Column(db.String(128), nullable=True)
    company       = db.Column(db.String(128), nullable=True)
    location      = db.Column(db.String(128), nullable=True)
    created_at    = db.Column(db.DateTime, default=now)
    last_login    = db.Column(db.DateTime, nullable=True)
    pipelines     = db.relationship("Pipeline",     back_populates="user", cascade="all,delete-orphan")
    datasets      = db.relationship("Dataset",      back_populates="user", cascade="all,delete-orphan")
    sessions      = db.relationship("LoginSession", back_populates="user", cascade="all,delete-orphan")

    def set_password(self, pw):
        self.password_hash = bcr.generate_password_hash(pw).decode()

    def check_password(self, pw):
        return bcr.check_password_hash(self.password_hash, pw)

    def to_dict(self):
        return dict(
            id=self.id,
            username=self.username,
            email=self.email,
            full_name=self.full_name or '',
            job_title=self.job_title or '',
            company=self.company or '',
            location=self.location or '',
            created_at=self.created_at.isoformat() if self.created_at else None,
            last_login=self.last_login.isoformat() if self.last_login else None,
            pipeline_count=len(self.pipelines),
            dataset_count=len(self.datasets),
        )


class LoginSession(db.Model):
    __tablename__ = "login_sessions"
    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    logged_in  = db.Column(db.DateTime, default=now)
    logged_out = db.Column(db.DateTime, nullable=True)
    ip_address = db.Column(db.String(45))
    user_agent = db.Column(db.String(300))
    user       = db.relationship("User", back_populates="sessions")


class Dataset(db.Model):
    __tablename__ = "datasets"

    id           = db.Column(db.Integer, primary_key=True)
    user_id      = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)

    ds_id        = db.Column(db.String(32), unique=True, index=True)   # NEW

    name         = db.Column(db.String(256), nullable=False)
    file_format  = db.Column(db.String(10))
    file_size    = db.Column(db.BigInteger)

    row_count    = db.Column(db.Integer)
    col_count    = db.Column(db.Integer)

    schema_json  = db.Column(db.JSON)      # NEW
    profile_json = db.Column(db.JSON)
    preview_json = db.Column(db.JSON)      # NEW

    status       = db.Column(db.String(20))  # NEW

    storage_path = db.Column(db.String(512))
    uploaded_at  = db.Column(db.DateTime, default=now)

    user          = db.relationship("User", back_populates="datasets")
    pipeline_runs = db.relationship("PipelineRun", back_populates="dataset")

    def to_dict(self):
        return dict(
            id=self.id,
            ds_id=self.ds_id,
            name=self.name,
            file_format=self.file_format,
            file_size=self.file_size,
            row_count=self.row_count,
            col_count=self.col_count,
            status=self.status,
            uploaded_at=self.uploaded_at.isoformat() if self.uploaded_at else None,
            storage_path=self.storage_path,
        )


class Pipeline(db.Model):
    __tablename__ = "pipelines"
    id             = db.Column(db.Integer, primary_key=True)
    user_id        = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    name           = db.Column(db.String(128), nullable=False)
    description    = db.Column(db.Text)
    steps_json     = db.Column(db.JSON, default=list)
    schedule       = db.Column(db.String(64))
    airflow_dag_id = db.Column(db.String(128))
    created_at     = db.Column(db.DateTime, default=now)
    updated_at     = db.Column(db.DateTime, default=now, onupdate=now)
    user = db.relationship("User", back_populates="pipelines")
    runs = db.relationship("PipelineRun", back_populates="pipeline", cascade="all,delete-orphan")

    def to_dict(self):
        return dict(
            id=self.id,
            name=self.name,
            description=self.description,
            steps=self.steps_json,
            schedule=self.schedule,
            airflow_dag_id=self.airflow_dag_id,
            run_count=len(self.runs),
            last_run=self.runs[-1].to_dict() if self.runs else None,
            created_at=self.created_at.isoformat() if self.created_at else None,
        )


class PipelineRun(db.Model):
    __tablename__ = "pipeline_runs"
    id              = db.Column(db.Integer, primary_key=True)
    run_id          = db.Column(db.String(64), unique=True, index=True, nullable=True)
    pipeline_id     = db.Column(db.Integer, db.ForeignKey("pipelines.id"), nullable=True)
    dataset_id      = db.Column(db.Integer, db.ForeignKey("datasets.id"),  nullable=True)
    user_id         = db.Column(db.Integer, db.ForeignKey("users.id"),     nullable=True)
    status          = db.Column(db.String(20), default="queued")
    trigger         = db.Column(db.String(20), default="manual")
    started_at      = db.Column(db.DateTime, default=now)
    finished_at     = db.Column(db.DateTime, nullable=True)

    # row counts — both naming conventions supported
    rows_in         = db.Column(db.Integer, nullable=True)
    rows_out        = db.Column(db.Integer, nullable=True)
    input_rows      = db.Column(db.Integer, nullable=True)
    output_rows     = db.Column(db.Integer, nullable=True)

    duration_s      = db.Column(db.Float,   nullable=True)
    output_path     = db.Column(db.String(512), nullable=True)

    # result storage — both naming conventions supported
    summary_json    = db.Column(db.JSON, nullable=True)
    result_json     = db.Column(db.JSON, nullable=True)
    pipeline_config = db.Column(db.JSON, nullable=True)

    pipeline  = db.relationship("Pipeline", back_populates="runs")
    dataset   = db.relationship("Dataset",  back_populates="pipeline_runs")
    logs      = db.relationship("RunLog",   back_populates="run", cascade="all,delete-orphan")
    dq_result = db.relationship("DQResult", back_populates="run", uselist=False, cascade="all,delete-orphan")

    def get_rows_in(self):
        return self.rows_in or self.input_rows or 0

    def get_rows_out(self):
        return self.rows_out or self.output_rows or 0

    def duration(self):
        if self.duration_s is not None:
            return self.duration_s
        if self.finished_at and self.started_at:
            return round((self.finished_at - self.started_at).total_seconds(), 2)
        return None

    def to_dict(self):
        return dict(
            id=self.id,
            run_id=self.run_id or str(self.id),
            pipeline_id=self.pipeline_id,
            dataset_id=self.dataset_id,
            status=self.status,
            trigger=self.trigger,
            started_at=self.started_at.isoformat()  if self.started_at  else None,
            finished_at=self.finished_at.isoformat() if self.finished_at else None,
            duration_s=self.duration(),
            duration_secs=self.duration(),
            input_rows=self.get_rows_in(),
            output_rows=self.get_rows_out(),
            rows_in=self.get_rows_in(),
            rows_out=self.get_rows_out(),
            output_path=self.output_path,
            summary=self.summary_json,
            result=self.result_json,
            pipeline_config=self.pipeline_config,
        )


class RunLog(db.Model):
    __tablename__ = "run_logs"
    id      = db.Column(db.Integer, primary_key=True)
    run_id  = db.Column(db.Integer, db.ForeignKey("pipeline_runs.id"), nullable=False)
    ts      = db.Column(db.DateTime, default=now)
    level   = db.Column(db.String(10), default="INFO")
    step    = db.Column(db.String(64))
    message = db.Column(db.Text)
    run     = db.relationship("PipelineRun", back_populates="logs")

    def to_dict(self):
        return dict(
            id=self.id,
            ts=self.ts.isoformat() if self.ts else None,
            level=self.level,
            step=self.step,
            message=self.message,
        )


class DQResult(db.Model):
    __tablename__ = "dq_results"
    id                = db.Column(db.Integer, primary_key=True)
    run_id            = db.Column(db.Integer, db.ForeignKey("pipeline_runs.id"), unique=True, nullable=False)
    evaluated_at      = db.Column(db.DateTime, default=now)
    success           = db.Column(db.Boolean)
    total_checks      = db.Column(db.Integer)
    passed_checks     = db.Column(db.Integer)
    failed_checks     = db.Column(db.Integer)
    success_pct       = db.Column(db.Float)
    expectations_json = db.Column(db.JSON)
    run               = db.relationship("PipelineRun", back_populates="dq_result")

    def to_dict(self):
        return dict(
            id=self.id,
            success=self.success,
            total_checks=self.total_checks,
            passed_checks=self.passed_checks,
            failed_checks=self.failed_checks,
            success_pct=self.success_pct,
            expectations=self.expectations_json,
            evaluated_at=self.evaluated_at.isoformat() if self.evaluated_at else None,
        )


class DQRuleSet(db.Model):
    __tablename__ = "dq_rule_sets"
    id              = db.Column(db.Integer, primary_key=True)
    user_id         = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    name            = db.Column(db.String(128), nullable=False)
    dataset_id      = db.Column(db.Integer, db.ForeignKey("datasets.id"), nullable=True)
    rules_json      = db.Column(db.JSON, default=list)
    created_at      = db.Column(db.DateTime, default=now)
    last_run_at     = db.Column(db.DateTime, nullable=True)
    last_run_status = db.Column(db.String(16), nullable=True)
    user            = db.relationship("User")
    dataset         = db.relationship("Dataset")


    def to_dict(self):          
        return dict(
            id=self.id,
            name=self.name,
            dataset_id=self.dataset_id,
            rules=self.rules_json or [],
            created_at=self.created_at.isoformat() if self.created_at else None,
            last_run_at=self.last_run_at.isoformat() if self.last_run_at else None,
            last_run_status=self.last_run_status,
        )