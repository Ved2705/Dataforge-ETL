"""
dbt_runner.py — Runs dbt models via subprocess and captures results.
"""
import subprocess, json, os, shutil
from pathlib import Path


def run_dbt_model(config: dict) -> dict:
    """
    config = {
        "project_dir": "/app/dbt_project",
        "profiles_dir": "/app/dbt_project",
        "model": "stg_dataset",          # optional: specific model
        "command": "run|test|compile"
    }
    """
    project_dir  = config.get("project_dir",  "/app/dbt_project")
    profiles_dir = config.get("profiles_dir", project_dir)
    model        = config.get("model")
    command      = config.get("command", "run")

    if not shutil.which("dbt"):
        return {
            "status": "skipped",
            "note":   "dbt not found in PATH — install dbt-postgres",
            "command": command,
        }

    cmd = [
        "dbt", command,
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
        "--no-use-colors",
    ]
    if model:
        cmd += ["--select", model]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=300
        )
        return {
            "status":     "success" if result.returncode == 0 else "failed",
            "command":    " ".join(cmd),
            "returncode": result.returncode,
            "stdout":     result.stdout[-3000:],  # last 3k chars
            "stderr":     result.stderr[-1000:],
        }
    except subprocess.TimeoutExpired:
        return {"status": "timeout", "command": " ".join(cmd)}
    except Exception as e:
        return {"status": "error", "error": str(e)}
