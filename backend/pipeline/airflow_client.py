"""
airflow_client.py — Triggers and monitors Airflow DAGs via REST API.
"""
import os, json, requests
from datetime import datetime, timezone


AIRFLOW_URL  = os.getenv("AIRFLOW_API_URL", "http://localhost:8080/api/v1")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASSWORD", "admin")
AUTH         = (AIRFLOW_USER, AIRFLOW_PASS)
HEADERS      = {"Content-Type": "application/json"}


def trigger_airflow_dag(dag_id: str, config: dict = None) -> dict:
    """Trigger a DAG run and return the run metadata."""
    if not dag_id:
        return {"status": "error", "error": "No dag_id provided"}

    conf = config.get("conf", {}) if config else {}
    payload = {
        "conf":            conf,
        "logical_date":    datetime.now(timezone.utc).isoformat(),
        "note":            "Triggered by DataForge ETL platform",
    }

    try:
        resp = requests.post(
            f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns",
            json=payload, auth=AUTH, headers=HEADERS, timeout=10,
        )
        if resp.status_code in (200, 201):
            data = resp.json()
            return {"status": "triggered", "dag_run_id": data.get("dag_run_id"),
                    "state": data.get("state"), "dag_id": dag_id}
        return {"status": "error", "http_status": resp.status_code, "body": resp.text[:500]}

    except requests.exceptions.ConnectionError:
        return {"status": "unreachable",
                "note": "Airflow is not reachable — is the webserver running?"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def get_dag_run_status(dag_id: str, run_id: str) -> dict:
    try:
        resp = requests.get(
            f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns/{run_id}",
            auth=AUTH, headers=HEADERS, timeout=10,
        )
        if resp.ok:
            d = resp.json()
            return {"dag_id": dag_id, "run_id": run_id,
                    "state": d.get("state"), "end_date": d.get("end_date")}
        return {"status": "error", "http_status": resp.status_code}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def list_dags() -> list:
    try:
        resp = requests.get(f"{AIRFLOW_URL}/dags", auth=AUTH, headers=HEADERS, timeout=10)
        if resp.ok:
            return resp.json().get("dags", [])
        return []
    except Exception:
        return []


def list_dag_runs(dag_id: str, limit: int = 10) -> list:
    try:
        resp = requests.get(
            f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns?limit={limit}&order_by=-execution_date",
            auth=AUTH, headers=HEADERS, timeout=10,
        )
        if resp.ok:
            return resp.json().get("dag_runs", [])
        return []
    except Exception:
        return []
