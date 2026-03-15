"""
api/scheduler.py — Scheduled Pipeline Runs + Notifications
Supports: cron schedules, email notifications, Slack webhooks
"""

from __future__ import annotations
import os, json, uuid, threading, time
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify
from flask_login import login_required, current_user

scheduler_bp = Blueprint("scheduler", __name__)

# In-memory schedule store (replace with DB table in production)
_schedules: dict[str, dict] = {}
_scheduler_thread = None
_scheduler_running = False

INTERVALS = {
    "hourly":  3600,
    "daily":   86400,
    "weekly":  604800,
    "monthly": 2592000,
}

# ── CRUD ─────────────────────────────────────────────

@scheduler_bp.post("/")
@login_required
def create_schedule():
    body = request.get_json(force=True)
    required = ["ds_id", "template_id", "interval"]
    for f in required:
        if not body.get(f):
            return jsonify({"error": f"'{f}' is required"}), 400

    interval = body["interval"]
    if interval not in INTERVALS and not _parse_cron(interval):
        return jsonify({"error": f"Invalid interval. Use: {list(INTERVALS.keys())} or a cron expression"}), 400

    sid = str(uuid.uuid4())[:8]
    schedule = {
        "id":           sid,
        "user_id":      current_user.id,
        "username":     current_user.username,
        "ds_id":        body["ds_id"],
        "template_id":  body["template_id"],
        "interval":     interval,
        "notify_email": body.get("notify_email", ""),
        "notify_slack": body.get("notify_slack", ""),
        "name":         body.get("name", f"Schedule-{sid}"),
        "enabled":      True,
        "created_at":   datetime.utcnow().isoformat(),
        "last_run":     None,
        "next_run":     _calc_next_run(interval),
        "run_count":    0,
        "last_status":  None,
    }
    _schedules[sid] = schedule
    _ensure_scheduler_running()
    return jsonify({"success": True, "schedule": schedule}), 201


@scheduler_bp.get("/")
@login_required
def list_schedules():
    user_scheds = [s for s in _schedules.values() if s["user_id"] == current_user.id]
    return jsonify({"schedules": user_scheds, "total": len(user_scheds)})


@scheduler_bp.get("/<sid>")
@login_required
def get_schedule(sid):
    s = _schedules.get(sid)
    if not s or s["user_id"] != current_user.id:
        return jsonify({"error": "Not found"}), 404
    return jsonify(s)


@scheduler_bp.patch("/<sid>")
@login_required
def update_schedule(sid):
    s = _schedules.get(sid)
    if not s or s["user_id"] != current_user.id:
        return jsonify({"error": "Not found"}), 404
    body = request.get_json(force=True)
    for key in ("name", "interval", "notify_email", "notify_slack", "enabled"):
        if key in body:
            s[key] = body[key]
    if "interval" in body:
        s["next_run"] = _calc_next_run(s["interval"])
    return jsonify({"success": True, "schedule": s})


@scheduler_bp.delete("/<sid>")
@login_required
def delete_schedule(sid):
    s = _schedules.get(sid)
    if not s or s["user_id"] != current_user.id:
        return jsonify({"error": "Not found"}), 404
    del _schedules[sid]
    return jsonify({"success": True})


@scheduler_bp.post("/<sid>/toggle")
@login_required
def toggle_schedule(sid):
    s = _schedules.get(sid)
    if not s or s["user_id"] != current_user.id:
        return jsonify({"error": "Not found"}), 404
    s["enabled"] = not s["enabled"]
    if s["enabled"]:
        s["next_run"] = _calc_next_run(s["interval"])
    return jsonify({"success": True, "enabled": s["enabled"]})


@scheduler_bp.post("/<sid>/run-now")
@login_required
def run_now(sid):
    """Trigger a scheduled pipeline immediately."""
    s = _schedules.get(sid)
    if not s or s["user_id"] != current_user.id:
        return jsonify({"error": "Not found"}), 404
    result = _execute_schedule(s)
    return jsonify({"success": True, "result": result})


# ── NOTIFICATIONS ─────────────────────────────────────

def send_email_notification(to_email: str, subject: str, body: str):
    """Send email via SMTP (configure via env vars)."""
    smtp_host = os.getenv("SMTP_HOST", "")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")
    from_addr = os.getenv("SMTP_FROM", smtp_user)

    if not smtp_host or not smtp_user:
        print(f"[Scheduler] Email not configured. Would send to {to_email}: {subject}")
        return False

    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = to_email

    html = f"""
    <html><body style="font-family:sans-serif;background:#0e1117;color:#dce8f0;padding:24px">
      <div style="max-width:480px;margin:0 auto">
        <h2 style="color:#00e5a0">DataForge ETL — Pipeline Notification</h2>
        <p>{body}</p>
        <hr style="border-color:#1c2535">
        <small style="color:#7a9ab0">DataForge ETL Platform · Automated notification</small>
      </div>
    </body></html>"""

    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_addr, to_email, msg.as_string())
        return True
    except Exception as e:
        print(f"[Scheduler] Email error: {e}")
        return False


def send_slack_notification(webhook_url: str, message: str):
    """Send Slack notification via incoming webhook."""
    if not webhook_url:
        return False
    import urllib.request
    payload = json.dumps({"text": message, "mrkdwn": True}).encode()
    try:
        req = urllib.request.Request(webhook_url, data=payload,
            headers={"Content-Type": "application/json"}, method="POST")
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        print(f"[Scheduler] Slack error: {e}")
        return False


def _notify(schedule: dict, status: str, details: str = ""):
    """Send notifications for a completed schedule run."""
    emoji  = "✅" if status == "success" else "❌"
    msg    = f"{emoji} *DataForge Pipeline* `{schedule['name']}` — {status.upper()}\n{details}"

    if schedule.get("notify_email"):
        subject = f"[DataForge] Pipeline {status}: {schedule['name']}"
        send_email_notification(schedule["notify_email"], subject, details or msg)

    if schedule.get("notify_slack"):
        send_slack_notification(schedule["notify_slack"], msg)


# ── SCHEDULER ENGINE ──────────────────────────────────

def _calc_next_run(interval: str) -> str:
    seconds = INTERVALS.get(interval)
    if seconds:
        return (datetime.utcnow() + timedelta(seconds=seconds)).isoformat()
    # cron-style fallback: next day
    return (datetime.utcnow() + timedelta(hours=24)).isoformat()


def _parse_cron(expr: str) -> bool:
    """Basic cron expression validator (5 fields)."""
    parts = expr.strip().split()
    return len(parts) == 5


def _execute_schedule(schedule: dict) -> dict:
    """Execute a scheduled pipeline run."""
    from flask import current_app
    print(f"[Scheduler] Running schedule '{schedule['name']}' (ds={schedule['ds_id']})")

    schedule["last_run"]  = datetime.utcnow().isoformat()
    schedule["run_count"] += 1

    try:
        with current_app.app_context():
            with current_app.test_client() as client:
                # Fetch template steps
                tpl_resp = client.get(
                    f"/api/templates/{schedule['template_id']}",
                    headers={"Content-Type": "application/json"}
                )
                if tpl_resp.status_code != 200:
                    raise Exception(f"Template {schedule['template_id']} not found")

                tpl  = tpl_resp.get_json()
                steps = tpl.get("steps", [])

                # Run pipeline
                resp = client.post("/api/pipeline/run", json={
                    "ds_id":           schedule["ds_id"],
                    "steps":           steps,
                    "use_spark":       False,
                    "trigger_airflow": False,
                    "generate_dbt":    False,
                })
                data = resp.get_json()

        if resp.status_code == 200 and data.get("status") == "success":
            schedule["last_status"] = "success"
            details = (f"Processed {data.get('input_rows',0)} → "
                       f"{data.get('output_rows',0)} rows in {data.get('duration_s',0)}s")
            _notify(schedule, "success", details)
            result = {"status": "success", **data}
        else:
            raise Exception(data.get("error", "Pipeline failed"))

    except Exception as e:
        schedule["last_status"] = "failed"
        _notify(schedule, "failed", str(e))
        result = {"status": "failed", "error": str(e)}

    schedule["next_run"] = _calc_next_run(schedule["interval"])
    return result


def _scheduler_loop(app):
    """Background thread that checks and runs due schedules."""
    with app.app_context():
        while _scheduler_running:
            now = datetime.utcnow()
            for sid, schedule in list(_schedules.items()):
                if not schedule.get("enabled"):
                    continue
                next_run_str = schedule.get("next_run")
                if not next_run_str:
                    continue
                try:
                    next_run = datetime.fromisoformat(next_run_str)
                    if now >= next_run:
                        threading.Thread(
                            target=_execute_schedule,
                            args=(schedule,),
                            daemon=True
                        ).start()
                except Exception as e:
                    print(f"[Scheduler] Error checking schedule {sid}: {e}")
            time.sleep(60)  # Check every minute


def _ensure_scheduler_running():
    global _scheduler_thread, _scheduler_running
    if _scheduler_thread and _scheduler_thread.is_alive():
        return
    from flask import current_app
    _scheduler_running = True
    _scheduler_thread = threading.Thread(
        target=_scheduler_loop,
        args=(current_app._get_current_object(),),
        daemon=True
    )
    _scheduler_thread.start()
    print("[Scheduler] Background scheduler started")


# ── STATUS ENDPOINT ───────────────────────────────────

@scheduler_bp.get("/status")
def scheduler_status():
    return jsonify({
        "running":          _scheduler_running,
        "thread_alive":     _scheduler_thread.is_alive() if _scheduler_thread else False,
        "total_schedules":  len(_schedules),
        "active_schedules": sum(1 for s in _schedules.values() if s.get("enabled")),
    })