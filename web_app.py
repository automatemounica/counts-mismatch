"""
EHSWatch Report Validation — Flask web app.
Wraps sql_test process.py with a browser UI, live progress, and CSV export.
"""

import importlib.util
import json
import os
import queue
import smtplib
import sys
import threading
import time
import uuid
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from flask import Flask, Response, jsonify, render_template, request, send_file

# ---------------------------------------------------------------------------
# Load "sql_test process.py" — filename has a space, so can't import directly.
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

_spec = importlib.util.spec_from_file_location(
    "sql_test_process",
    os.path.join(BASE_DIR, "sql_test process.py"),
)
stp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(stp)

from tenants import load_tenants_for_sql_mode, TENANTS  # noqa: E402

# ---------------------------------------------------------------------------
# Email config — set EMAIL_PASSWORD before use
# ---------------------------------------------------------------------------
EMAIL_HOST       = "smtp.office365.com"
EMAIL_PORT       = 587
EMAIL_USERNAME   = "noreply@exceego.com"
EMAIL_PASSWORD   = "Pass@121"   # <-- paste your Outlook password or app password here
EMAIL_FROM       = "noreply@exceego.com"
EMAIL_DEFAULT_TO = "mounica.jagu@exceego.com"

# ---------------------------------------------------------------------------
# Recipients — persisted to recipients.json
# ---------------------------------------------------------------------------
RECIPIENTS_FILE = os.path.join(BASE_DIR, "recipients.json")
_extra_recipients: list[str] = []


def _load_recipients() -> None:
    global _extra_recipients
    try:
        if os.path.exists(RECIPIENTS_FILE):
            with open(RECIPIENTS_FILE, "r", encoding="utf-8") as f:
                _extra_recipients = json.load(f)
    except Exception:
        pass


def _save_recipients_file() -> None:
    try:
        with open(RECIPIENTS_FILE, "w", encoding="utf-8") as f:
            json.dump(_extra_recipients, f)
    except Exception:
        pass


def _all_recipients() -> list[str]:
    """Default addresses + any extras, deduplicated, preserving order."""
    seen: set[str] = set()
    result: list[str] = []
    defaults = [e.strip() for e in EMAIL_DEFAULT_TO.split(",") if e.strip()]
    for addr in defaults + _extra_recipients:
        if addr.lower() not in seen:
            seen.add(addr.lower())
            result.append(addr)
    return result


def _build_and_send_email(instance: str, rows: list, csv_path: str | None) -> None:
    """Build HTML report and send to all configured recipients."""
    to_list = _all_recipients()
    if not EMAIL_PASSWORD or not to_list or not rows:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total   = len(rows)
    passed  = sum(1 for r in rows if r["status"] == "PASS")
    failed  = sum(1 for r in rows if r["status"] == "FAIL")
    not_run = sum(1 for r in rows if r["status"] == "NOT RUN")
    checked = total - not_run
    rate    = round((passed / checked) * 100) if checked > 0 else 0

    tenant_map: dict[str, dict] = {}
    for r in rows:
        t = r["tenant"]
        if t not in tenant_map:
            tenant_map[t] = {"pass": 0, "fail": 0, "total": 0}
        tenant_map[t]["total"] += 1
        if r["status"] == "PASS":
            tenant_map[t]["pass"] += 1
        elif r["status"] == "FAIL":
            tenant_map[t]["fail"] += 1

    tenant_rows_html = "".join(
        f'<tr style="background:{"#fff3f3" if s["fail"] > 0 else "#f3fff8"};">'
        f'<td style="padding:8px 12px;border-bottom:1px solid #eee;">{t.capitalize()}</td>'
        f'<td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:center;">{s["total"]}</td>'
        f'<td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:center;color:#198754;font-weight:600;">{s["pass"]}</td>'
        f'<td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:center;color:#dc3545;font-weight:600;">{s["fail"]}</td>'
        f'</tr>'
        for t, s in sorted(tenant_map.items())
    )

    inst_label = instance.capitalize()
    html_body = f"""<!DOCTYPE html>
<html><body style="margin:0;padding:0;background:#f4f6f8;font-family:Arial,sans-serif;color:#333;">
<div style="max-width:680px;margin:24px auto;background:#fff;border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
  <div style="background:linear-gradient(135deg,#1a7a4a,#27ae60);padding:24px 28px;">
    <h2 style="color:#fff;margin:0;font-size:1.25rem;">EHSWatch Report Validation</h2>
    <p style="color:rgba(255,255,255,0.85);margin:4px 0 0;font-size:0.88rem;">Completed successfully &mdash; {inst_label} Instance</p>
  </div>
  <div style="padding:24px 28px;">
    <p style="margin-top:0;">Hi,</p>
    <p>The EHSWatch validation on the <strong>{inst_label}</strong> instance completed on <strong>{timestamp}</strong>.</p>
    <table width="100%" style="border-collapse:separate;border-spacing:8px;margin:16px 0;">
      <tr>
        <td style="background:#f8f9fa;border:1px solid #e0e0e0;border-radius:6px;padding:14px;text-align:center;">
          <div style="font-size:22px;font-weight:700;color:#333;">{total}</div>
          <div style="font-size:11px;color:#888;margin-top:3px;">Total Checks</div>
        </td>
        <td style="background:#f8f9fa;border:1px solid #e0e0e0;border-radius:6px;padding:14px;text-align:center;">
          <div style="font-size:22px;font-weight:700;color:#198754;">{passed}</div>
          <div style="font-size:11px;color:#888;margin-top:3px;">PASS</div>
        </td>
        <td style="background:#f8f9fa;border:1px solid #e0e0e0;border-radius:6px;padding:14px;text-align:center;">
          <div style="font-size:22px;font-weight:700;color:#dc3545;">{failed}</div>
          <div style="font-size:11px;color:#888;margin-top:3px;">FAIL</div>
        </td>
        <td style="background:#f8f9fa;border:1px solid #e0e0e0;border-radius:6px;padding:14px;text-align:center;">
          <div style="font-size:22px;font-weight:700;color:#1a7a4a;">{rate}%</div>
          <div style="font-size:11px;color:#888;margin-top:3px;">Pass Rate</div>
        </td>
      </tr>
    </table>
    <h3 style="font-size:0.92rem;color:#444;margin:20px 0 10px;">Tenant Summary</h3>
    <table width="100%" style="border-collapse:collapse;font-size:0.88rem;">
      <thead><tr style="background:#1a7a4a;color:#fff;">
        <th style="padding:8px 12px;text-align:left;">Tenant</th>
        <th style="padding:8px 12px;text-align:center;">Apps</th>
        <th style="padding:8px 12px;text-align:center;">PASS</th>
        <th style="padding:8px 12px;text-align:center;">FAIL</th>
      </tr></thead>
      <tbody>{tenant_rows_html}</tbody>
    </table>
    <p style="margin-top:20px;color:#666;font-size:0.82rem;">Full details are attached as a CSV file.</p>
  </div>
  <div style="background:#f8f9fa;padding:12px 28px;border-top:1px solid #eee;font-size:0.75rem;color:#999;">
    EHSWatch Report Validation &mdash; Auto-generated &mdash; {timestamp}
  </div>
</div>
</body></html>"""

    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"EHSWatch Validation Report — {inst_label} — {timestamp[:10]}"
    msg["From"]    = EMAIL_FROM
    msg["To"]      = ", ".join(to_list)
    msg.attach(MIMEText(html_body, "html"))

    if csv_path and os.path.exists(csv_path):
        with open(csv_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(csv_path)}"')
        msg.attach(part)

    try:
        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, to_list, msg.as_string())
        print(f"[email] Report sent to {to_list}")
    except Exception as exc:
        print(f"[email] FAILED: {exc}")


# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__)

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Instance config — add PROD values when available
# ---------------------------------------------------------------------------
INSTANCE_CONFIG: dict[str, dict] = {
    "demo": {
        # Demo uses the existing Excel/JSON config loaded at startup — no overrides needed.
    },
    "prod": {
        # TODO: fill in when prod config is received
        # "excel_credentials": "path/to/prod_credentials.xlsx",
        # "excel_endpoints":   "path/to/prod_endpoints.xlsx",
        # "json_modules":      "path/to/prod_modules.json",
        # "db_server":         "prod-db.example.com",
        # "db_name":           "EHSWatchV3Prod_ReportService",
    },
}

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------
_state_lock = threading.Lock()
_validation_active = False
_stop_requested = False
_latest_rows: list[dict[str, Any]] = []
_latest_csv_path: str | None = None
_job_queues: dict[str, queue.Queue] = {}
_runtime_tenants: dict = {}      # demo tenants (loaded at startup)
_run_history: list[dict] = []    # up to 2 entries: {timestamp, rows, csv_path}


_dev_tenants: dict = {}


def _init_tenants():
    global _runtime_tenants, _dev_tenants
    try:
        _runtime_tenants = load_tenants_for_sql_mode(domain="demoehswatch.com")
        print(f"[web_app] Loaded {len(_runtime_tenants)} demo tenants.")
    except Exception as exc:
        print(f"[web_app] Demo Excel load failed ({exc}). Using fallback TENANTS.")
        _runtime_tenants = TENANTS
    try:
        _dev_tenants = load_tenants_for_sql_mode(domain="dev-ehswatch.com")
        print(f"[web_app] Loaded {len(_dev_tenants)} dev tenants.")
    except Exception as exc:
        print(f"[web_app] Dev tenant load failed ({exc}). Dev instance unavailable.")
        _dev_tenants = {}


def _get_tenants_for_instance(instance: str) -> dict:
    if instance == "dev":
        return _dev_tenants
    return _runtime_tenants


LAST_RUN_FILE = os.path.join(BASE_DIR, "last_run.json")


def _save_last_run(rows: list, csv_path: str | None) -> None:
    global _run_history
    entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rows": rows,
        "csv_path": csv_path or "",
    }
    _run_history = ([entry] + _run_history)[:2]
    try:
        with open(LAST_RUN_FILE, "w", encoding="utf-8") as f:
            json.dump(_run_history, f)
    except Exception:
        pass


def _load_last_run() -> None:
    global _latest_rows, _latest_csv_path, _run_history
    try:
        if os.path.exists(LAST_RUN_FILE):
            with open(LAST_RUN_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            # Handle old dict format vs new list format
            if isinstance(data, dict):
                _run_history = [{
                    "timestamp": "unknown",
                    "rows": data.get("rows", []),
                    "csv_path": data.get("csv_path", ""),
                }]
            else:
                _run_history = data[:2]
            if _run_history:
                _latest_rows = _run_history[0]["rows"]
                _latest_csv_path = _run_history[0]["csv_path"] or None
            print(f"[web_app] Last run loaded from disk: {len(_latest_rows)} rows, {len(_run_history)} run(s) in history.")
    except Exception:
        pass


_init_tenants()
_load_last_run()
_load_recipients()
_validation_active = False  # ensure clean state on (re)start
_stop_requested = False


# ---------------------------------------------------------------------------
# Background validation worker
# ---------------------------------------------------------------------------
def _emit(q: queue.Queue, msg_type: str, **data):
    q.put({"type": msg_type, **data})


def _run_validation_job(job_id: str, tenant_names: list[str], instance: str = "demo"):
    global _stop_requested
    q = _job_queues.get(job_id)
    if q is None:
        return

    all_rows: list[dict[str, Any]] = []

    for name in tenant_names:
        if _stop_requested:
            _emit(q, "log", message="⏹ Validation stopped by user.")
            break

        tenant_cfg = _runtime_tenants.get(name)
        if not tenant_cfg:
            _emit(q, "log", message=f"[{name}] Not found in config, skipping.")
            continue

        applications = tenant_cfg.get("api_config", {}).get("applications", {})
        total_apps = len(applications)
        _emit(q, "tenant_start", tenant=name, total_apps=total_apps)

        try:
            token = stp.get_token(tenant_cfg)
        except Exception as exc:
            error_msg = str(exc)
            _emit(q, "log", message=f"[{name}] Token error: {error_msg}")
            _emit(q, "tenant_done", tenant=name, status="FAILED", error=error_msg, rows=[])
            continue

        tenant_rows: list[dict[str, Any]] = []
        app_done = 0
        run_apps: set[str] = set()

        for app_name, app_cfg in applications.items():
            if _stop_requested:
                break
            app_done += 1
            run_apps.add(app_name)
            _emit(q, "app_start", tenant=name, app=app_name,
                  app_num=app_done, total_apps=total_apps)
            try:
                list_count, sql_count, status = stp._run_application_sql_verification(
                    name, tenant_cfg, token, app_name, app_cfg
                )
                row: dict[str, Any] = {
                    "application": app_name,
                    "list_count": list_count,
                    "sql_count": sql_count,
                    "difference": (list_count - sql_count) if sql_count >= 0 else 0,
                    "status": status,
                }
            except Exception as exc:
                row = {
                    "application": app_name,
                    "list_count": -1, "sql_count": -1, "difference": 0,
                    "status": "FAIL", "error_detail": str(exc),
                }
            tenant_rows.append(row)
            _emit(q, "app_done", tenant=name, app=app_name,
                  app_num=app_done, total_apps=total_apps, status=row["status"])

        for row in tenant_rows:
            all_rows.append({
                "tenant": name,
                "application": row["application"],
                "list_count": row["list_count"],
                "sql_count": row["sql_count"],
                "difference": row["difference"],
                "status": row["status"],
                "error_detail": row.get("error_detail", ""),
            })

        # Mark any apps skipped due to stop as NOT RUN
        for skipped in applications:
            if skipped not in run_apps:
                tenant_rows.append({
                    "application": skipped,
                    "list_count": -1, "sql_count": -1, "difference": 0,
                    "status": "NOT RUN",
                })

        _emit(q, "tenant_done",
              tenant=name, status="OK",
              rows=[{
                  "application": r["application"],
                  "list_count": r["list_count"],
                  "sql_count": r["sql_count"],
                  "difference": r["difference"],
                  "status": r["status"],
              } for r in tenant_rows])

    # Write CSV
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    csv_path = os.path.join(BASE_DIR, f"csv_file_summary_{timestamp}.csv")
    try:
        stp._write_summary_csv(all_rows, csv_path)
        _emit(q, "log", message=f"CSV written: {os.path.basename(csv_path)}")
    except Exception as exc:
        _emit(q, "log", message=f"CSV write error: {exc}")
        csv_path = None

    _save_last_run(all_rows, csv_path)

    with _state_lock:
        global _latest_rows, _latest_csv_path, _validation_active
        _latest_rows = all_rows
        _latest_csv_path = csv_path
        _validation_active = False
        _stop_requested = False

    total = len(all_rows)
    passed = sum(1 for r in all_rows if r["status"] == "PASS")
    failed = total - passed
    _emit(q, "done", total=total, passed=passed, failed=failed)
    q.put(None)  # sentinel — closes SSE stream

    # Auto-send email notification
    if EMAIL_PASSWORD:
        threading.Thread(
            target=_build_and_send_email,
            args=(instance, all_rows, csv_path),
            daemon=True,
        ).start()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/tenants")
def get_tenants():
    result = {}
    for name, cfg in _runtime_tenants.items():
        apps = sorted(cfg.get("api_config", {}).get("applications", {}).keys())
        result[name] = apps
    return jsonify(result)


@app.route("/api/validate", methods=["POST"])
def start_validation():
    global _validation_active
    with _state_lock:
        if _validation_active:
            return jsonify({"error": "Validation already running"}), 409
        _validation_active = True
        _stop_requested = False

    data = request.get_json(silent=True) or {}
    mode = data.get("mode", "all")
    tenant = data.get("tenant", "").strip()
    instance = data.get("instance", "demo")

    tenants = _get_tenants_for_instance(instance)
    tenant_names = [tenant] if (mode == "one" and tenant) else list(tenants.keys())

    job_id = str(uuid.uuid4())
    q: queue.Queue = queue.Queue()
    _job_queues[job_id] = q

    t = threading.Thread(target=_run_validation_job, args=(job_id, tenant_names, instance), daemon=True)
    t.start()

    return jsonify({"job_id": job_id})


@app.route("/api/stream/<job_id>")
def stream(job_id: str):
    def generate():
        q = _job_queues.get(job_id)
        if q is None:
            yield f"data: {json.dumps({'type': 'error', 'message': 'Job not found'})}\n\n"
            return
        while True:
            try:
                msg = q.get(timeout=60)
            except queue.Empty:
                yield ": heartbeat\n\n"
                continue
            if msg is None:
                yield f"data: {json.dumps({'type': 'end'})}\n\n"
                _job_queues.pop(job_id, None)
                break
            yield f"data: {json.dumps(msg)}\n\n"

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.route("/api/last-results")
def last_results():
    with _state_lock:
        history = list(_run_history)
        rows = list(_latest_rows)
        csv_path = _latest_csv_path
    runs = [
        {
            "timestamp": run["timestamp"],
            "count": len(run["rows"]),
            "rows": run["rows"],
            "csv_available": bool(run["csv_path"] and os.path.exists(run["csv_path"])),
        }
        for run in history
    ]
    return jsonify({
        "runs": runs,
        "rows": rows,
        "csv_available": bool(csv_path and os.path.exists(csv_path)),
        "count": len(rows),
    })


@app.route("/api/stop", methods=["POST"])
def stop_validation():
    global _stop_requested
    with _state_lock:
        if _validation_active:
            _stop_requested = True
            return jsonify({"status": "stop requested"})
    return jsonify({"status": "no validation running"})


@app.route("/api/download")
def download():
    with _state_lock:
        csv_path = _latest_csv_path
    if not csv_path or not os.path.exists(csv_path):
        return jsonify({"error": "No CSV available. Run validation first."}), 404
    return send_file(
        csv_path,
        as_attachment=True,
        download_name=os.path.basename(csv_path),
        mimetype="text/csv",
    )


@app.route("/api/test-email", methods=["POST"])
def test_email():
    """Send a quick test email to verify SMTP config."""
    if not EMAIL_PASSWORD:
        return jsonify({"error": "EMAIL_PASSWORD not set in web_app.py"}), 503
    to_list = _all_recipients()
    if not to_list:
        return jsonify({"error": "No recipients configured."}), 400
    try:
        msg = MIMEMultipart("mixed")
        msg["Subject"] = "EHSWatch — SMTP Test"
        msg["From"]    = EMAIL_FROM
        msg["To"]      = ", ".join(to_list)
        msg.attach(MIMEText("<p>This is a test email from EHSWatch scheduler.</p>", "html"))
        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, to_list, msg.as_string())
        print(f"[email] Test email sent to {to_list}")
        return jsonify({"status": "sent", "to": to_list})
    except Exception as exc:
        print(f"[email] Test FAILED: {exc}")
        return jsonify({"error": str(exc)}), 500


@app.route("/api/send-email", methods=["POST"])
def send_email_report():
    if not EMAIL_PASSWORD:
        return jsonify({"error": "Email not configured — set EMAIL_PASSWORD in web_app.py."}), 503
    with _state_lock:
        rows     = list(_latest_rows)
        csv_path = _latest_csv_path
    if not rows:
        return jsonify({"error": "No results to send. Run a validation first."}), 400
    instance = (request.get_json(silent=True) or {}).get("instance", "demo")
    threading.Thread(
        target=_build_and_send_email,
        args=(instance, rows, csv_path),
        daemon=True,
    ).start()
    return jsonify({"status": "queued"})


@app.route("/api/recipients", methods=["GET"])
def get_recipients():
    defaults = [e.strip() for e in EMAIL_DEFAULT_TO.split(",") if e.strip()]
    return jsonify({"defaults": defaults, "extra": list(_extra_recipients)})


@app.route("/api/recipients", methods=["POST"])
def add_recipient():
    global _extra_recipients
    email = (request.get_json(silent=True) or {}).get("email", "").strip()
    if not email or "@" not in email:
        return jsonify({"error": "Invalid email address."}), 400
    defaults_lower = [e.strip().lower() for e in EMAIL_DEFAULT_TO.split(",")]
    if email.lower() in defaults_lower or email.lower() in [e.lower() for e in _extra_recipients]:
        return jsonify({"error": "Already in recipients list."}), 409
    _extra_recipients.append(email)
    _save_recipients_file()
    return jsonify({"status": "added", "extra": _extra_recipients})


@app.route("/api/recipients/<path:email>", methods=["DELETE"])
def remove_recipient(email: str):
    global _extra_recipients
    _extra_recipients = [e for e in _extra_recipients if e.lower() != email.lower()]
    _save_recipients_file()
    return jsonify({"status": "removed", "extra": _extra_recipients})


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------
SCHEDULES_FILE = os.path.join(BASE_DIR, "schedules.json")
_schedules: list[dict] = []
_schedules_lock = threading.Lock()


def _load_schedules() -> None:
    global _schedules
    try:
        if os.path.exists(SCHEDULES_FILE):
            with open(SCHEDULES_FILE, "r", encoding="utf-8") as f:
                _schedules = json.load(f)
    except Exception:
        _schedules = []


def _save_schedules() -> None:
    try:
        with open(SCHEDULES_FILE, "w", encoding="utf-8") as f:
            json.dump(_schedules, f)
    except Exception:
        pass


_scheduler_fired_minutes: set[str] = set()  # track already-fired "YYYY-MM-DD HH:MM"


def _scheduler_loop() -> None:
    """Background thread: fires validation when a schedule's time matches."""
    while True:
        time.sleep(20)
        now = datetime.now()
        day_name = now.strftime("%A").lower()
        hhmm = now.strftime("%H:%M")
        fire_key = now.strftime("%Y-%m-%d ") + hhmm  # unique per day+minute

        with _schedules_lock:
            scheds = list(_schedules)

        # Clean up keys from previous minutes
        current_keys = {now.strftime("%Y-%m-%d ") + s.get("time", "") for s in scheds}
        _scheduler_fired_minutes.intersection_update(current_keys | {fire_key})

        for s in scheds:
            if not s.get("enabled", True):
                continue
            if s.get("time") != hhmm:
                continue
            if s.get("type") == "weekly" and s.get("day") != day_name:
                continue
            if fire_key in _scheduler_fired_minutes:
                continue  # already fired this minute

            print(f"[scheduler] Firing scheduled validation at {hhmm} (schedule id={s['id']})")
            _scheduler_fired_minutes.add(fire_key)

            with _state_lock:
                global _validation_active, _stop_requested
                if _validation_active:
                    print("[scheduler] Skipped — validation already running.")
                    continue
                _validation_active = True
                _stop_requested = False

            job_id = str(uuid.uuid4())
            q: queue.Queue = queue.Queue()
            _job_queues[job_id] = q
            tenant_names = list(_runtime_tenants.keys())

            def _scheduled_run(jid, tnames):
                _run_validation_job(jid, tnames, "demo")
                _job_queues.pop(jid, None)  # clean up queue after done
                print(f"[scheduler] Scheduled run {jid} complete.")

            threading.Thread(
                target=_scheduled_run,
                args=(job_id, tenant_names),
                daemon=True,
            ).start()


_load_schedules()
threading.Thread(target=_scheduler_loop, daemon=True).start()


@app.route("/api/schedules", methods=["GET"])
def get_schedules():
    with _schedules_lock:
        return jsonify(list(_schedules))


@app.route("/api/schedules", methods=["POST"])
def add_schedule():
    data = request.get_json(silent=True) or {}
    stype = data.get("type", "daily")
    stime = data.get("time", "").strip()
    sday  = data.get("day", "monday").strip().lower()
    if not stime:
        return jsonify({"error": "Time is required."}), 400
    new_sched = {
        "id": str(uuid.uuid4()),
        "type": stype,
        "time": stime,
        "day": sday,
        "enabled": True,
    }
    with _schedules_lock:
        _schedules.append(new_sched)
        _save_schedules()
        return jsonify(list(_schedules))


@app.route("/api/schedules/<sched_id>", methods=["DELETE"])
def delete_schedule(sched_id: str):
    with _schedules_lock:
        global _schedules
        _schedules = [s for s in _schedules if s["id"] != sched_id]
        _save_schedules()
        return jsonify(list(_schedules))


@app.route("/api/schedules/<sched_id>/toggle", methods=["POST"])
def toggle_schedule(sched_id: str):
    with _schedules_lock:
        for s in _schedules:
            if s["id"] == sched_id:
                s["enabled"] = not s.get("enabled", True)
                break
        _save_schedules()
        return jsonify(list(_schedules))


if __name__ == "__main__":
    app.run(debug=True, port=5000, threaded=True, use_reloader=False)
