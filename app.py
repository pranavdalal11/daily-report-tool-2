import csv
import hashlib
import json
import os
import secrets
import shutil
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from flask import Flask, Response, abort, g, jsonify, redirect, render_template_string, request, send_file, session, url_for
from werkzeug.middleware.proxy_fix import ProxyFix

import atexit
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

@atexit.register
def close_pool():
    global _pool
    try:
        if _pool:
            _pool.closeall()
    except Exception:
        pass



@dataclass(frozen=True)
class Task:
    id: str
    label: str
    estimate: int
    category: str


MEMBER_NAMES = [
    "Pranav Dalal",
    "Omprakash Shripad",
    "Jay Patel",
    "Harpalsinh Jadeja",
    "Yashpalsingh Chauhan",
    "Vidhi Trivedi",
]

DEFAULT_TEAM_ID = "default"
DEFAULT_TEAM_NAME = "Default"


def _normalize_team_id(value: str) -> str:
    v = (value or "").strip().lower()
    out = []
    for ch in v:
        if ch.isalnum():
            out.append(ch)
        elif ch in {" ", "-", "_"}:
            if out and out[-1] != "-":
                out.append("-")
    tid = "".join(out).strip("-")
    return tid or DEFAULT_TEAM_ID


def _team_prefix(team_id: str) -> str:
    tid = (team_id or "").strip() or DEFAULT_TEAM_ID
    return f"{tid}::"


def _team_scoped_value(value: str, team_id: str) -> str:
    v = (value or "").strip()
    if not v:
        return v
    if team_id == DEFAULT_TEAM_ID:
        return v
    prefix = _team_prefix(team_id)
    return v if v.startswith(prefix) else prefix + v


def _team_unscoped_value(value: str, team_id: str) -> str:
    v = (value or "").strip()
    if not v:
        return v
    prefix = _team_prefix(team_id)
    return v[len(prefix) :] if v.startswith(prefix) else v


WORKDAY_MINUTES = 465

DEFAULT_TASKS = [
    Task(id="draw_check_pdm", label="Drawing check in PDM", estimate=2, category="For Drawings"),
    Task(id="part_drawing_creation", label="Part Drawing creation", estimate=25, category="For Drawings"),
    Task(id="part_drg_qc", label="Part Drg QC", estimate=15, category="For Drawings"),
    Task(id="assly_drg_creation", label="Assly drg creation", estimate=40, category="For Drawings"),
    Task(id="assly_drg_qc", label="Assly drg QC", estimate=15, category="For Drawings"),
    Task(id="swood_assly_drawing_creation", label="Swood Assembly Drawing creation", estimate=210, category="For Drawings"),
    Task(id="swood_drawing_qc", label="Swood drawing QC", estimate=30, category="For Drawings"),
    Task(id="swood_part_drawings", label="Swood Part Drawings", estimate=25, category="For Drawings"),
    Task(id="drawing_model_updates", label="Drawing related Model updates", estimate=20, category="For Drawings"),
    Task(id="part_check_pdm", label="Part Check PDM", estimate=2, category="For Modelings"),
    Task(id="part_model_creation", label="Part Model creation", estimate=30, category="For Modelings"),
    Task(id="main_assembly", label="Main Assembly", estimate=240, category="For Modelings"),
    Task(id="sub_assembly", label="Sub Assembly", estimate=20, category="For Modelings"),
    Task(id="hardware_data_search", label="Hardware Data search", estimate=5, category="For Modelings"),
    Task(id="assembly_qc_reporting", label="Assembly QC reporting", estimate=5, category="For Modelings"),
    Task(id="swood_assly_complete", label="Swood Assembly & Swood Complete", estimate=240, category="For Modelings"),
    Task(id="part_update_qc", label="Part update while QC", estimate=10, category="For Modelings"),
    Task(id="assembly_hardware_change", label="Assembly hardware change only", estimate=12, category="For Modelings"),
    Task(id="any_assembly_modifications", label="Any Assembly Modifications", estimate=50, category="For Modelings"),
    Task(id="any_parts_modifications", label="Any parts modifications", estimate=30, category="For Modelings"),
    Task(id="bom_excel_creation", label="BOM Excel creation", estimate=15, category="For Overall Datacard work"),
    Task(id="data_card_update", label="Data card update", estimate=2, category="For Overall Datacard work"),
    Task(id="data_card_error_solving", label="Data Card Error solving", estimate=5, category="For Overall Datacard work"),
    Task(id="configuration_updates", label="Configuration Updates", estimate=10, category="For Overall Datacard work"),
]

LEGACY_TASKS = {
    "dc_entry": {"label": "Data Card Entry (Legacy)", "estimate": 10},
    "dc_verify": {"label": "Data Card Verification (Legacy)", "estimate": 8},
    "check_overall": {"label": "Overall Checking (Legacy)", "estimate": 12},
    "finalize": {"label": "Finalization (Legacy)", "estimate": 6},
    "misc": {"label": "Other Task (Legacy)", "estimate": 5},
}


app = Flask(__name__)
DATABASE_URL = os.environ.get("DATABASE_URL")
SQLITE_PATH = os.environ.get("SQLITE_PATH") or os.path.join(os.path.dirname(__file__), "server_data", "master.sqlite3")
SECRET_KEY = os.environ.get("SECRET_KEY")
if not SECRET_KEY:
    if DATABASE_URL:
        raise RuntimeError("SECRET_KEY must be set when DATABASE_URL is configured for production.")
    SECRET_KEY = secrets.token_hex(32)
app.secret_key = SECRET_KEY
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PREFERRED_URL_SCHEME="https",
)
if os.environ.get("SESSION_COOKIE_SECURE") is not None:
    app.config["SESSION_COOKIE_SECURE"] = str(os.environ.get("SESSION_COOKIE_SECURE")).lower() in {"1", "true", "yes"}
elif os.environ.get("FLASK_ENV", "").lower() == "production" or DATABASE_URL:
    app.config["SESSION_COOKIE_SECURE"] = True

def ensure_db():
    global _db_initialized
    if not _db_initialized:
        with _db_init_lock:
            if not _db_initialized:
                _init_db()
                _db_initialized = True

app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
if os.environ.get("RENDER"):
    app.config["SESSION_COOKIE_SECURE"] = True


@app.before_request
def _assign_request_id():
    ensure_db()
    if request.path.startswith("/admin") and not _is_admin_session_valid():
        if request.path in {"/admin/login", "/admin/logout"}:
            pass
        elif request.path.startswith("/admin/login"):
            pass
        else:
            return redirect(url_for("admin_login_page"))
    g.request_id = request.headers.get("X-Request-ID") or secrets.token_hex(8)


@app.after_request
def _attach_request_id(response):
    rid = getattr(g, "request_id", None)
    if rid:
        response.headers["X-Request-ID"] = rid
    return response


@app.teardown_request
def _log_request_exception(exc):
    if not exc:
        return
    import sys
    import traceback

    rid = getattr(g, "request_id", None)
    payload = {
        "level": "error",
        "request_id": rid,
        "method": request.method,
        "path": request.path,
        "query": request.query_string.decode("utf-8", errors="ignore"),
        "remote_addr": request.headers.get("X-Forwarded-For") or request.remote_addr,
        "ua": request.headers.get("User-Agent"),
        "error": repr(exc),
    }
    print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)
    traceback.print_exc()


_TASK_CACHE_TTL_SECONDS = 15.0
_task_cache: dict[str, tuple[float, list[Task]]] = {}
_ADMIN_REPORT_CACHE_TTL_SECONDS = 20.0
_admin_report_cache: dict[str, tuple[float, str]] = {}
_tasks_version_cache: dict[str, tuple[float, str]] = {}
_teams_version_cache: tuple[float, str] | None = None

_pool = None
_db_initialized = False
_db_init_lock = threading.Lock()
_sqlite_swap_lock = threading.Lock()



def _pg():
    try:
        import psycopg2
        return psycopg2
    except ModuleNotFoundError as e:
        raise RuntimeError("psycopg2 not installed") from e

def _use_postgres() -> bool:
    return bool(DATABASE_URL) and str(DATABASE_URL).startswith(("postgres://", "postgresql://"))


def _pg_connect_args() -> dict[str, str]:
    sslmode = os.environ.get("PGSSLMODE") or os.environ.get("SSL_MODE") or os.environ.get("SSLMODE")
    if sslmode:
        return {"sslmode": sslmode}
    if DATABASE_URL and "sslmode=" not in DATABASE_URL.lower():
        return {"sslmode": "require"}
    return {}

def _adapt_sqlite_sql(sql: str) -> str:
    return (
        sql.replace("%s", "?")
        .replace("NOW()", "CURRENT_TIMESTAMP")
        .replace("::text", "")
        .replace(" ILIKE ", " LIKE ")
        .replace("TRUE", "1")
        .replace("FALSE", "0")
    )

def _sql(pg_sql: str, sqlite_sql: str | None = None) -> str:
    if _use_postgres():
        return pg_sql
    if sqlite_sql is not None:
        return sqlite_sql
    return _adapt_sqlite_sql(pg_sql)

def _get_pool():
    global _pool
    if _pool is not None:
        return _pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    _pool = ThreadedConnectionPool(
        1,
        int(os.environ.get("DB_POOL_MAX", "10")),
        DATABASE_URL,
        **_pg_connect_args(),
    )
    return _pool


def _today_str() -> str:
    return datetime.now(_APP_TZ).date().isoformat()


def _parse_date(value: str) -> str:
    return datetime.strptime(value, "%Y-%m-%d").date().isoformat()


def _period_range(anchor_date: str, period: str) -> tuple[str, str]:
    d = datetime.strptime(anchor_date, "%Y-%m-%d").date()
    if period == "week":
        start = d - timedelta(days=d.weekday())
        end = start + timedelta(days=6)
    elif period == "month":
        start = d.replace(day=1)
        if start.month == 12:
            next_month = start.replace(year=start.year + 1, month=1, day=1)
        else:
            next_month = start.replace(month=start.month + 1, day=1)
        end = next_month - timedelta(days=1)
    else:
        start = d
        end = d
    return start.isoformat(), end.isoformat()


def _minutes_label(minutes: float) -> str:
    value = int(round(float(minutes)))
    if value < 60:
        return f"{value} min"
    hours = value // 60
    mins = value % 60
    if mins == 0:
        return f"{hours} hr"
    return f"{hours}h {mins}m"


def _hours_value(minutes: float) -> float:
    return round(float(minutes) / 60.0, 2)


def _article_key(article: str) -> str:
    return (article or "").strip().lower()


def _dt_to_iso(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    return str(value)


_APP_TZ = ZoneInfo(os.environ.get("APP_TIMEZONE") or "Asia/Kolkata")
_LOCAL_TZ = _APP_TZ


def _dt_for_math(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
    if dt.tzinfo is not None:
        dt = dt.astimezone(_LOCAL_TZ).replace(tzinfo=None)
    return dt


def _date_range_list(start_date: str, end_date: str) -> list[str]:
    start_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
    if start_obj > end_obj:
        start_obj, end_obj = end_obj, start_obj
    out: list[str] = []
    cur = start_obj
    while cur <= end_obj:
        out.append(cur.isoformat())
        cur = cur + timedelta(days=1)
    return out


def _break_minutes_for_pauses(pauses: list[dict], entry_date: str, now_dt: datetime) -> float:
    is_today = entry_date == _today_str()
    break_seconds = 0
    for p in pauses:
        s = _dt_for_math(p.get("start"))
        if not s:
            continue
        if p.get("end"):
            e = _dt_for_math(p.get("end"))
            if not e:
                continue
        else:
            e = now_dt if is_today else s
        if e > s:
            break_seconds += int((e - s).total_seconds())
    return float(break_seconds) / 60.0


def _csv_response(rows: list[list[str]], filename: str) -> Response:
    def gen():
        from io import StringIO

        sio = StringIO()
        w = csv.writer(sio)
        for row in rows:
            sio.seek(0)
            sio.truncate(0)
            w.writerow(row)
            yield sio.getvalue()

    return Response(gen(), mimetype="text/csv", headers={"Content-Disposition": f'attachment; filename="{filename}"'})


def _env_flag(name: str) -> bool:
    v = os.environ.get(name)
    if v is None:
        return False
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _sqlite_restore_enabled() -> bool:
    if _use_postgres():
        return False
    return _env_flag("ALLOW_DB_RESTORE")


def _db_backup_payload(team_id: str | None = None) -> dict:
    tid = (team_id or "").strip()
    params: tuple = ()
    where = ""
    if tid:
        where = "WHERE team_id=%s"
        params = (tid,)
    with _db() as conn:
        entries = _fetchall(
            conn,
            _sql(
                f"SELECT id, team_id, entry_date::text AS entry_date, member, article, tasks_json, completed, created_at, NOW() AS exported_at FROM entries {where} ORDER BY created_at, id",
                f"SELECT id, team_id, entry_date AS entry_date, member, article, tasks_json, completed, created_at, CURRENT_TIMESTAMP AS exported_at FROM entries {where} ORDER BY created_at, id",
            ),
            params,
        )
        member_logins = _fetchall(
            conn,
            _sql(
                f"SELECT id, team_id, entry_date::text AS entry_date, member, login_at, logout_at FROM member_logins {where} ORDER BY entry_date, member",
                f"SELECT id, team_id, entry_date AS entry_date, member, login_at, logout_at FROM member_logins {where} ORDER BY entry_date, member",
            ),
            params,
        )
        member_pauses = _fetchall(
            conn,
            _sql(
                f"SELECT id, team_id, entry_date::text AS entry_date, member, pause_start, pause_end FROM member_pauses {where} ORDER BY entry_date, member, pause_start",
                f"SELECT id, team_id, entry_date AS entry_date, member, pause_start, pause_end FROM member_pauses {where} ORDER BY entry_date, member, pause_start",
            ),
            params,
        )
        tasks = _fetchall(conn, f"SELECT id, team_id, label, estimate, category, active FROM tasks {where} ORDER BY category, label", params)
        categories = _fetchall(conn, "SELECT name FROM task_categories ORDER BY name")
        if tid:
            team_rows = _fetchall(conn, "SELECT id, name FROM teams WHERE id=%s", (tid,))
            members_rows = _fetchall(conn, "SELECT team_id, member, active FROM team_members WHERE team_id=%s ORDER BY member", (tid,))
        else:
            team_rows = _fetchall(conn, "SELECT id, name FROM teams ORDER BY name")
            members_rows = _fetchall(conn, "SELECT team_id, member, active FROM team_members ORDER BY team_id, member")

    for r in entries:
        r["created_at"] = _dt_to_iso(r.get("created_at"))
        if "exported_at" in r:
            r.pop("exported_at", None)
        if isinstance(r.get("tasks_json"), str):
            try:
                r["tasks_json"] = json.loads(r["tasks_json"])
            except Exception:
                pass
    for r in member_logins:
        r["login_at"] = _dt_to_iso(r.get("login_at"))
        r["logout_at"] = _dt_to_iso(r.get("logout_at"))
    for r in member_pauses:
        r["pause_start"] = _dt_to_iso(r.get("pause_start"))
        r["pause_end"] = _dt_to_iso(r.get("pause_end"))
    for r in tasks:
        if not _use_postgres():
            r["active"] = bool(int(r.get("active") or 0))

    category_names = [str(c["name"]) for c in categories]
    if tid:
        prefix = _team_prefix(tid)
        if tid == DEFAULT_TEAM_ID:
            category_names = [c for c in category_names if ("::" not in c or c.startswith(prefix))]
        else:
            category_names = [c for c in category_names if c.startswith(prefix)]

    return {
        "meta": {
            "exported_at": datetime.now(_APP_TZ).isoformat(timespec="seconds"),
            "timezone": str(_APP_TZ),
            "db": "postgres" if _use_postgres() else "sqlite",
            "team_id": tid or None,
        },
        "tables": {
            "teams": team_rows,
            "team_members": members_rows,
            "entries": entries,
            "member_logins": member_logins,
            "member_pauses": member_pauses,
            "tasks": tasks,
            "task_categories": [{"name": n} for n in category_names],
        },
    }


def _stream_file(path: str, download_name: str) -> Response:
    def gen():
        with open(path, "rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                yield chunk

    return Response(
        gen(),
        mimetype="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{download_name}"'},
    )


def _stream_sqlite_snapshot(download_name: str) -> Response:
    os.makedirs(os.path.dirname(SQLITE_PATH), exist_ok=True)
    ensure_db()
    stamp = datetime.now(_APP_TZ).strftime("%Y%m%d_%H%M%S")
    snapshot_path = f"{SQLITE_PATH}.export_{stamp}"
    with _sqlite_swap_lock:
        shutil.copy2(SQLITE_PATH, snapshot_path)

    def gen():
        try:
            with open(snapshot_path, "rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    yield chunk
        finally:
            try:
                os.remove(snapshot_path)
            except Exception:
                pass

    return Response(
        gen(),
        mimetype="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{download_name}"'},
    )


@contextmanager
def _db():
    if _use_postgres():
        pool = _get_pool()
        conn = pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            pool.putconn(conn)
        return

    os.makedirs(os.path.dirname(SQLITE_PATH), exist_ok=True)
    conn = sqlite3.connect(SQLITE_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _fetchone(conn, sql: str, params: tuple | None = None) -> dict | None:
    if _use_postgres():
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
    else:
        cur = conn.cursor()
        cur.execute(_adapt_sqlite_sql(sql), params or ())
        row = cur.fetchone()
    return dict(row) if row else None


def _fetchall(conn, sql: str, params: tuple | None = None) -> list[dict]:
    if _use_postgres():
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
    else:
        cur = conn.cursor()
        cur.execute(_adapt_sqlite_sql(sql), params or ())
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def _execute(conn, sql: str, params: tuple | None = None) -> None:
    if _use_postgres():
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
        return
    cur = conn.cursor()
    cur.execute(_adapt_sqlite_sql(sql), params or ())


def _init_db() -> None:
    with _db() as conn:
        def _try(sql: str, params: tuple | None = None) -> None:
            try:
                _execute(conn, sql, params)
            except Exception:
                return

        if _use_postgres():
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS teams (
                  id TEXT PRIMARY KEY,
                  name TEXT NOT NULL UNIQUE,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """,
            )
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS team_members (
                  team_id TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
                  member TEXT NOT NULL,
                  active BOOLEAN NOT NULL DEFAULT TRUE,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  PRIMARY KEY (team_id, member)
                )
                """,
            )
        else:
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS teams (
                  id TEXT PRIMARY KEY,
                  name TEXT NOT NULL UNIQUE,
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """,
            )
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS team_members (
                  team_id TEXT NOT NULL,
                  member TEXT NOT NULL,
                  active INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  PRIMARY KEY (team_id, member)
                )
                """,
            )

        if _use_postgres():
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS app_state (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """,
            )
        else:
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS app_state (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL,
                  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """,
            )

        _execute(
            conn,
            _sql(
                "INSERT INTO teams(id, name) VALUES(%s,%s) ON CONFLICT (id) DO NOTHING",
                "INSERT OR IGNORE INTO teams(id, name) VALUES(?,?)",
            ),
            (DEFAULT_TEAM_ID, DEFAULT_TEAM_NAME),
        )
        for m in MEMBER_NAMES:
            _execute(
                conn,
                _sql(
                    "INSERT INTO team_members(team_id, member, active) VALUES(%s,%s,TRUE) ON CONFLICT DO NOTHING",
                    "INSERT OR IGNORE INTO team_members(team_id, member, active) VALUES(?,?,1)",
                ),
                (DEFAULT_TEAM_ID, m),
            )

        for t in _fetchall(conn, "SELECT id FROM teams"):
            tid = str(t["id"])
            _execute(
                conn,
                _sql(
                    "INSERT INTO app_state(key, value, updated_at) VALUES(%s,%s,NOW()) ON CONFLICT (key) DO NOTHING",
                    "INSERT OR IGNORE INTO app_state(key, value, updated_at) VALUES(?,?,CURRENT_TIMESTAMP)",
                ),
                (f"tasks_version:{tid}", "1"),
            )
        _execute(
            conn,
            _sql(
                "INSERT INTO app_state(key, value, updated_at) VALUES(%s,%s,NOW()) ON CONFLICT (key) DO NOTHING",
                "INSERT OR IGNORE INTO app_state(key, value, updated_at) VALUES(?,?,CURRENT_TIMESTAMP)",
            ),
            ("teams_version", "1"),
        )

        if _use_postgres():
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS entries (
                  id BIGSERIAL PRIMARY KEY,
                  team_id TEXT NOT NULL DEFAULT 'default',
                  entry_date DATE NOT NULL,
                  member TEXT NOT NULL,
                  article TEXT NOT NULL,
                  tasks_json JSONB NOT NULL,
                  completed BOOLEAN NOT NULL DEFAULT TRUE,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """,
            )
        else:
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS entries (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  team_id TEXT NOT NULL DEFAULT 'default',
                  entry_date TEXT NOT NULL,
                  member TEXT NOT NULL,
                  article TEXT NOT NULL,
                  tasks_json TEXT NOT NULL,
                  completed INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """,
            )
        _try(_sql("ALTER TABLE entries ADD COLUMN IF NOT EXISTS team_id TEXT NOT NULL DEFAULT 'default'", "ALTER TABLE entries ADD COLUMN team_id TEXT NOT NULL DEFAULT 'default'"))
        _try("UPDATE entries SET team_id='default' WHERE team_id IS NULL OR team_id=''")
        _execute(conn, "CREATE INDEX IF NOT EXISTS idx_entries_team_date ON entries(team_id, entry_date)")
        _execute(conn, "CREATE INDEX IF NOT EXISTS idx_entries_team_member_date ON entries(team_id, member, entry_date)")
        if _use_postgres():
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS member_logins (
                  id BIGSERIAL PRIMARY KEY,
                  team_id TEXT NOT NULL DEFAULT 'default',
                  entry_date DATE NOT NULL,
                  member TEXT NOT NULL,
                  login_at TIMESTAMPTZ NOT NULL,
                  logout_at TIMESTAMPTZ
                )
                """,
            )
        else:
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS member_logins (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  team_id TEXT NOT NULL DEFAULT 'default',
                  entry_date TEXT NOT NULL,
                  member TEXT NOT NULL,
                  login_at TEXT NOT NULL,
                  logout_at TEXT
                )
                """,
            )
        _try(_sql("ALTER TABLE member_logins ADD COLUMN IF NOT EXISTS team_id TEXT NOT NULL DEFAULT 'default'", "ALTER TABLE member_logins ADD COLUMN team_id TEXT NOT NULL DEFAULT 'default'"))
        _try("UPDATE member_logins SET team_id='default' WHERE team_id IS NULL OR team_id=''")
        _try("DROP INDEX IF EXISTS idx_member_logins_unique")
        _execute(conn, "CREATE UNIQUE INDEX IF NOT EXISTS idx_member_logins_unique ON member_logins(team_id, entry_date, member)")
        _execute(conn, "CREATE INDEX IF NOT EXISTS idx_member_logins_team_date ON member_logins(team_id, entry_date)")
        if _use_postgres():
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS member_pauses (
                  id BIGSERIAL PRIMARY KEY,
                  team_id TEXT NOT NULL DEFAULT 'default',
                  entry_date DATE NOT NULL,
                  member TEXT NOT NULL,
                  pause_start TIMESTAMPTZ NOT NULL,
                  pause_end TIMESTAMPTZ
                )
                """,
            )
        else:
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS member_pauses (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  team_id TEXT NOT NULL DEFAULT 'default',
                  entry_date TEXT NOT NULL,
                  member TEXT NOT NULL,
                  pause_start TEXT NOT NULL,
                  pause_end TEXT
                )
                """,
            )
        _try(_sql("ALTER TABLE member_pauses ADD COLUMN IF NOT EXISTS team_id TEXT NOT NULL DEFAULT 'default'", "ALTER TABLE member_pauses ADD COLUMN team_id TEXT NOT NULL DEFAULT 'default'"))
        _try("UPDATE member_pauses SET team_id='default' WHERE team_id IS NULL OR team_id=''")
        _execute(conn, "CREATE INDEX IF NOT EXISTS idx_member_pauses_team_member_date ON member_pauses(team_id, member, entry_date)")
        _execute(conn, "CREATE INDEX IF NOT EXISTS idx_member_pauses_team_date ON member_pauses(team_id, entry_date)")
        _execute(conn, "CREATE TABLE IF NOT EXISTS task_categories (name TEXT PRIMARY KEY)")
        if _use_postgres():
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS tasks (
                  id TEXT PRIMARY KEY,
                  team_id TEXT NOT NULL DEFAULT 'default',
                  label TEXT NOT NULL,
                  estimate INTEGER NOT NULL,
                  category TEXT NOT NULL,
                  active BOOLEAN NOT NULL DEFAULT TRUE
                )
                """,
            )
        else:
            _execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS tasks (
                  id TEXT PRIMARY KEY,
                  team_id TEXT NOT NULL DEFAULT 'default',
                  label TEXT NOT NULL,
                  estimate INTEGER NOT NULL,
                  category TEXT NOT NULL,
                  active INTEGER NOT NULL DEFAULT 1
                )
                """,
            )
        _try(_sql("ALTER TABLE tasks ADD COLUMN IF NOT EXISTS team_id TEXT NOT NULL DEFAULT 'default'", "ALTER TABLE tasks ADD COLUMN team_id TEXT NOT NULL DEFAULT 'default'"))
        _try("UPDATE tasks SET team_id='default' WHERE team_id IS NULL OR team_id=''")
        _execute(conn, "CREATE INDEX IF NOT EXISTS idx_tasks_team_category ON tasks(team_id, category)")
        _execute(conn, "CREATE INDEX IF NOT EXISTS idx_tasks_team_active ON tasks(team_id, active)")
        row = _fetchone(conn, "SELECT COUNT(1) AS c FROM tasks WHERE team_id=%s", (DEFAULT_TEAM_ID,))
        if row and int(row["c"]) == 0:
            for c in sorted({t.category for t in DEFAULT_TASKS}):
                _execute(conn, _sql("INSERT INTO task_categories(name) VALUES(%s) ON CONFLICT DO NOTHING", "INSERT OR IGNORE INTO task_categories(name) VALUES(?)"), (c,))
            for t in DEFAULT_TASKS:
                if _use_postgres():
                    _execute(
                        conn,
                        "INSERT INTO tasks(id, team_id, label, estimate, category, active) VALUES(%s,%s,%s,%s,%s,TRUE) ON CONFLICT DO NOTHING",
                        (t.id, DEFAULT_TEAM_ID, t.label, int(t.estimate), t.category),
                    )
                else:
                    _execute(
                        conn,
                        "INSERT OR IGNORE INTO tasks(id, team_id, label, estimate, category, active) VALUES(?,?,?,?,?,1)",
                        (t.id, DEFAULT_TEAM_ID, t.label, int(t.estimate), t.category),
                    )


def _require_admin() -> None:
    if not _is_admin_session_valid():
        abort(401)


def _admin_team_id() -> str:
    tid = (request.args.get("team") or "").strip()
    if not tid:
        tid = str(session.get("admin_team") or "").strip()
    tid = tid or DEFAULT_TEAM_ID
    if not _team_exists(tid):
        tid = DEFAULT_TEAM_ID
    session["admin_team"] = tid
    return tid


def _admin_password() -> str | None:
    pw = os.environ.get("ADMIN_PASSWORD")
    if not pw:
        return None
    return str(pw)


_admin_login_attempts: dict[str, list[float]] = {}
_ADMIN_LOGIN_WINDOW_SECONDS = 10 * 60.0
_ADMIN_LOGIN_MAX_ATTEMPTS = 8
_ADMIN_SESSION_TTL_SECONDS = float(os.environ.get("ADMIN_SESSION_TTL_SECONDS") or (12 * 60 * 60))


def _admin_login_client_key() -> str:
    forwarded = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
    return forwarded or (request.remote_addr or "unknown")


def _admin_session_fingerprint() -> str:
    ua = request.headers.get("User-Agent") or ""
    ip = _admin_login_client_key()
    raw = f"{ua}|{ip}|{SECRET_KEY}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _admin_token_sig(token: str, issued: int, fp: str) -> str:
    raw = f"{token}|{issued}|{fp}|{SECRET_KEY}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _grant_admin_session() -> None:
    token = secrets.token_urlsafe(24)
    issued = int(time.time())
    fp = _admin_session_fingerprint()
    session["admin"] = True
    session["admin_token"] = token
    session["admin_issued_at"] = issued
    session["admin_fp"] = fp
    session["admin_sig"] = _admin_token_sig(token, issued, fp)


def _clear_admin_session() -> None:
    session.pop("admin", None)
    session.pop("admin_token", None)
    session.pop("admin_issued_at", None)
    session.pop("admin_fp", None)
    session.pop("admin_sig", None)


def _is_admin_session_valid() -> bool:
    if not session.get("admin"):
        return False
    token = str(session.get("admin_token") or "")
    issued = int(session.get("admin_issued_at") or 0)
    fp = str(session.get("admin_fp") or "")
    sig = str(session.get("admin_sig") or "")
    if not token or issued <= 0 or not fp or not sig:
        return False
    if (time.time() - float(issued)) > _ADMIN_SESSION_TTL_SECONDS:
        return False
    if fp != _admin_session_fingerprint():
        return False
    return sig == _admin_token_sig(token, issued, fp)


def _team_exists(team_id: str) -> bool:
    tid = (team_id or "").strip() or DEFAULT_TEAM_ID
    with _db() as conn:
        row = _fetchone(conn, "SELECT id FROM teams WHERE id=%s LIMIT 1", (tid,))
    return bool(row)


def _all_teams() -> list[dict]:
    with _db() as conn:
        rows = _fetchall(conn, "SELECT id, name FROM teams ORDER BY name")
    return [{"id": str(r["id"]), "name": str(r["name"])} for r in rows]


def _team_members(team_id: str) -> list[str]:
    tid = (team_id or "").strip() or DEFAULT_TEAM_ID
    with _db() as conn:
        rows = _fetchall(conn, "SELECT member FROM team_members WHERE team_id=%s AND active=TRUE ORDER BY member", (tid,))
    return [str(r["member"]) for r in rows]


def _get_team_id() -> str:
    tid = (request.args.get("team") or "").strip()
    if not tid:
        tid = str(session.get("team") or "").strip()
    tid = tid or DEFAULT_TEAM_ID
    if not _team_exists(tid):
        tid = DEFAULT_TEAM_ID
    session["team"] = tid
    return tid


def _resolve_team_id(payload: dict | None = None) -> str:
    tid = ""
    if payload:
        tid = str(payload.get("team") or "").strip()
    if not tid:
        tid = (request.args.get("team") or "").strip()
    if not tid:
        tid = str(session.get("team") or "").strip()
    tid = tid or DEFAULT_TEAM_ID
    if not _team_exists(tid):
        tid = DEFAULT_TEAM_ID
    session["team"] = tid
    return tid


def _valid_member(member: str, team_id: str) -> bool:
    m = (member or "").strip()
    if not m:
        return False
    with _db() as conn:
        row = _fetchone(conn, "SELECT member FROM team_members WHERE team_id=%s AND member=%s AND active=TRUE LIMIT 1", (team_id, m))
    return bool(row)


def _load_tasks_from_db(active_only: bool, team_id: str) -> list[Task]:
    with _db() as conn:
        if active_only:
            rows = _fetchall(conn, "SELECT id, label, estimate, category FROM tasks WHERE team_id=%s AND active=TRUE ORDER BY category, label", (team_id,))
        else:
            rows = _fetchall(conn, "SELECT id, label, estimate, category FROM tasks WHERE team_id=%s ORDER BY category, label", (team_id,))
    return [Task(id=str(r["id"]), label=str(r["label"]), estimate=int(r["estimate"]), category=str(r["category"])) for r in rows]


def _team_tasks_version(team_id: str, use_cache: bool = True) -> str:
    tid = (team_id or "").strip() or DEFAULT_TEAM_ID
    now = time.time()
    cached = _tasks_version_cache.get(tid)
    if use_cache and cached and (now - cached[0]) < 3.0:
        return cached[1]
    key = f"tasks_version:{tid}"
    version = "0"
    try:
        with _db() as conn:
            row = _fetchone(conn, "SELECT value FROM app_state WHERE key=%s LIMIT 1", (key,))
            if row and row.get("value"):
                version = str(row["value"])
    except Exception:
        version = str(int(now))
    _tasks_version_cache[tid] = (now, version)
    return version


def _global_teams_version(use_cache: bool = True) -> str:
    global _teams_version_cache
    now = time.time()
    if use_cache and _teams_version_cache and (now - _teams_version_cache[0]) < 3.0:
        return _teams_version_cache[1]
    version = "0"
    try:
        with _db() as conn:
            row = _fetchone(conn, "SELECT value FROM app_state WHERE key=%s LIMIT 1", ("teams_version",))
            if row and row.get("value"):
                version = str(row["value"])
    except Exception:
        version = str(int(now))
    _teams_version_cache = (now, version)
    return version


def _bump_global_teams_version() -> None:
    global _teams_version_cache
    ver = str(int(time.time() * 1000))
    with _db() as conn:
        _execute(
            conn,
            _sql(
                "INSERT INTO app_state(key, value, updated_at) VALUES(%s,%s,NOW()) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()",
                "INSERT INTO app_state(key, value, updated_at) VALUES(?,?,CURRENT_TIMESTAMP) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
            ),
            ("teams_version", ver),
        )
    _teams_version_cache = (time.time(), ver)


def _bump_team_tasks_version(team_id: str) -> None:
    tid = (team_id or "").strip() or DEFAULT_TEAM_ID
    key = f"tasks_version:{tid}"
    ver = str(int(time.time() * 1000))
    with _db() as conn:
        _execute(
            conn,
            _sql(
                "INSERT INTO app_state(key, value, updated_at) VALUES(%s,%s,NOW()) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()",
                "INSERT INTO app_state(key, value, updated_at) VALUES(?,?,CURRENT_TIMESTAMP) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
            ),
            (key, ver),
        )
    _tasks_version_cache[tid] = (time.time(), ver)


def _current_tasks(team_id: str, active_only: bool = True) -> list[Task]:
    version = _team_tasks_version(team_id)
    key = f"{team_id}:{version}:active" if active_only else f"{team_id}:{version}:all"
    cached = _task_cache.get(key)
    now = time.time()
    if cached and (now - cached[0]) < _TASK_CACHE_TTL_SECONDS:
        return cached[1]
    tasks: list[Task] = []
    try:
        tasks = _load_tasks_from_db(active_only=active_only, team_id=team_id)
    except Exception:
        tasks = []
    if not tasks:
        tasks = list(DEFAULT_TASKS)
    _task_cache[key] = (now, tasks)
    return tasks


def _task_map(team_id: str, include_inactive: bool = False) -> dict[str, Task]:
    tasks = _current_tasks(team_id=team_id, active_only=not include_inactive)
    return {t.id: t for t in tasks}


def _normalize_task_ids(raw_ids: list[str], team_id: str) -> list[str]:
    ids: list[str] = []
    for item in raw_ids:
        if not isinstance(item, str):
            continue
        v = item.strip()
        if v:
            ids.append(v)
    deduped = list(dict.fromkeys(ids))
    task_by_id = _task_map(team_id=team_id, include_inactive=False)
    unknown = [t for t in deduped if t not in task_by_id]
    if unknown:
        raise ValueError(f"Unknown task id(s): {', '.join(unknown)}")
    return deduped


def _task_estimate(task_id: str, team_id: str) -> int:
    task_by_id = _task_map(team_id=team_id, include_inactive=True)
    if task_id in task_by_id:
        return task_by_id[task_id].estimate
    legacy = LEGACY_TASKS.get(task_id)
    if legacy:
        return int(legacy["estimate"])
    return 0


def _task_label(task_id: str, team_id: str) -> str:
    task_by_id = _task_map(team_id=team_id, include_inactive=True)
    if task_id in task_by_id:
        return task_by_id[task_id].label
    legacy = LEGACY_TASKS.get(task_id)
    if legacy:
        return str(legacy["label"])
    return f"Unknown Task ({task_id})"


def _member_login_record_for_date(member: str, entry_date: str, team_id: str) -> dict | None:
    with _db() as conn:
        row = _fetchone(conn, "SELECT login_at, logout_at FROM member_logins WHERE team_id=%s AND member=%s AND entry_date=%s LIMIT 1", (team_id, member, entry_date))
    if not row:
        if entry_date == _today_str():
            active = _active_shift_record(member, team_id)
            if active and active.get("login_at") and not active.get("logout_at"):
                return {"login_at": active["login_at"], "logout_at": active["logout_at"]}
        return None
    return {"login_at": _dt_to_iso(row["login_at"]), "logout_at": _dt_to_iso(row["logout_at"])}


def _active_shift_record(member: str, team_id: str) -> dict | None:
    today = _today_str()
    with _db() as conn:
        open_row = _fetchone(
            conn,
            _sql(
                "SELECT entry_date::text AS entry_date, login_at, logout_at FROM member_logins WHERE team_id=%s AND member=%s AND logout_at IS NULL ORDER BY entry_date DESC, login_at DESC LIMIT 1",
                "SELECT entry_date AS entry_date, login_at, logout_at FROM member_logins WHERE team_id=%s AND member=%s AND logout_at IS NULL ORDER BY entry_date DESC, login_at DESC LIMIT 1",
            ),
            (team_id, member),
        )
        if open_row:
            return {"entry_date": str(open_row["entry_date"]), "login_at": _dt_to_iso(open_row["login_at"]), "logout_at": _dt_to_iso(open_row["logout_at"])}

        row = _fetchone(
            conn,
            _sql(
                "SELECT entry_date::text AS entry_date, login_at, logout_at FROM member_logins WHERE team_id=%s AND member=%s AND entry_date=%s LIMIT 1",
                "SELECT entry_date AS entry_date, login_at, logout_at FROM member_logins WHERE team_id=%s AND member=%s AND entry_date=%s LIMIT 1",
            ),
            (team_id, member, today),
        )
    if not row:
        return None
    return {"entry_date": str(row["entry_date"]), "login_at": _dt_to_iso(row["login_at"]), "logout_at": _dt_to_iso(row["logout_at"])}


def _member_login_for_date(member: str, entry_date: str, team_id: str) -> str | None:
    rec = _member_login_record_for_date(member, entry_date, team_id)
    return rec["login_at"] if rec else None


def _pause_rows_for_member_date(member: str, entry_date: str, team_id: str) -> list[dict]:
    with _db() as conn:
        rows = _fetchall(conn, "SELECT pause_start, pause_end FROM member_pauses WHERE team_id=%s AND member=%s AND entry_date=%s ORDER BY pause_start", (team_id, member, entry_date))
    return [{"start": _dt_to_iso(r["pause_start"]), "end": _dt_to_iso(r["pause_end"])} for r in rows]


def _pause_state_for_member(member: str, entry_date: str, team_id: str) -> dict:
    pauses = _pause_rows_for_member_date(member, entry_date, team_id)
    closed_seconds = 0
    open_start = None
    for p in pauses:
        s = _dt_for_math(p.get("start"))
        if not s:
            continue
        if p.get("end"):
            e = _dt_for_math(p.get("end"))
            if not e:
                continue
            if e > s:
                closed_seconds += int((e - s).total_seconds())
        else:
            open_start = str(p["start"])
    return {"paused_closed_seconds": closed_seconds, "open_pause_start": open_start}


def _build_member_day_entries(member: str, entry_date: str, team_id: str) -> dict:
    login_rec = _member_login_record_for_date(member, entry_date, team_id)
    login_at = login_rec["login_at"] if login_rec else None
    logout_at = login_rec["logout_at"] if login_rec else None
    pauses = _pause_rows_for_member_date(member, entry_date, team_id)

    with _db() as conn:
        rows = _fetchall(
            conn,
            "SELECT id, entry_date::text AS entry_date, article, tasks_json, completed, created_at FROM entries WHERE team_id=%s AND member=%s AND entry_date=%s ORDER BY created_at, id",
            (team_id, member, entry_date),
        )

    task_state: dict[tuple[str, str], dict] = {}
    for r in rows:
        article_raw = str(r["article"])
        article_norm = _article_key(article_raw)
        created_at = r["created_at"]
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        completed = bool(r["completed"])
        task_ids = r["tasks_json"] if isinstance(r["tasks_json"], list) else json.loads(str(r["tasks_json"]))

        for tid in task_ids:
            task_id = str(tid)
            key = (article_norm, task_id)
            state = task_state.get(key)
            if state is None:
                state = {
                    "article": article_raw,
                    "task_id": task_id,
                    "first": created_at,
                    "last": created_at,
                    "start": None,
                    "spent": 0.0,
                    "spent_is_estimate": False,
                    "completed": False,
                }
                task_state[key] = state
            if state["completed"] and not completed:
                continue
            state["first"] = min(state["first"], created_at)
            state["last"] = max(state["last"], created_at)
            if not completed:
                if state["start"] is None:
                    state["start"] = created_at
                if float(state["spent"]) <= 0.0:
                    state["spent"] = float(_task_estimate(task_id, team_id))
                    state["spent_is_estimate"] = True
                state["completed"] = False
            else:
                if state["start"] is not None:
                    delta = max(0.0, (created_at - state["start"]).total_seconds() / 60.0)
                    state["spent"] = float(delta)
                    state["spent_is_estimate"] = False
                elif float(state["spent"]) <= 0.0:
                    state["spent"] = float(_task_estimate(task_id, team_id))
                    state["spent_is_estimate"] = True
                state["completed"] = True

    by_article: dict[str, dict] = {}
    for st in task_state.values():
        row = by_article.get(_article_key(st["article"]))
        if row is None:
            row = {
                "article": st["article"],
                "tasks": [],
                "task_spent": {},
                "completed": True,
                "spent_total": 0.0,
                "created_at": st["first"],
                "last_at": st["last"],
            }
            by_article[_article_key(st["article"])] = row
        row["tasks"].append(st["task_id"])
        row["task_spent"][st["task_id"]] = float(st["spent"])
        row["spent_total"] += float(st["spent"])
        row["created_at"] = min(row["created_at"], st["first"])
        row["last_at"] = max(row["last_at"], st["last"])
        if not st["completed"]:
            row["completed"] = False

    login_dt = _dt_for_math(login_at) if login_at else None
    logout_dt = _dt_for_math(logout_at) if logout_at else None

    def paused_seconds_until(until_at: datetime) -> int:
        if not login_dt:
            return 0
        until_at = _dt_for_math(until_at) or until_at
        total = 0
        for p in pauses:
            s = _dt_for_math(p.get("start"))
            if not s:
                continue
            e = _dt_for_math(p.get("end")) if p.get("end") else until_at
            if not e:
                continue
            e2 = min(e, until_at)
            start = max(login_dt, s)
            if e2 > start:
                total += int((e2 - start).total_seconds())
        return total

    entries = []
    total_productive = 0.0
    for a in by_article.values():
        effective_last = _dt_for_math(a["last_at"]) or a["last_at"]
        if logout_dt and logout_dt < effective_last:
            effective_last = logout_dt
        overtime = False
        if login_dt:
            net = int((effective_last - login_dt).total_seconds()) - paused_seconds_until(effective_last)
            overtime = net > WORKDAY_MINUTES * 60
        entries.append(
            {
                "article": a["article"],
                "tasks": list(dict.fromkeys(a["tasks"])),
                "task_spent": a["task_spent"],
                "task_labels": [_task_label(tid, team_id) for tid in list(dict.fromkeys(a["tasks"]))],
                "completed": bool(a["completed"]),
                "overtime": overtime,
                "spent_total": float(a["spent_total"]),
                "spent_total_label": _minutes_label(a["spent_total"]),
                "created_at_local": a["created_at"].isoformat(sep=" ", timespec="seconds"),
                "last_at_local": a["last_at"].isoformat(sep=" ", timespec="seconds"),
            }
        )
        total_productive += float(a["spent_total"])

    entries.sort(key=lambda x: x["created_at_local"], reverse=True)
    return {"entries": entries, "productive_min": total_productive, "login_at": login_at, "logout_at": logout_at, "pauses": pauses}


@app.get("/")
def home():
    selected_date = request.args.get("date") or _today_str()
    try:
        selected_date = _parse_date(selected_date)
    except Exception:
        selected_date = _today_str()

    team_id = _resolve_team_id()
    teams = _all_teams()
    members = _team_members(team_id)

    editable = selected_date == _today_str()
    active_tasks = _current_tasks(team_id=team_id, active_only=True)
    categories = sorted({_team_unscoped_value(t.category, team_id) for t in active_tasks})
    tasks_by_cat: dict[str, list[Task]] = {c: [] for c in categories}
    for t in active_tasks:
        tasks_by_cat[_team_unscoped_value(t.category, team_id)].append(t)
    estimate_labels = {t.id: _minutes_label(t.estimate) for t in active_tasks}
    task_meta = {t.id: {"label": t.label, "estimate": t.estimate, "category": t.category} for t in _current_tasks(team_id=team_id, active_only=False)}

    html = """
    <!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>Daily Report Tool - {{ team_name }}</title>
    <style>
      :root{--bg-a:#fff8ea;--bg-b:#ecf5f4;--bg-c:#e8eef8;--panel:rgba(255,255,255,.92);--line:#d7e0ea;--text:#182133;--muted:#607086;--primary:#0f766e;--primary-strong:#115e59;--primary-soft:#e8f7f5;--accent:#f59e0b;--danger:#b42318;--shadow:0 22px 50px rgba(24,33,51,.1)}
      *{box-sizing:border-box}
      body{font-family:"Trebuchet MS","Segoe UI",sans-serif;margin:0;padding:20px;background:radial-gradient(circle at top left,var(--bg-a) 0,#f8f1e7 22%,var(--bg-b) 58%,var(--bg-c) 100%);color:var(--text);min-height:100vh}
      .wrap{max-width:1280px;margin:0 auto}
      a{text-decoration:none}
      .top{display:flex;justify-content:space-between;align-items:flex-end;gap:16px;flex-wrap:wrap}
      .hero-card{padding:26px;border:1px solid rgba(255,255,255,.58);border-radius:26px;background:linear-gradient(145deg,rgba(255,255,255,.86),rgba(255,255,255,.68));box-shadow:var(--shadow);backdrop-filter:blur(10px)}
      .hero-copy{max-width:760px}
      .eyebrow{font-size:12px;font-weight:700;letter-spacing:.16em;text-transform:uppercase;color:var(--primary);margin-bottom:10px}
      .hero-badges{display:flex;gap:10px;flex-wrap:wrap;margin-top:14px}
      .date-chip{display:inline-flex;align-items:center;padding:9px 14px;border-radius:999px;background:rgba(255,255,255,.86);border:1px solid var(--line);font-size:12px;font-weight:700}
      .date-chip.warn{background:#fff5de;border-color:#f4d28a;color:#8a5a02}
      .nav-btn{display:inline-flex;align-items:center;justify-content:center;padding:12px 16px;border-radius:14px;background:rgba(255,255,255,.86);border:1px solid var(--line);color:var(--text);font-weight:700}
      .card{background:var(--panel);border:1px solid rgba(255,255,255,.66);border-radius:24px;padding:22px;margin-top:16px;box-shadow:var(--shadow);backdrop-filter:blur(10px)}
      .grid{display:grid;grid-template-columns:1.35fr .85fr;gap:18px}
      .form-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;align-items:end}
      .report-panel{margin-top:12px;padding:18px;border-radius:22px;background:rgba(255,255,255,.86);border:1px solid rgba(255,255,255,.9);min-height:200px}
      .field{display:flex;flex-direction:column}
      label:not(.task){font-size:12px;font-weight:700;color:var(--muted);display:block;margin:14px 0 8px;letter-spacing:.06em;text-transform:uppercase}
      input,select{width:100%;padding:12px 14px;border:1px solid var(--line);border-radius:14px;background:rgba(255,255,255,.96);font-size:14px;color:var(--text);outline:none}
      input:focus,select:focus{border-color:#86cfc5;box-shadow:0 0 0 4px rgba(15,118,110,.12)}
      button{padding:12px 16px;border:none;border-radius:14px;background:linear-gradient(135deg,var(--primary),var(--primary-strong));color:#fff;font-weight:700;cursor:pointer;box-shadow:0 18px 32px rgba(15,118,110,.24);transition:transform .18s ease,box-shadow .18s ease,background .18s ease}
      button:hover,.nav-btn:hover{transform:translateY(-1px)}
      button.secondary{background:rgba(255,255,255,.88);border:1px solid var(--line);color:var(--text);box-shadow:none}
      button:disabled{opacity:.5;cursor:not-allowed;transform:none;box-shadow:none}
      .btn-active{background:linear-gradient(135deg,var(--primary),var(--primary-strong))!important;color:#fff!important;box-shadow:0 18px 32px rgba(15,118,110,.24)!important}
      .btn-faded{opacity:.45}
      .muted{color:var(--muted);font-size:12px;line-height:1.55}
      .section-copy{margin:6px 0 0;color:var(--muted);font-size:13px;line-height:1.55}
      .status-box{margin-top:14px;padding:16px 18px;border-radius:18px;background:linear-gradient(135deg,#17324a,#214c61);color:#f7fbff;font-size:14px}
      .summary-chip{display:inline-flex;align-items:center;margin-top:14px;padding:10px 14px;border-radius:999px;background:var(--primary-soft);border:1px solid #c8ece8;color:var(--text);font-size:12px;font-weight:700}
      .tasks{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:14px;margin-top:12px}
      .cat{border:1px solid var(--line);border-radius:22px;padding:18px;background:rgba(255,255,255,.78)}
      .cat h3{margin:0 0 6px;font-size:16px}
      .task{display:flex;gap:12px;align-items:flex-start;border-top:1px dashed #dde6ee;padding:12px 0;margin:0}
      .task input[type=checkbox]{width:18px;height:18px;margin:3px 0 0;flex:0 0 auto;accent-color:var(--primary)}
      .task .task-text{flex:1;line-height:1.45}
      .task:first-of-type{border-top:0}
      .badge{font-size:11px;border:1px solid #f4d28a;background:#fff7e8;color:#8a5a02;border-radius:999px;padding:5px 10px;white-space:nowrap;flex:0 0 auto;font-weight:700}
      .actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:14px}
      #err{margin-top:14px;padding:12px 14px;border-radius:16px;background:rgba(255,255,255,.84);border:1px solid transparent;color:var(--muted);min-height:48px;font-size:13px}
      #err.error{color:var(--danger);background:#fff1f0;border-color:#f4c7c3}
      .empty-report{padding:28px 18px;border-radius:20px;border:1px dashed var(--line);background:rgba(255,255,255,.62);text-align:center;color:var(--muted);font-size:14px;line-height:1.6;margin-top:14px}
      table{width:100%;border-collapse:collapse;margin-top:12px}
      th,td{border-bottom:1px solid #e8edf3;padding:10px 8px;font-size:13px;text-align:left;vertical-align:top}
      th{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.05em}
      @media (max-width:980px){.grid{grid-template-columns:1fr}.top{align-items:flex-start}.hero-card,.card{padding:18px}}
    </style></head><body><div class="wrap">
      <div class="top hero-card">
        <div class="hero-copy">
          <div class="eyebrow">Production Tracking Workspace</div>
          <h2 style="margin:0;font-size:clamp(28px,4vw,42px);line-height:1.05">Daily Report Tool - {{ team_name }}</h2>
          <div class="section-copy">Track live work, manage shift actions, and review one-day output from one cleaner dashboard.</div>
          <div class="hero-badges">
            <span class="date-chip">Date {{ selected_date }}</span>
            <span class="date-chip">Team {{ team_name }}</span>
            <span class="date-chip">{{ members|length }} Members</span>
            {% if not editable %}<span class="date-chip warn">Read Only View</span>{% endif %}
          </div>
          <div class="hero-badges" style="margin-top:10px">
            {% for t in teams %}
              <a class="date-chip" href="/?team={{ t.id }}" style="cursor:pointer;{% if t.id == team_id %}background:#e8f7f5;border-color:#86cfc5;{% endif %}"> {{ t.name }} </a>
            {% endfor %}
          </div>
        </div>
        <div><a href="{{ url_for('admin_login_page') }}" class="nav-btn">Open Admin</a></div>
      </div>
      <div class="grid">
        <div class="card">
          <h3 style="margin:0 0 8px">Daily Workboard</h3>
          <div class="section-copy">Record work entries for the day with clearer shift controls and grouped task estimates.</div>
          <div class="form-grid">
            <div class="field"><label>Member</label><select id="member"><option value="">Select</option>{% for m in members %}<option value="{{ m }}">{{ m }}</option>{% endfor %}</select></div>
            <div class="field"><label>Today</label><input id="today" value="{{ today }}" readonly></div>
          </div>
          <div class="actions">
            <button id="loginBtn" class="secondary" {% if not editable %}disabled{% endif %}>Login</button>
            <button id="pauseBtn" class="secondary" {% if not editable %}disabled{% endif %}>Pause</button>
            <button id="resumeBtn" class="secondary" {% if not editable %}disabled{% endif %}>Resume</button>
            <button id="logoutBtn" class="secondary" {% if not editable %}disabled{% endif %}>Logout</button>
          </div>
          <div id="status" class="status-box muted">Select member and login.</div>
          <label>Article Number</label><input id="article" placeholder="Enter article">
          <div class="task" style="border-top:0"><input id="completed" type="checkbox" checked><div class="task-text">Task Completed</div><div class="badge">Unchecked = In Progress</div></div>
          <div class="muted" style="margin-top:10px">Tasks</div>
          <div id="selectionSummary" class="summary-chip">No tasks selected</div>
          <div class="tasks">
            {% for cat in categories %}
              <div class="cat"><h3>{{ cat }}</h3>
                {% for t in tasks_by_cat[cat] %}
                  <label class="task"><input class="taskBox" type="checkbox" value="{{ t.id }}"><div class="task-text">{{ t.label }}</div><div class="badge">{{ estimate_labels[t.id] }}</div></label>
                {% endfor %}
              </div>
            {% endfor %}
          </div>
          <div class="actions">
            <button id="saveBtn" {% if not editable %}disabled{% endif %}>Save</button>
            <button id="clearBtn" class="secondary" {% if not editable %}disabled{% endif %}>Clear</button>
          </div>
          <div id="err" class="muted" style="color:#991b1b"></div>
        </div>

        <div class="card">
          <h3 style="margin:0 0 8px">Member Report (One Day)</h3>
          <div class="section-copy">Load a one-day snapshot with entries, tasks, time spent, and article coverage.</div>
          <div class="form-grid">
            <div class="field"><label>Report Date</label><input id="reportDate" type="date" max="{{ today }}" value="{{ selected_date }}"></div>
            <div class="field"><label>Member</label><select id="reportMember"><option value="">Select</option>{% for m in members %}<option value="{{ m }}">{{ m }}</option>{% endfor %}</select></div>
          </div>
          <div class="actions"><button id="loadBtn" class="secondary">Load</button></div>
          <div id="reportArea" class="report-panel"><div class="empty-report">Choose a member and date to load the daily report.</div></div>
        </div>
      </div>
    </div>
    <script>
      const taskMeta = {{ task_meta | tojson }};
      const teamId = "{{ team_id }}";
      let syncVersion = "{{ sync_version }}";
      const isEditable = {{ "true" if editable else "false" }};
      const appTimeZone = "{{ app_timezone }}";
      let state = { loggedIn:false, loggedOut:false, openPause:false, loginAt:null, logoutAt:null, pausedClosed:0, openPauseStart:null, timerId:null };

      function selectedTasks(){ return Array.from(document.querySelectorAll(".taskBox")).filter(b=>b.checked).map(b=>b.value); }
      function setMessage(message, isError=false){
        const el = document.getElementById("err");
        el.textContent = message || "";
        el.classList.toggle("error", !!isError);
      }
      function resetState(){
        if(state.timerId){ clearInterval(state.timerId); }
        state = { loggedIn:false, loggedOut:false, openPause:false, loginAt:null, logoutAt:null, pausedClosed:0, openPauseStart:null, timerId:null };
      }
      function updateSelectionSummary(){
        const ids = selectedTasks();
        const total = ids.reduce((sum, id) => sum + Number(taskMeta[id]?.estimate || 0), 0);
        const el = document.getElementById("selectionSummary");
        if(!ids.length){ el.textContent = "No tasks selected"; return; }
        el.textContent = `${ids.length} task${ids.length===1?"":"s"} selected • ${formatMinutesLabel(total)}`;
      }
      function setBtn(btn, enabled, active){
        const allow = !!isEditable && !!enabled;
        btn.disabled = !allow;
        btn.classList.toggle("btn-active", !!active && allow);
        btn.classList.toggle("btn-faded", !allow || !active);
      }

      function refreshButtons(){
        const hasMember = !!document.getElementById("member").value;
        const loginBtn = document.getElementById("loginBtn");
        const pauseBtn = document.getElementById("pauseBtn");
        const resumeBtn = document.getElementById("resumeBtn");
        const logoutBtn = document.getElementById("logoutBtn");
        if(!isEditable){ [loginBtn,pauseBtn,resumeBtn,logoutBtn].forEach(b=>setBtn(b,false,false)); return; }
        if(!hasMember){ [loginBtn,pauseBtn,resumeBtn,logoutBtn].forEach(b=>setBtn(b,false,false)); return; }
        if(!state.loggedIn){ setBtn(loginBtn,true,true); setBtn(pauseBtn,false,false); setBtn(resumeBtn,false,false); setBtn(logoutBtn,false,false); return; }
        if(state.loggedOut){ [loginBtn,pauseBtn,resumeBtn,logoutBtn].forEach(b=>setBtn(b,false,false)); return; }
        setBtn(loginBtn,false,false); setBtn(logoutBtn,true,true);
        if(state.openPause){ setBtn(pauseBtn,false,false); setBtn(resumeBtn,true,true); } else { setBtn(pauseBtn,true,true); setBtn(resumeBtn,false,false); }
      }

      function refreshSave(){
        const saveBtn = document.getElementById("saveBtn");
        const hasMember = !!document.getElementById("member").value;
        saveBtn.disabled = !isEditable || !hasMember || !state.loggedIn || state.loggedOut || selectedTasks().length===0;
      }

      function renderStatus(){
        const el = document.getElementById("status");
        const member = document.getElementById("member").value;
        if(!member){ el.textContent = isEditable ? "Select a member to begin the day." : "Past dates are view-only. Use the report panel to review saved entries."; return; }
        if(!isEditable){ el.textContent = "This date is locked for editing. Use the member report panel to review saved work."; return; }
        if(!state.loggedIn){ el.textContent = "Ready to log the shift. Start with Login."; return; }
        const loginText = state.loginAt ? state.loginAt.replace("T"," ") : "";
        const logoutText = state.loggedOut && state.logoutAt ? (" Logged out at " + state.logoutAt.replace("T"," ") + ".") : "";
        const breakText = state.openPause ? " Break is active." : "";
        el.textContent = "Logged in at " + loginText + "." + logoutText + breakText;
      }

      function startTimer(){
        const el = document.getElementById("status");
        if(state.timerId) clearInterval(state.timerId);
        function tick(){
          if(!state.loggedIn || !state.loginAt) return;
          const loginMs = new Date(state.loginAt).getTime();
          const nowMs = (state.loggedOut && state.logoutAt) ? new Date(state.logoutAt).getTime() : Date.now();
          const openPauseSec = (state.openPause && state.openPauseStart) ? Math.max(0, Math.floor((Date.now()-new Date(state.openPauseStart).getTime())/1000)) : 0;
          const elapsed = Math.max(0, Math.floor((nowMs - loginMs)/1000));
          const productive = Math.max(0, elapsed - (state.pausedClosed||0) - openPauseSec);
          const hh = String(Math.floor(productive/3600)).padStart(2,"0");
          const mm = String(Math.floor((productive%3600)/60)).padStart(2,"0");
          const ss = String(productive%60).padStart(2,"0");
          const ot = productive > 465*60 ? " (Overtime)" : "";
          const br = state.openPause ? " (On Break)" : "";
          el.textContent = (state.loggedOut? "Logged out. ":"") + "Login: " + state.loginAt.replace("T"," ") + ". Timer: " + hh+":"+mm+":"+ss + ot + br;
        }
        tick();
        state.timerId = setInterval(tick,1000);
      }

      async function refreshStatus(){
        const member = document.getElementById("member").value;
        if(!member || !isEditable){ resetState(); renderStatus(); refreshButtons(); refreshSave(); updateSelectionSummary(); return; }
        const res = await fetch(`/api/member-login-status?team=${encodeURIComponent(teamId)}&member=${encodeURIComponent(member)}`);
        const data = await res.json().catch(()=>null);
        if(!res.ok || !data){ setMessage("Unable to load member status.", true); return; }
        const pageToday = document.getElementById("today").value;
        const dayChanged = !!(data.today && pageToday && data.today !== pageToday);
        if(dayChanged){
          document.getElementById("today").value = data.today;
          document.getElementById("reportDate").max = data.today;
        }
        state.loggedIn = !!data.logged_in;
        state.loggedOut = !!data.logged_out;
        state.loginAt = data.login_at;
        state.logoutAt = data.logout_at;
        state.pausedClosed = Number(data.paused_closed_seconds||0);
        state.openPauseStart = data.open_pause_start;
        state.openPause = !!data.open_pause_start && !state.loggedOut;
        if(dayChanged){
          if(state.loggedIn && !state.loggedOut){
            setMessage(`New day started (${data.today}). Continuing current login session. Entries will save under today's date.`);
          } else {
            setMessage(`New day started (${data.today}). Please login to start today's work.`);
          }
        }
        renderStatus();
        refreshButtons();
        refreshSave();
        updateSelectionSummary();
        if(state.loggedIn) startTimer(); else if(state.timerId) clearInterval(state.timerId);
      }

      async function postJson(url, body){
        const res = await fetch(url,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({ ...body, team: teamId })});
        const data = await res.json().catch(()=>null);
        if(!res.ok) throw new Error(data?.error||"Request failed");
        return data;
      }

      document.getElementById("member").addEventListener("change", ()=>{ setMessage("Ready when you are."); refreshStatus(); });
      document.querySelectorAll(".taskBox").forEach(b=>b.addEventListener("change", ()=>{ updateSelectionSummary(); refreshSave(); }));
      document.getElementById("loginBtn").addEventListener("click", async ()=>{
        try{
          setMessage("Recording login...");
          const out = await postJson("/api/member-login",{member:document.getElementById("member").value});
          if(out?.already_logged_out){
            setMessage("Already logged out for today. Ask admin to reset logout if this is wrong.", true);
          } else if(out?.already_logged_in){
            setMessage("Already logged in.");
          } else {
            setMessage("Login recorded.");
          }
        }catch(e){
          setMessage(e.message, true);
        }
        await refreshStatus();
      });
      document.getElementById("pauseBtn").addEventListener("click", async ()=>{ try{ setMessage("Starting break..."); await postJson("/api/member-pause",{member:document.getElementById("member").value}); setMessage("Break started."); }catch(e){ setMessage(e.message, true); } await refreshStatus(); });
      document.getElementById("resumeBtn").addEventListener("click", async ()=>{ try{ setMessage("Ending break..."); await postJson("/api/member-resume",{member:document.getElementById("member").value}); setMessage("Break ended."); }catch(e){ setMessage(e.message, true); } await refreshStatus(); });
      document.getElementById("logoutBtn").addEventListener("click", async ()=>{ try{ setMessage("Recording logout..."); await postJson("/api/member-logout",{member:document.getElementById("member").value}); setMessage("Logout recorded."); }catch(e){ setMessage(e.message, true); } await refreshStatus(); refreshSave(); });

      document.getElementById("clearBtn").addEventListener("click", ()=>{ document.getElementById("article").value=""; document.getElementById("completed").checked=true; document.querySelectorAll(".taskBox").forEach(b=>b.checked=false); updateSelectionSummary(); refreshSave(); setMessage("Form cleared."); });
      document.getElementById("saveBtn").addEventListener("click", async ()=>{
        setMessage("Saving entry...");
        const member = document.getElementById("member").value;
        const article = document.getElementById("article").value.trim();
        const completed = document.getElementById("completed").checked;
        const tasks = selectedTasks();
        try{ await postJson("/api/entries",{member,article,tasks,completed}); document.getElementById("article").value=""; document.querySelectorAll(".taskBox").forEach(b=>b.checked=false); updateSelectionSummary(); refreshSave(); setMessage("Entry saved."); }catch(e){ setMessage(e.message, true); }
      });

      function formatMinutesLabel(minutes) {
        const value = Math.round(Number(minutes) || 0);
        if (value < 60) return `${value} min`;
        const h = Math.floor(value / 60);
        const m = value % 60;
        if (m === 0) return `${h} hr`;
        return `${h}h ${m}m`;
      }
      function escapeHtml(v){ const d=document.createElement("div"); d.textContent=v||""; return d.innerHTML; }
      async function loadReport(){
        const member = document.getElementById("reportMember").value;
        const d = document.getElementById("reportDate").value;
        if(!member){ document.getElementById("reportArea").textContent="Select member."; return; }
        const res = await fetch(`/api/report?team=${encodeURIComponent(teamId)}&member=${encodeURIComponent(member)}&date=${encodeURIComponent(d)}`);
        const data = await res.json().catch(()=>null);
        if(!res.ok){ document.getElementById("reportArea").textContent=data?.error||"Failed"; return; }
        const taskTotals = {};
        const articleSet = new Set();
        (data.entries||[]).forEach((e) => {
          articleSet.add(e.article);
          const spentMap = e.task_spent || {};
          (e.tasks||[]).forEach((tid) => {
            taskTotals[tid] = (taskTotals[tid] || 0) + Number(spentMap[tid] || 0);
          });
        });
        const taskLines = Object.keys(taskTotals).sort((a,b)=>taskTotals[b]-taskTotals[a]).map((tid)=>`<div class="muted" style="display:flex;justify-content:space-between;gap:10px;"><span>${escapeHtml(taskMeta[tid]?.label||tid)}</span><span><b>${escapeHtml(formatMinutesLabel(taskTotals[tid]))}</b></span></div>`).join("");
        const hubsBox = `<div class="card" style="margin-top:12px;background:#fbfdff;border:1px solid #e2e8f0;"><div style="display:flex;justify-content:space-between;align-items:baseline;gap:10px;flex-wrap:wrap;"><h4 style="margin:0;">HUBS</h4><div class="muted">Articles: <b>${articleSet.size}</b></div></div><div class="muted" style="margin-top:6px;">${Array.from(articleSet).filter(Boolean).map(escapeHtml).join(", ") || "-"}</div><div style="margin-top:10px;">${taskLines || "<div class='muted'>No tasks.</div>"}</div></div>`;
        const rows = data.entries.map(e=>`<tr><td>${e.created_at_local}</td><td>${escapeHtml(e.article)}</td><td>${e.completed?"Completed":"In Progress"}</td><td>${escapeHtml(e.tasks.map(t=>taskMeta[t]?.label||t).join(", "))}</td><td>${e.spent_total_label}</td></tr>`).join("");
        document.getElementById("reportArea").innerHTML = hubsBox + `<div class="muted" style="margin-top:10px;">Productive: <b>${data.day_total_label}</b> (${data.day_total_hours} hr) | Break: <b>${data.break_total_label}</b> (${data.break_total_hours} hr) | Free: <b>${data.free_total_label||"-"}</b> | Overtime: <b>${data.overtime_total_label||"-"}</b></div>` +
          (data.entries.length? `<table><thead><tr><th>Time</th><th>Article</th><th>Status</th><th>Tasks</th><th>Spent</th></tr></thead><tbody>${rows}</tbody></table>` : "<div class='muted'>No entries.</div>");
      }
      document.getElementById("loadBtn").addEventListener("click", loadReport);

      function tzNow(tz){
        const s = new Date().toLocaleString("en-US",{ timeZone: tz });
        return new Date(s);
      }
      function scheduleMidnightSwitch(){
        if(!isEditable) return;
        const now = tzNow(appTimeZone);
        const next = new Date(now);
        next.setHours(24,0,0,0);
        const wait = Math.max(1000, next.getTime() - now.getTime());
        setTimeout(() => {
          if(!isEditable) return;
          refreshStatus();
          scheduleMidnightSwitch();
        }, wait);
      }

      async function refreshTaskVersion(){
        try{
          const res = await fetch(`/api/sync-version?team=${encodeURIComponent(teamId)}`);
          const data = await res.json().catch(()=>null);
          if(!res.ok || !data?.sync_version) return;
          if(String(data.sync_version) !== String(syncVersion)){
            setMessage("Tasks/categories were updated by admin. Refreshing...", false);
            setTimeout(()=>window.location.reload(), 600);
            return;
          }
        }catch(_e){}
      }

      updateSelectionSummary();
      refreshStatus();
      refreshSave();
      scheduleMidnightSwitch();
      setInterval(()=>{ if(isEditable) refreshStatus(); }, 60000);
      setInterval(refreshTaskVersion, 15000);
    </script></body></html>
    """

    return render_template_string(
        html,
        team_id=team_id,
        team_name=next((t["name"] for t in teams if t["id"] == team_id), DEFAULT_TEAM_NAME),
        teams=teams,
        selected_date=selected_date,
        today=_today_str(),
        editable=editable,
        app_timezone=str(_APP_TZ),
        sync_version=f"{_team_tasks_version(team_id)}:{_global_teams_version()}",
        members=members,
        categories=categories,
        tasks_by_cat=tasks_by_cat,
        estimate_labels=estimate_labels,
        task_meta=task_meta,
    )


@app.get("/status")
def status():
    """Simple status check that doesn't require database."""
    return jsonify({
        "status": "running",
        "service": "Daily Report Tool",
        "db_configured": bool(DATABASE_URL),
        "db_initialized": _db_initialized,
    })

@app.get("/healthz")
def healthz():
    try:
        with _db() as conn:
            _fetchone(conn, "SELECT 1 AS ok")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/api/member-login-status")
def member_login_status():
    member = (request.args.get("member") or "").strip()
    team_id = _resolve_team_id()
    if not _valid_member(member, team_id):
        return jsonify({"error": "Select a valid member."}), 400
    entry_date = _today_str()
    shift = _active_shift_record(member, team_id)
    active_date = shift.get("entry_date") if shift else entry_date
    pause = _pause_state_for_member(member, active_date, team_id)
    return jsonify(
        {
            "today": entry_date,
            "active_entry_date": active_date,
            "logged_in": bool(shift and shift.get("login_at")),
            "logged_out": bool(shift and shift.get("logout_at")),
            "login_at": shift.get("login_at") if shift else None,
            "logout_at": shift.get("logout_at") if shift else None,
            "paused_closed_seconds": int(pause["paused_closed_seconds"]),
            "open_pause_start": pause["open_pause_start"],
        }
    )


@app.post("/api/member-login")
def member_login():
    payload = request.get_json(silent=True) or {}
    team_id = _resolve_team_id(payload)
    member = (payload.get("member") or "").strip()
    if not _valid_member(member, team_id):
        return jsonify({"error": "Select a valid member."}), 400
    entry_date = _today_str()
    active = _active_shift_record(member, team_id)
    if active and active.get("login_at") and not active.get("logout_at"):
        return jsonify({"ok": True, "already_logged_in": True})
    with _db() as conn:
        existing = _fetchone(conn, "SELECT login_at, logout_at FROM member_logins WHERE team_id=%s AND member=%s AND entry_date=%s LIMIT 1", (team_id, member, entry_date))
        if existing:
            return jsonify({"ok": True, "already_logged_in": True, "already_logged_out": bool(existing.get("logout_at"))})
        _execute(conn, "INSERT INTO member_logins(team_id, entry_date, member, login_at, logout_at) VALUES(%s,%s,%s,NOW(),NULL)", (team_id, entry_date, member))
    return jsonify({"ok": True})


@app.post("/api/member-pause")
def member_pause():
    payload = request.get_json(silent=True) or {}
    team_id = _resolve_team_id(payload)
    member = (payload.get("member") or "").strip()
    if not _valid_member(member, team_id):
        return jsonify({"error": "Select a valid member."}), 400
    shift = _active_shift_record(member, team_id)
    if not shift or not shift.get("login_at"):
        return jsonify({"error": "Member must login first."}), 400
    if shift.get("logout_at"):
        return jsonify({"error": "Member already logged out."}), 400
    entry_date = str(shift.get("entry_date") or _today_str())
    with _db() as conn:
        open_row = _fetchone(conn, "SELECT id FROM member_pauses WHERE team_id=%s AND member=%s AND entry_date=%s AND pause_end IS NULL LIMIT 1", (team_id, member, entry_date))
        if open_row:
            return jsonify({"ok": True})
        _execute(conn, "INSERT INTO member_pauses(team_id, entry_date, member, pause_start, pause_end) VALUES(%s,%s,%s,NOW(),NULL)", (team_id, entry_date, member))
    return jsonify({"ok": True})


@app.post("/api/member-resume")
def member_resume():
    payload = request.get_json(silent=True) or {}
    team_id = _resolve_team_id(payload)
    member = (payload.get("member") or "").strip()
    if not _valid_member(member, team_id):
        return jsonify({"error": "Select a valid member."}), 400
    shift = _active_shift_record(member, team_id)
    if not shift or shift.get("logout_at"):
        return jsonify({"error": "Member is not active."}), 400
    entry_date = str(shift.get("entry_date") or _today_str())
    with _db() as conn:
        _execute(conn, "UPDATE member_pauses SET pause_end=NOW() WHERE team_id=%s AND member=%s AND entry_date=%s AND pause_end IS NULL", (team_id, member, entry_date))
    return jsonify({"ok": True})


@app.post("/api/member-logout")
def member_logout():
    payload = request.get_json(silent=True) or {}
    team_id = _resolve_team_id(payload)
    member = (payload.get("member") or "").strip()
    if not _valid_member(member, team_id):
        return jsonify({"error": "Select a valid member."}), 400
    shift = _active_shift_record(member, team_id)
    if not shift or not shift.get("login_at"):
        return jsonify({"error": "Member must login first."}), 400
    if shift.get("logout_at"):
        return jsonify({"ok": True})
    entry_date = str(shift.get("entry_date") or _today_str())
    with _db() as conn:
        _execute(conn, "UPDATE member_pauses SET pause_end=NOW() WHERE team_id=%s AND member=%s AND entry_date=%s AND pause_end IS NULL", (team_id, member, entry_date))
        _execute(conn, "UPDATE member_logins SET logout_at=NOW() WHERE team_id=%s AND member=%s AND entry_date=%s", (team_id, member, entry_date))
    return jsonify({"ok": True})


@app.post("/api/entries")
def create_entry():
    payload = request.get_json(silent=True) or {}
    team_id = _resolve_team_id(payload)
    member = (payload.get("member") or "").strip()
    article = (payload.get("article") or "").strip()
    tasks = payload.get("tasks") or []
    completed = bool(payload.get("completed", True))

    if not _valid_member(member, team_id):
        return jsonify({"error": "Select a valid member."}), 400
    if not article:
        return jsonify({"error": "Article number is required."}), 400
    if not isinstance(tasks, list):
        return jsonify({"error": "Tasks must be a list."}), 400
    try:
        task_ids = _normalize_task_ids(tasks, team_id)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    if not task_ids:
        return jsonify({"error": "Select at least one task."}), 400

    entry_date = _today_str()
    shift = _active_shift_record(member, team_id)
    if not shift or not shift.get("login_at"):
        return jsonify({"error": "Member must login before saving entries."}), 400
    if shift.get("logout_at"):
        return jsonify({"error": "Member already logged out. Saving is blocked."}), 400

    with _db() as conn:
        _execute(
            conn,
            "INSERT INTO entries(team_id, entry_date, member, article, tasks_json, completed, created_at) VALUES(%s,%s,%s,%s,%s,%s,NOW())",
            (team_id, entry_date, member, article, json.dumps(task_ids), completed),
        )
    return jsonify({"ok": True})


@app.get("/api/report")
def report():
    member = (request.args.get("member") or "").strip()
    requested_date = request.args.get("date") or _today_str()
    team_id = _resolve_team_id()
    if not _valid_member(member, team_id):
        return jsonify({"error": "Select a valid member."}), 400
    try:
        requested_date = _parse_date(requested_date)
    except Exception:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400

    data = _build_member_day_entries(member, requested_date, team_id)
    pauses = data["pauses"]
    break_seconds = 0
    now_dt = datetime.now()
    for p in pauses:
        s = _dt_for_math(p.get("start"))
        if not s:
            continue
        if p.get("end"):
            e = _dt_for_math(p.get("end"))
            if not e:
                continue
        else:
            e = now_dt if requested_date == _today_str() else s
        if e > s:
            break_seconds += int((e - s).total_seconds())
    break_min = float(break_seconds) / 60.0

    free_min = None
    overtime_min = None
    if data["login_at"]:
        free_min = max(0.0, float(WORKDAY_MINUTES) - float(data["productive_min"]) - break_min)
        overtime_min = max(0.0, float(data["productive_min"]) + break_min - float(WORKDAY_MINUTES))

    return jsonify(
        {
            "date": requested_date,
            "member": member,
            "entries": data["entries"],
            "day_total": data["productive_min"],
            "day_total_label": _minutes_label(data["productive_min"]),
            "day_total_hours": _hours_value(data["productive_min"]),
            "break_total": break_min,
            "break_total_label": _minutes_label(break_min),
            "break_total_hours": _hours_value(break_min),
            "free_total": free_min,
            "free_total_label": _minutes_label(free_min) if free_min is not None else None,
            "free_total_hours": _hours_value(free_min) if free_min is not None else None,
            "overtime_total": overtime_min,
            "overtime_total_label": _minutes_label(overtime_min) if overtime_min is not None else None,
            "overtime_total_hours": _hours_value(overtime_min) if overtime_min is not None else None,
        }
    )


@app.get("/api/tasks-version")
def tasks_version():
    team_id = _resolve_team_id()
    return jsonify({"team": team_id, "version": _team_tasks_version(team_id, use_cache=False)})


@app.get("/api/sync-version")
def sync_version():
    team_id = _resolve_team_id()
    v = f"{_team_tasks_version(team_id, use_cache=False)}:{_global_teams_version(use_cache=False)}"
    return jsonify({"team": team_id, "sync_version": v})


@app.get("/admin/login")
def admin_login_page():
    html = """
    <!doctype html><html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>Admin Login</title>
    <style>
      *{box-sizing:border-box}
      body{font-family:"Trebuchet MS","Segoe UI",sans-serif;margin:0;display:grid;place-items:center;min-height:100vh;padding:18px;background:radial-gradient(circle at top left,#fff4df 0,#f4ede3 28%,#e7f1ef 62%,#dde8f5 100%);color:#182133}
      .shell{width:min(980px,100%);display:grid;grid-template-columns:minmax(0,1.1fr) minmax(320px,.9fr);border-radius:28px;overflow:hidden;border:1px solid rgba(255,255,255,.62);background:rgba(255,255,255,.9);box-shadow:0 28px 60px rgba(24,33,51,.14);backdrop-filter:blur(12px)}
      .hero{padding:34px;background:linear-gradient(145deg,#17324a,#22566a);color:#f7fbff}
      .eyebrow{font-size:12px;font-weight:700;letter-spacing:.16em;text-transform:uppercase;opacity:.72}
      .hero h1{margin:14px 0 0;font-size:clamp(30px,4vw,44px);line-height:1.04}
      .hero p{margin:14px 0 0;font-size:15px;line-height:1.6;color:rgba(247,251,255,.78)}
      .hero .mini{display:inline-flex;margin-top:18px;padding:10px 14px;border-radius:999px;border:1px solid rgba(255,255,255,.18);background:rgba(255,255,255,.08);font-size:12px;font-weight:700}
      .card{padding:30px;display:flex;flex-direction:column;justify-content:center}
      .top{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:14px}
      .top h2{margin:0;font-size:26px}
      .top a{text-decoration:none;padding:10px 14px;border-radius:12px;border:1px solid #d6dfeb;background:#f7fafc;color:#182133;font-size:13px;font-weight:700}
      .muted{color:#607086;font-size:14px;line-height:1.6;margin:0 0 16px}
      label{display:block;margin-bottom:8px;font-size:12px;font-weight:700;color:#607086;letter-spacing:.06em;text-transform:uppercase}
      input{width:100%;padding:13px 14px;border-radius:14px;border:1px solid #d6dfeb;background:#fff;font-size:15px;color:#182133;outline:none}
      input:focus{border-color:#86cfc5;box-shadow:0 0 0 4px rgba(15,118,110,.12)}
      button{margin-top:14px;width:100%;padding:13px 16px;border:none;border-radius:14px;background:linear-gradient(135deg,#0f766e,#115e59);color:#fff;font-size:15px;font-weight:700;cursor:pointer;box-shadow:0 18px 32px rgba(15,118,110,.24)}
      #err{min-height:22px;margin-top:12px;color:#b42318;font-size:13px}
      @media (max-width:860px){.shell{grid-template-columns:1fr}.hero,.card{padding:22px}}
    </style>
    </head><body><div class="shell"><section class="hero"><div class="eyebrow">Admin Access</div><h1>Team control center</h1><p>Review team activity, search articles, adjust task estimates, and export reports from one cleaner admin space.</p><div class="mini">Secure password required</div></section><section class="card"><div class="top"><h2>Admin Login</h2><a href="{{ url_for('home') }}">Back Home</a></div><p class="muted">Use the admin password to unlock reports, task settings, and efficiency views.</p><label for="pw">Password</label><input id="pw" type="password" placeholder="Enter password" autocomplete="current-password"><button id="btn">Login</button><div id="err"></div></section></div><script>
      async function login(){
        const pw=document.getElementById("pw").value;
        const res=await fetch("/admin/login",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({password:pw})});
        const data=await res.json().catch(()=>null);
        if(!res.ok){document.getElementById("err").textContent=data?.error||"Login failed";return;}
        window.location.href="/admin/report";
      }
      document.getElementById("btn").addEventListener("click", login);
      document.getElementById("pw").addEventListener("keydown", (event)=>{ if(event.key==="Enter") login(); });
    </script></body></html>
    """
    return render_template_string(html)


@app.post("/admin/login")
def admin_login_post():
    key = _admin_login_client_key()
    now = time.time()
    attempts = _admin_login_attempts.get(key, [])
    attempts = [t for t in attempts if (now - t) <= _ADMIN_LOGIN_WINDOW_SECONDS]
    if len(attempts) >= _ADMIN_LOGIN_MAX_ATTEMPTS:
        wait_s = int(max(1.0, _ADMIN_LOGIN_WINDOW_SECONDS - (now - min(attempts))))
        _admin_login_attempts[key] = attempts
        return jsonify({"error": f"Too many attempts. Try again in {wait_s} seconds."}), 429

    payload = request.get_json(silent=True) or {}
    password = (payload.get("password") or "").strip()
    expected_password = _admin_password()
    if not expected_password:
        return jsonify({"error": "Admin is not configured. Set ADMIN_PASSWORD in environment variables."}), 503
    if password != expected_password:
        attempts.append(now)
        _admin_login_attempts[key] = attempts
        return jsonify({"error": "Invalid password."}), 401

    _admin_login_attempts.pop(key, None)
    _grant_admin_session()
    return jsonify({"ok": True})


@app.post("/admin/logout")
def admin_logout():
    _clear_admin_session()
    return jsonify({"ok": True})


@app.post("/admin/reset-login")
def admin_reset_login():
    _require_admin()
    payload = request.get_json(silent=True) or {}
    team_id = _resolve_team_id(payload)
    member = (payload.get("member") or "").strip()
    entry_date = (payload.get("date") or "").strip()
    time_hhmm = (payload.get("time") or "").strip()

    if not _valid_member(member, team_id):
        return jsonify({"error": "Select a valid member."}), 400
    try:
        entry_date = _parse_date(entry_date)
    except Exception:
        return jsonify({"error": "Invalid date. Use YYYY-MM-DD."}), 400

    try:
        login_at = datetime.combine(
            datetime.strptime(entry_date, "%Y-%m-%d").date(),
            datetime.strptime(time_hhmm, "%H:%M").time(),
        )
    except Exception:
        return jsonify({"error": "Invalid time. Use HH:MM."}), 400

    with _db() as conn:
        existing = _fetchone(conn, "SELECT id FROM member_logins WHERE team_id=%s AND member=%s AND entry_date=%s LIMIT 1", (team_id, member, entry_date))
        if existing:
            _execute(
                conn,
                "UPDATE member_logins SET login_at=%s WHERE team_id=%s AND member=%s AND entry_date=%s",
                (login_at, team_id, member, entry_date),
            )
        else:
            _execute(
                conn,
                "INSERT INTO member_logins(team_id, entry_date, member, login_at, logout_at) VALUES(%s,%s,%s,%s,NULL)",
                (team_id, entry_date, member, login_at),
            )

    return jsonify({"ok": True})


@app.post("/admin/reset-logout")
def admin_reset_logout():
    _require_admin()
    payload = request.get_json(silent=True) or {}
    team_id = _resolve_team_id(payload)
    member = (payload.get("member") or "").strip()
    entry_date = (payload.get("date") or "").strip()

    if not _valid_member(member, team_id):
        return jsonify({"error": "Select a valid member."}), 400
    try:
        entry_date = _parse_date(entry_date)
    except Exception:
        return jsonify({"error": "Invalid date. Use YYYY-MM-DD."}), 400

    with _db() as conn:
        _execute(conn, "UPDATE member_logins SET logout_at=NULL WHERE team_id=%s AND member=%s AND entry_date=%s", (team_id, member, entry_date))

    return jsonify({"ok": True})


@app.get("/admin/search")
def admin_search():
    _require_admin()
    team_id = _admin_team_id()
    teams = _all_teams()
    query = (request.args.get("article") or "").strip()

    rows: list[dict] = []
    if query:
        with _db() as conn:
            combos = _fetchall(
                conn,
                """
                SELECT DISTINCT entry_date::text AS entry_date, member
                FROM entries
                WHERE team_id=%s AND article ILIKE %s
                ORDER BY entry_date DESC, member
                """,
                (team_id, f"%{query}%"),
            )

        now_dt = datetime.now()
        for c in combos:
            member = str(c["member"])
            entry_date = str(c["entry_date"])
            data = _build_member_day_entries(member, entry_date, team_id)

            break_seconds = 0
            for p in data["pauses"]:
                s = _dt_for_math(p.get("start"))
                if not s:
                    continue
                if p.get("end"):
                    e = _dt_for_math(p.get("end"))
                    if not e:
                        continue
                else:
                    e = now_dt if entry_date == _today_str() else s
                if e > s:
                    break_seconds += int((e - s).total_seconds())
            break_min = float(break_seconds) / 60.0

            for e in data["entries"]:
                if query.lower() not in str(e["article"]).lower():
                    continue
                rows.append(
                    {
                        "entry_date": entry_date,
                        "first_at_local": e["created_at_local"],
                        "last_at_local": e.get("last_at_local") or e["created_at_local"],
                        "member": member,
                        "article": e["article"],
                        "completed": bool(e["completed"]),
                        "task_labels": [_task_label(str(t), team_id) for t in e["tasks"]],
                        "spent_label": e["spent_total_label"],
                        "break_label": _minutes_label(break_min),
                    }
                )

    html = """
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Admin Search</title>
        <style>
          * { box-sizing: border-box; }
          body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0; padding: 28px 16px; color: #0f172a; background: radial-gradient(circle at 0% 0%, #eef4ff 0, #f8fbff 30%, #f6f8fc 100%); }
          .wrap { max-width: 1260px; margin: 0 auto; }
          .top { display:flex; justify-content: space-between; align-items: center; gap:10px; flex-wrap: wrap; }
          .card { border: 1px solid #dbe3ee; border-radius: 14px; padding: 16px; margin-top: 12px; background: #fff; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05); }
          .muted { color: #64748b; font-size: 12px; }
          form { display: grid; grid-template-columns: 1fr auto; gap: 10px; }
          input[type=text] { width: 100%; padding: 10px 12px; border: 1px solid #cbd5e1; border-radius: 10px; font-size: 14px; outline: none; }
          button { padding: 10px 12px; border: none; border-radius: 10px; cursor: pointer; background: #2563eb; color: #fff; font-size: 14px; font-weight: 600; }
          a { background: #e2e8f0; color: #0f172a; text-decoration: none; border-radius: 10px; padding: 8px 10px; font-weight: 600; border: 1px solid #cbd5e1; display: inline-block; }
          table { width: 100%; border-collapse: collapse; margin-top: 12px; }
          th, td { text-align: left; border-bottom: 1px solid #edf2f7; padding: 8px 6px; font-size: 13px; vertical-align: top; }
          th { font-size: 12px; color: #334155; background: #f8fafc; }
          tr:nth-child(even) td { background: #fcfdff; }
          .badge { display: inline-block; margin: 2px 4px 2px 0; font-size: 12px; color: #1e3a8a; background: #dbeafe; padding: 2px 8px; border-radius: 999px; white-space: nowrap; font-weight: 600; }
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="top">
            <div>
              <h1 style="font-size: 20px; margin: 0;">Admin Search</h1>
              <div class="muted">Search by article and view all member entries, tasks performed, time and break.</div>
            </div>
            <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap;">
              <select id="teamSel">{% for t in teams %}<option value="{{ t.id }}" {% if t.id == team_id %}selected{% endif %}>{{ t.name }}</option>{% endfor %}</select>
              <a href="{{ url_for('admin_report') }}?team={{ team_id }}">Back To Admin Report</a>
              <a href="{{ url_for('admin_tasks') }}?team={{ team_id }}">Tasks &amp; Estimation</a>
              <a href="{{ url_for('admin_efficiency') }}?team={{ team_id }}">Member Efficiency</a>
              <a href="{{ url_for('home') }}?team={{ team_id }}">Home</a>
              <button type="button" id="logoutBtn" class="secondary" style="background:#e2e8f0;color:#0f172a;">Logout</button>
            </div>
          </div>

          <div class="card">
            <form method="GET" action="{{ url_for('admin_search') }}">
              <input type="hidden" name="team" value="{{ team_id }}" />
              <input type="text" name="article" value="{{ query }}" placeholder="Enter article number (full or partial)" />
              <button type="submit">Search</button>
            </form>
            <div class="muted" style="margin-top: 8px;">
              {% if query %}
                Search: <strong>{{ query }}</strong> | Results: <strong>{{ rows|length }}</strong>
              {% else %}
                Enter an article number to search.
              {% endif %}
            </div>
          </div>

          {% if query %}
            <div class="card">
              {% if rows|length == 0 %}
                <div class="muted">No entries found.</div>
              {% else %}
                <table>
                  <thead>
                    <tr>
                      <th>Date</th>
                      <th>First</th>
                      <th>Last</th>
                      <th>Member</th>
                      <th>Article</th>
                      <th>Status</th>
                      <th>Tasks</th>
                      <th>Spent</th>
                      <th>Break</th>
                    </tr>
                  </thead>
                  <tbody>
                    {% for r in rows %}
                      <tr>
                        <td>{{ r.entry_date }}</td>
                        <td>{{ r.first_at_local }}</td>
                        <td>{{ r.last_at_local }}</td>
                        <td>{{ r.member }}</td>
                        <td>{{ r.article }}</td>
                        <td>{{ "Completed" if r.completed else "In Progress" }}</td>
                        <td>
                          {% for t in r.task_labels %}
                            <span class="badge">{{ t }}</span>
                          {% endfor %}
                        </td>
                        <td>{{ r.spent_label }}</td>
                        <td>{{ r.break_label }}</td>
                      </tr>
                    {% endfor %}
                  </tbody>
                </table>
              {% endif %}
            </div>
          {% endif %}
        </div>
        <script>
          document.getElementById("teamSel").addEventListener("change", () => {
            const url = new URL(window.location.href);
            url.searchParams.set("team", document.getElementById("teamSel").value);
            window.location.href = url.toString();
          });
          document.getElementById("logoutBtn").addEventListener("click", async () => {
            await fetch("/admin/logout", { method: "POST" });
            window.location.href = "/admin/login";
          });
        </script>
      </body>
    </html>
    """
    return render_template_string(html, team_id=team_id, teams=teams, query=query, rows=rows)


@app.get("/admin/tasks")
def admin_tasks():
    _require_admin()
    team_id = _admin_team_id()
    teams = _all_teams()
    selected_name = next((t["name"] for t in teams if t["id"] == team_id), DEFAULT_TEAM_NAME)
    with _db() as conn:
        all_categories = [str(r["name"]) for r in _fetchall(conn, "SELECT name FROM task_categories ORDER BY name")]
        tasks = _fetchall(conn, "SELECT id, label, estimate, category, active FROM tasks WHERE team_id=%s ORDER BY category, label", (team_id,))

    prefix = _team_prefix(team_id)
    if team_id == DEFAULT_TEAM_ID:
        categories = [_team_unscoped_value(c, DEFAULT_TEAM_ID) for c in all_categories if ("::" not in c or c.startswith(prefix))]
    else:
        categories = [_team_unscoped_value(c, team_id) for c in all_categories if c.startswith(prefix)]

    tasks_view = []
    for t in tasks:
        tid = str(t["id"])
        cat = str(t["category"])
        tasks_view.append(
            {
                "id": tid,
                "display_id": _team_unscoped_value(tid, team_id),
                "label": str(t["label"]),
                "estimate": int(t["estimate"]),
                "category": cat,
                "display_category": _team_unscoped_value(cat, team_id),
                "active": bool(t["active"]),
            }
        )

    html = """
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Tasks & Estimation - {{ selected_name }}</title>
        <style>
          * { box-sizing: border-box; }
          body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0; padding: 28px 16px; color: #0f172a; background: radial-gradient(circle at 0% 0%, #eef4ff 0, #f8fbff 30%, #f6f8fc 100%); }
          .wrap { max-width: 1260px; margin: 0 auto; }
          .top { display:flex; justify-content: space-between; align-items: center; gap:10px; flex-wrap: wrap; }
          .muted { color: #64748b; font-size: 12px; }
          .card { border: 1px solid #dbe3ee; border-radius: 14px; padding: 16px; margin-top: 12px; background: #fff; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05); }
          input, select { padding: 9px 10px; border: 1px solid #cbd5e1; border-radius: 10px; font-size: 14px; outline: none; background: #fff; }
          button { padding: 10px 12px; border: none; border-radius: 10px; cursor: pointer; background: #2563eb; color: #fff; font-size: 14px; font-weight: 600; }
          a { background: #e2e8f0; color: #0f172a; text-decoration: none; border-radius: 10px; padding: 8px 10px; font-weight: 600; border: 1px solid #cbd5e1; display: inline-block; }
          table { width: 100%; border-collapse: collapse; margin-top: 12px; }
          th, td { text-align: left; border-bottom: 1px solid #edf2f7; padding: 8px 6px; font-size: 13px; vertical-align: top; }
          th { font-size: 12px; color: #334155; background: #f8fafc; }
          tr:nth-child(even) td { background: #fcfdff; }
          .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
          .row { display: grid; grid-template-columns: 160px 1fr; gap: 10px; align-items: center; margin-top: 10px; }
          .row label { font-size: 12px; color: #334155; font-weight: 700; }
          .inline { display:flex; gap: 10px; align-items: center; flex-wrap: wrap; }
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="top">
            <div>
              <h1 style="font-size: 20px; margin: 0;">Tasks & Estimation</h1>
              <div class="muted">Admin can create categories, create tasks, and update estimations (minutes).</div>
            </div>
            <div class="inline">
              <select id="teamSel">{% for t in teams %}<option value="{{ t.id }}" {% if t.id == team_id %}selected{% endif %}>{{ t.name }}</option>{% endfor %}</select>
              <a href="{{ url_for('admin_report') }}?team={{ team_id }}">Back To Admin Report</a>
              <a href="{{ url_for('admin_search') }}?team={{ team_id }}">Admin Search</a>
              <a href="{{ url_for('admin_efficiency') }}?team={{ team_id }}">Member Efficiency</a>
              <a href="{{ url_for('home') }}?team={{ team_id }}">Home</a>
              <a href="#" id="logoutBtn">Logout</a>
            </div>
          </div>

          <div class="card">
            <h2 style="margin:0 0 8px; font-size: 16px;">Manage Categories</h2>
            <div class="muted">Deleting a category will permanently remove category and tasks for this team. Old saved entries stay unchanged.</div>
            <div class="inline" style="margin-top: 10px; flex-wrap: wrap;">
              {% for c in categories %}
                <form method="POST" action="/admin/categories/delete" class="inline delete-category-form" style="margin: 0;">
                  <input type="hidden" name="team" value="{{ team_id }}" />
                  <input type="hidden" name="name" value="{{ c }}" />
                  <span class="muted" style="font-weight:700;">{{ c }}</span>
                  <button type="submit" style="background:#ef4444;">Delete</button>
                </form>
              {% endfor %}
            </div>
          </div>

          <div class="card">
            <h2 style="margin:0 0 8px; font-size: 16px;">Add Category</h2>
            <form method="POST" action="/admin/categories/add" class="inline">
              <input type="hidden" name="team" value="{{ team_id }}" />
              <input type="text" name="name" placeholder="Category name" required />
              <button type="submit">Add</button>
            </form>
          </div>

          <div class="card">
            <h2 style="margin:0 0 8px; font-size: 16px;">Add Task</h2>
            <form method="POST" action="/admin/tasks/add">
              <input type="hidden" name="team" value="{{ team_id }}" />
              <div class="grid">
                <div class="row"><label>Task ID</label><input type="text" name="id" placeholder="example: new_task_id" required /></div>
                <div class="row"><label>Label</label><input type="text" name="label" placeholder="Task display name" required /></div>
                <div class="row"><label>Estimate (min)</label><input type="number" min="0" name="estimate" value="0" required /></div>
                <div class="row">
                  <label>Category</label>
                  <div class="inline">
                    <select name="category">{% for c in categories %}<option value="{{ c }}">{{ c }}</option>{% endfor %}</select>
                    <span class="muted">or</span>
                    <input type="text" name="new_category" placeholder="New category" />
                  </div>
                </div>
              </div>
              <div style="margin-top:12px;"><button type="submit">Create Task</button></div>
            </form>
          </div>

          <div class="card">
            <h2 style="margin:0 0 8px; font-size: 16px;">Existing Tasks</h2>
            <div class="muted">Edit label/estimate/category/active and save. Bulk actions work with the checkboxes.</div>
            <div class="inline" style="margin-top: 10px;">
              <button type="button" id="bulkAll" style="background:#0f766e;">Select all</button>
              <button type="button" id="bulkNone" style="background:#64748b;">Select none</button>
              <select id="bulkAction">
                <option value="activate">Set Active</option>
                <option value="deactivate">Set Inactive</option>
                <option value="delete">Delete Permanently</option>
              </select>
              <button type="button" id="bulkApply" style="background:#0f766e;">Apply</button>
              <span class="muted" id="bulkMsg"></span>
            </div>
            <table>
              <thead>
                <tr>
                  <th style="width:36px;"><input type="checkbox" id="bulkHead" /></th>
                  <th>Task ID</th>
                  <th>Label</th>
                  <th>Estimate (min)</th>
                  <th>Category</th>
                  <th>Active</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {% for t in tasks %}
                  <tr>
                    <form method="POST" action="/admin/tasks/update">
                      <td><input type="checkbox" class="bulkBox" data-id="{{ t.id }}" /></td>
                      <td>{{ t.display_id }}<input type="hidden" name="id" value="{{ t.id }}" /><input type="hidden" name="team" value="{{ team_id }}" /></td>
                      <td><input type="text" name="label" value="{{ t.label }}" required /></td>
                      <td><input type="number" min="0" name="estimate" value="{{ t.estimate }}" required /></td>
                      <td><select name="category">{% for c in categories %}<option value="{{ c }}" {% if c == t.display_category %}selected{% endif %}>{{ c }}</option>{% endfor %}</select></td>
                      <td style="width:110px;"><select name="active"><option value="1" {% if t.active %}selected{% endif %}>Yes</option><option value="0" {% if not t.active %}selected{% endif %}>No</option></select></td>
                      <td style="width:220px;display:flex;gap:8px;">
                        <button type="submit">Save</button>
                        <button type="button" class="delete-task-btn" data-id="{{ t.id }}" style="background:#ef4444;">Delete</button>
                      </td>
                    </form>
                  </tr>
                {% endfor %}
              </tbody>
            </table>
          </div>
        </div>
        <script>
          document.getElementById("teamSel").addEventListener("change", () => {
            const url = new URL(window.location.href);
            url.searchParams.set("team", document.getElementById("teamSel").value);
            window.location.href = url.toString();
          });

          document.getElementById("logoutBtn").addEventListener("click", async (e) => {
            e.preventDefault();
            await fetch("/admin/logout", { method: "POST" });
            window.location.href = "/admin/login";
          });

          function selectedIds(){
            return Array.from(document.querySelectorAll(".bulkBox")).filter(b=>b.checked).map(b=>b.dataset.id);
          }
          function setAll(val){
            Array.from(document.querySelectorAll(".bulkBox")).forEach(b=>b.checked=val);
            document.getElementById("bulkHead").checked = val;
          }
          document.getElementById("bulkAll").addEventListener("click", ()=>setAll(true));
          document.getElementById("bulkNone").addEventListener("click", ()=>setAll(false));
          document.getElementById("bulkHead").addEventListener("change", (e)=>setAll(!!e.target.checked));
          document.getElementById("bulkApply").addEventListener("click", async ()=>{
            const ids = selectedIds();
            const action = document.getElementById("bulkAction").value;
            const msg = document.getElementById("bulkMsg");
            if(!ids.length){ msg.textContent = "Select at least one task."; return; }
            if(action === "delete" && !confirm(`Delete ${ids.length} selected task(s) permanently for this team?`)){ return; }
            msg.textContent = "Applying...";
            const res = await fetch("/admin/tasks/bulk", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ team: "{{ team_id }}", action, ids }) });
            if(!res.ok){
              const data = await res.json().catch(()=>null);
              msg.textContent = data?.error || "Failed.";
              return;
            }
            window.location.reload();
          });
          document.querySelectorAll(".delete-category-form").forEach((form) => {
            form.addEventListener("submit", (e) => {
              const name = form.querySelector("input[name='name']")?.value || "this category";
              if(!confirm(`Delete category "${name}" and all tasks in it for this team?`)){
                e.preventDefault();
              }
            });
          });
          document.querySelectorAll(".delete-task-btn").forEach((btn) => {
            btn.addEventListener("click", async () => {
              const id = btn.dataset.id || "this task";
              if(!confirm(`Delete task "${id}" permanently for this team?`)){ return; }
              const form = document.createElement("form");
              form.method = "POST";
              form.action = "/admin/tasks/delete";
              form.style.display = "none";
              const team = document.createElement("input");
              team.type = "hidden";
              team.name = "team";
              team.value = "{{ team_id }}";
              const tid = document.createElement("input");
              tid.type = "hidden";
              tid.name = "id";
              tid.value = id;
              form.appendChild(team);
              form.appendChild(tid);
              document.body.appendChild(form);
              form.submit();
            });
          });
        </script>
      </body>
    </html>
    """
    return render_template_string(html, team_id=team_id, teams=teams, selected_name=selected_name, categories=categories, tasks=tasks_view)


@app.post("/admin/categories/add")
def admin_add_category():
    _require_admin()
    team_id = _resolve_team_id({"team": request.form.get("team")})
    name = (request.form.get("name") or "").strip()
    if not name:
        return redirect(url_for("admin_tasks", team=team_id))
    name_store = _team_scoped_value(name, team_id)
    try:
        with _db() as conn:
            _execute(conn, _sql("INSERT INTO task_categories(name) VALUES(%s) ON CONFLICT DO NOTHING", "INSERT OR IGNORE INTO task_categories(name) VALUES(?)"), (name_store,))
        _bump_team_tasks_version(team_id)
    except Exception:
        app.logger.exception("admin_add_category_failed")
    return redirect(url_for("admin_tasks", team=team_id))


@app.post("/admin/categories/delete")
def admin_delete_category():
    _require_admin()
    team_id = _resolve_team_id({"team": request.form.get("team")})
    name = (request.form.get("name") or "").strip()
    if not name:
        return redirect(url_for("admin_tasks", team=team_id))
    category_store = _team_scoped_value(name, team_id)
    try:
        with _db() as conn:
            _execute(conn, "DELETE FROM tasks WHERE team_id=%s AND category=%s", (team_id, category_store))
            used = _fetchone(conn, "SELECT COUNT(1) AS c FROM tasks WHERE category=%s", (category_store,))
            if not used or int(used["c"]) == 0:
                _execute(conn, "DELETE FROM task_categories WHERE name=%s", (category_store,))
        _task_cache.clear()
        _bump_team_tasks_version(team_id)
    except Exception:
        app.logger.exception("admin_delete_category_failed")
    return redirect(url_for("admin_tasks", team=team_id))


@app.post("/admin/tasks/add")
def admin_add_task():
    _require_admin()
    team_id = _resolve_team_id({"team": request.form.get("team")})
    tid = (request.form.get("id") or "").strip()
    label = (request.form.get("label") or "").strip()
    estimate_raw = (request.form.get("estimate") or "0").strip()
    category = (request.form.get("new_category") or "").strip() or (request.form.get("category") or "").strip()

    if not tid or not label or not category:
        return redirect(url_for("admin_tasks", team=team_id))
    if not all(ch.isalnum() or ch == "_" for ch in tid):
        return redirect(url_for("admin_tasks", team=team_id))
    try:
        estimate = int(estimate_raw)
    except Exception:
        estimate = 0
    if estimate < 0:
        estimate = 0

    tid_store = _team_scoped_value(tid, team_id)
    category_store = _team_scoped_value(category, team_id)
    try:
        with _db() as conn:
            _execute(conn, _sql("INSERT INTO task_categories(name) VALUES(%s) ON CONFLICT DO NOTHING", "INSERT OR IGNORE INTO task_categories(name) VALUES(?)"), (category_store,))
            if _use_postgres():
                _execute(
                    conn,
                    """
                    INSERT INTO tasks(id, team_id, label, estimate, category, active)
                    VALUES(%s,%s,%s,%s,%s,TRUE)
                    ON CONFLICT (id)
                    DO UPDATE SET team_id=EXCLUDED.team_id, label=EXCLUDED.label, estimate=EXCLUDED.estimate, category=EXCLUDED.category, active=TRUE
                    """,
                    (tid_store, team_id, label, estimate, category_store),
                )
            else:
                _execute(
                    conn,
                    "INSERT OR REPLACE INTO tasks(id, team_id, label, estimate, category, active) VALUES(?,?,?,?,?,1)",
                    (tid_store, team_id, label, estimate, category_store),
                )
        _bump_team_tasks_version(team_id)
    except Exception:
        app.logger.exception("admin_add_task_failed")
    return redirect(url_for("admin_tasks", team=team_id))


@app.post("/admin/tasks/update")
def admin_update_task():
    _require_admin()
    team_id = _resolve_team_id({"team": request.form.get("team")})
    tid = (request.form.get("id") or "").strip()
    label = (request.form.get("label") or "").strip()
    estimate_raw = (request.form.get("estimate") or "0").strip()
    category = (request.form.get("category") or "").strip()
    active_raw = (request.form.get("active") or "1").strip()

    if not tid or not label or not category:
        return redirect(url_for("admin_tasks", team=team_id))
    try:
        estimate = int(estimate_raw)
    except Exception:
        estimate = 0
    if estimate < 0:
        estimate = 0
    active = active_raw == "1"

    category_store = _team_scoped_value(category, team_id)
    try:
        with _db() as conn:
            _execute(conn, _sql("INSERT INTO task_categories(name) VALUES(%s) ON CONFLICT DO NOTHING", "INSERT OR IGNORE INTO task_categories(name) VALUES(?)"), (category_store,))
            active_val = active if _use_postgres() else (1 if active else 0)
            _execute(conn, "UPDATE tasks SET label=%s, estimate=%s, category=%s, active=%s WHERE team_id=%s AND id=%s", (label, estimate, category_store, active_val, team_id, tid))
        _task_cache.clear()
        _bump_team_tasks_version(team_id)
    except Exception:
        app.logger.exception("admin_update_task_failed")
    return redirect(url_for("admin_tasks", team=team_id))


@app.post("/admin/tasks/bulk")
def admin_tasks_bulk():
    _require_admin()
    payload = request.get_json(silent=True) or {}
    team_id = _resolve_team_id(payload)
    action = str(payload.get("action") or "").strip().lower()
    ids = payload.get("ids") or []
    if not isinstance(ids, list) or not ids:
        return jsonify({"error": "No tasks selected."}), 400
    task_ids = [str(x) for x in ids if isinstance(x, str) and x.strip()]
    if not task_ids:
        return jsonify({"error": "No valid task ids."}), 400

    if action not in {"activate", "deactivate", "delete"}:
        return jsonify({"error": "Invalid action."}), 400
    try:
        with _db() as conn:
            if action == "delete":
                for tid in task_ids:
                    _execute(conn, "DELETE FROM tasks WHERE team_id=%s AND id=%s", (team_id, tid))
                cats = _fetchall(conn, "SELECT name FROM task_categories")
                for c in cats:
                    name = str(c["name"])
                    left = _fetchone(conn, "SELECT COUNT(1) AS c FROM tasks WHERE category=%s", (name,))
                    if not left or int(left["c"]) == 0:
                        _execute(conn, "DELETE FROM task_categories WHERE name=%s", (name,))
            else:
                active = True if action == "activate" else False
                active_val = active if _use_postgres() else (1 if active else 0)
                for tid in task_ids:
                    _execute(conn, "UPDATE tasks SET active=%s WHERE team_id=%s AND id=%s", (active_val, team_id, tid))
        _task_cache.clear()
        _bump_team_tasks_version(team_id)
        return jsonify({"ok": True})
    except Exception:
        app.logger.exception("admin_tasks_bulk_failed")
        return jsonify({"error": "Bulk operation failed. No changes were committed."}), 500


@app.post("/admin/tasks/delete")
def admin_delete_task():
    _require_admin()
    team_id = _resolve_team_id({"team": request.form.get("team")})
    tid = (request.form.get("id") or "").strip()
    if not tid:
        return redirect(url_for("admin_tasks", team=team_id))
    try:
        with _db() as conn:
            row = _fetchone(conn, "SELECT category FROM tasks WHERE team_id=%s AND id=%s", (team_id, tid))
            _execute(conn, "DELETE FROM tasks WHERE team_id=%s AND id=%s", (team_id, tid))
            if row and row.get("category"):
                cat = str(row["category"])
                used = _fetchone(conn, "SELECT COUNT(1) AS c FROM tasks WHERE category=%s", (cat,))
                if not used or int(used["c"]) == 0:
                    _execute(conn, "DELETE FROM task_categories WHERE name=%s", (cat,))
        _task_cache.clear()
        _bump_team_tasks_version(team_id)
    except Exception:
        app.logger.exception("admin_delete_task_failed")
    return redirect(url_for("admin_tasks", team=team_id))


@app.get("/admin/teams")
def admin_teams():
    _require_admin()
    team_id = _admin_team_id()
    teams = _all_teams()
    selected_name = next((t["name"] for t in teams if t["id"] == team_id), DEFAULT_TEAM_NAME)
    members = _team_members(team_id)
    html = """
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Teams</title>
        <style>
          * { box-sizing: border-box; }
          body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0; padding: 24px 14px; color: #0f172a; background: radial-gradient(circle at 0% 0%, #eef4ff 0, #f8fbff 30%, #f6f8fc 100%); }
          .wrap { max-width: 1100px; margin: 0 auto; }
          .top { display:flex; justify-content: space-between; align-items: center; gap:10px; flex-wrap: wrap; }
          .card { border: 1px solid #dbe3ee; border-radius: 16px; padding: 16px; margin-top: 12px; background: #fff; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05); }
          .muted { color: #64748b; font-size: 12px; line-height: 1.55; }
          .row { display:flex; gap: 10px; flex-wrap: wrap; align-items: center; }
          input, select { padding: 10px 12px; border: 1px solid #cbd5e1; border-radius: 12px; font-size: 14px; outline: none; background: #fff; }
          button { padding: 10px 12px; border: none; border-radius: 12px; cursor: pointer; background: linear-gradient(135deg, #0f766e, #115e59); color: #fff; font-size: 14px; font-weight: 800; }
          a { background: #e2e8f0; color: #0f172a; text-decoration: none; border-radius: 12px; padding: 10px 12px; font-weight: 700; border: 1px solid #cbd5e1; display: inline-block; }
          table { width: 100%; border-collapse: collapse; margin-top: 10px; }
          th, td { text-align: left; border-bottom: 1px solid #edf2f7; padding: 8px 6px; font-size: 13px; vertical-align: top; }
          th { font-size: 12px; color: #334155; background: #f8fafc; }
          .pill { display:inline-flex; gap:8px; align-items:center; padding: 8px 12px; border-radius: 999px; border: 1px solid #e2e8f0; background: #fbfdff; font-weight: 800; font-size: 12px; }
          .danger { background: #fee2e2; border-color: #fecaca; color: #7f1d1d; }
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="top">
            <div>
              <h1 style="font-size: 20px; margin: 0;">Teams</h1>
              <div class="muted">Create teams and manage members. Tasks, exports and reports are scoped by team.</div>
            </div>
            <div class="row">
              <a href="{{ url_for('admin_report') }}?team={{ team_id }}">Back To Admin Report</a>
              <a href="{{ url_for('home') }}?team={{ team_id }}">Home</a>
            </div>
          </div>

          <div class="card">
            <div class="row" style="justify-content: space-between;">
              <div class="pill">Selected Team: {{ selected_name }} ({{ team_id }})</div>
              <form class="row" method="GET" action="{{ url_for('admin_teams') }}">
                <select name="team">
                  {% for t in teams %}
                    <option value="{{ t.id }}" {% if t.id == team_id %}selected{% endif %}>{{ t.name }}</option>
                  {% endfor %}
                </select>
                <button type="submit">Switch</button>
              </form>
            </div>
          </div>

          <div class="card">
            <h2 style="margin:0 0 8px; font-size: 16px;">All Teams</h2>
            <div class="muted">Rename any team here. Team ID stays the same.</div>
            <table>
              <thead><tr><th>Team ID</th><th>Team Name</th><th></th></tr></thead>
              <tbody>
                {% for t in teams %}
                  <tr>
                    <td style="width:220px;">{{ t.id }}</td>
                    <td>
                      <form method="POST" action="{{ url_for('admin_team_rename') }}" class="row" style="margin:0;">
                        <input type="hidden" name="team" value="{{ t.id }}" />
                        <input type="text" name="name" value="{{ t.name }}" required />
                        <button type="submit">Save</button>
                      </form>
                    </td>
                    <td style="width:140px;">
                      <a href="{{ url_for('admin_teams') }}?team={{ t.id }}">Open</a>
                    </td>
                  </tr>
                {% endfor %}
              </tbody>
            </table>
          </div>

          <div class="card">
            <h2 style="margin:0 0 8px; font-size: 16px;">Create Team</h2>
            <form method="POST" action="{{ url_for('admin_team_create') }}" class="row">
              <input type="text" name="name" placeholder="Team name (e.g. Team A)" required />
              <button type="submit">Create</button>
            </form>
            <div class="muted" style="margin-top: 10px;">New teams start with a copy of Default team tasks.</div>
          </div>

          <div class="card">
            <h2 style="margin:0 0 8px; font-size: 16px;">Members ({{ members|length }})</h2>
            <form method="POST" action="{{ url_for('admin_team_member_add') }}" class="row">
              <input type="hidden" name="team" value="{{ team_id }}" />
              <input type="text" name="member" placeholder="Member full name" required />
              <button type="submit">Add Member</button>
            </form>
            <table>
              <thead><tr><th>Member</th><th></th></tr></thead>
              <tbody>
                {% for m in members %}
                  <tr>
                    <td>{{ m }}</td>
                    <td style="width:140px;">
                      <form method="POST" action="{{ url_for('admin_team_member_remove') }}">
                        <input type="hidden" name="team" value="{{ team_id }}" />
                        <input type="hidden" name="member" value="{{ m }}" />
                        <button class="danger" type="submit" style="background:#ef4444;">Remove</button>
                      </form>
                    </td>
                  </tr>
                {% endfor %}
              </tbody>
            </table>
          </div>

          <div class="card">
            <h2 style="margin:0 0 8px; font-size: 16px;">Delete Team</h2>
            <div class="muted">Delete is blocked if the team has any saved data.</div>
            <form method="POST" action="{{ url_for('admin_team_delete') }}" class="row">
              <input type="hidden" name="team" value="{{ team_id }}" />
              <button type="submit" style="background:#ef4444;">Delete Selected Team</button>
            </form>
            {% if team_id == default_team_id %}
              <div class="muted" style="margin-top: 10px;">Default team cannot be deleted.</div>
            {% endif %}
          </div>
        </div>
      </body>
    </html>
    """
    return render_template_string(html, team_id=team_id, selected_name=selected_name, teams=teams, members=members, default_team_id=DEFAULT_TEAM_ID)


@app.post("/admin/teams/create")
def admin_team_create():
    _require_admin()
    name = (request.form.get("name") or "").strip()
    if not name:
        return redirect(url_for("admin_teams", team=DEFAULT_TEAM_ID))
    tid = _normalize_team_id(name)
    try:
        with _db() as conn:
            existing_name = _fetchone(conn, "SELECT id FROM teams WHERE name=%s LIMIT 1", (name,))
            if existing_name:
                return redirect(url_for("admin_teams", team=str(existing_name["id"])))
            existing = _fetchone(conn, "SELECT id FROM teams WHERE id=%s LIMIT 1", (tid,))
            if existing:
                tid = f"{tid}-{secrets.token_hex(2)}"
            try:
                _execute(conn, _sql("INSERT INTO teams(id, name) VALUES(%s,%s)", "INSERT INTO teams(id, name) VALUES(?,?)"), (tid, name))
            except Exception:
                existing_name2 = _fetchone(conn, "SELECT id FROM teams WHERE name=%s LIMIT 1", (name,))
                if existing_name2:
                    return redirect(url_for("admin_teams", team=str(existing_name2["id"])))
                return redirect(url_for("admin_teams", team=DEFAULT_TEAM_ID))
            rows = _fetchall(conn, "SELECT id, label, estimate, category, active FROM tasks WHERE team_id=%s", (DEFAULT_TEAM_ID,))
            for r in rows:
                raw_id = str(r["id"])
                new_id = _team_scoped_value(_team_unscoped_value(raw_id, DEFAULT_TEAM_ID), tid)
                cat_store = _team_scoped_value(_team_unscoped_value(str(r["category"]), DEFAULT_TEAM_ID), tid)
                _execute(conn, _sql("INSERT INTO task_categories(name) VALUES(%s) ON CONFLICT DO NOTHING", "INSERT OR IGNORE INTO task_categories(name) VALUES(?)"), (cat_store,))
                if _use_postgres():
                    _execute(
                        conn,
                        "INSERT INTO tasks(id, team_id, label, estimate, category, active) VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                        (new_id, tid, str(r["label"]), int(r["estimate"]), cat_store, bool(r["active"])),
                    )
                else:
                    _execute(
                        conn,
                        "INSERT OR IGNORE INTO tasks(id, team_id, label, estimate, category, active) VALUES(?,?,?,?,?,?)",
                        (new_id, tid, str(r["label"]), int(r["estimate"]), cat_store, int(r["active"])),
                    )
            _execute(
                conn,
                _sql(
                    "INSERT INTO app_state(key, value, updated_at) VALUES(%s,%s,NOW()) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()",
                    "INSERT INTO app_state(key, value, updated_at) VALUES(?,?,CURRENT_TIMESTAMP) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
                ),
                (f"tasks_version:{tid}", str(int(time.time() * 1000))),
            )
        _task_cache.clear()
        _bump_global_teams_version()
    except Exception:
        app.logger.exception("admin_team_create_failed")
    return redirect(url_for("admin_teams", team=tid))


@app.post("/admin/teams/rename")
def admin_team_rename():
    _require_admin()
    team_id = _resolve_team_id({"team": request.form.get("team")})
    name = (request.form.get("name") or "").strip()
    if not name:
        return redirect(url_for("admin_teams", team=team_id))
    with _db() as conn:
        try:
            _execute(conn, "UPDATE teams SET name=%s WHERE id=%s", (name, team_id))
        except Exception:
            return redirect(url_for("admin_teams", team=team_id))
    _bump_global_teams_version()
    return redirect(url_for("admin_teams", team=team_id))


@app.post("/admin/teams/members/add")
def admin_team_member_add():
    _require_admin()
    team_id = _resolve_team_id({"team": request.form.get("team")})
    member = (request.form.get("member") or "").strip()
    if not member:
        return redirect(url_for("admin_teams", team=team_id))
    try:
        with _db() as conn:
            if _use_postgres():
                _execute(
                    conn,
                    "INSERT INTO team_members(team_id, member, active) VALUES(%s,%s,TRUE) ON CONFLICT (team_id, member) DO UPDATE SET active=TRUE",
                    (team_id, member),
                )
            else:
                _execute(conn, "INSERT OR REPLACE INTO team_members(team_id, member, active) VALUES(?,?,1)", (team_id, member))
        _bump_team_tasks_version(team_id)
    except Exception:
        app.logger.exception("admin_team_member_add_failed")
    return redirect(url_for("admin_teams", team=team_id))


@app.post("/admin/teams/members/remove")
def admin_team_member_remove():
    _require_admin()
    team_id = _resolve_team_id({"team": request.form.get("team")})
    member = (request.form.get("member") or "").strip()
    if not member:
        return redirect(url_for("admin_teams", team=team_id))
    try:
        with _db() as conn:
            _execute(conn, "UPDATE team_members SET active=FALSE WHERE team_id=%s AND member=%s", (team_id, member))
        _bump_team_tasks_version(team_id)
    except Exception:
        app.logger.exception("admin_team_member_remove_failed")
    return redirect(url_for("admin_teams", team=team_id))


@app.post("/admin/teams/delete")
def admin_team_delete():
    _require_admin()
    team_id = _resolve_team_id({"team": request.form.get("team")})
    if team_id == DEFAULT_TEAM_ID:
        return redirect(url_for("admin_teams", team=team_id))
    try:
        with _db() as conn:
            counts = [
                _fetchone(conn, "SELECT COUNT(1) AS c FROM entries WHERE team_id=%s", (team_id,)),
                _fetchone(conn, "SELECT COUNT(1) AS c FROM member_logins WHERE team_id=%s", (team_id,)),
                _fetchone(conn, "SELECT COUNT(1) AS c FROM member_pauses WHERE team_id=%s", (team_id,)),
            ]
            if any(int(r["c"]) > 0 for r in counts if r):
                return redirect(url_for("admin_teams", team=team_id))
            _execute(conn, "DELETE FROM tasks WHERE team_id=%s", (team_id,))
            prefix = _team_prefix(team_id)
            _execute(conn, _sql("DELETE FROM task_categories WHERE name LIKE %s", "DELETE FROM task_categories WHERE name LIKE ?"), (prefix + "%",))
            _execute(conn, "DELETE FROM team_members WHERE team_id=%s", (team_id,))
            _execute(conn, "DELETE FROM teams WHERE id=%s", (team_id,))
        _task_cache.clear()
        _bump_global_teams_version()
    except Exception:
        app.logger.exception("admin_team_delete_failed")
    return redirect(url_for("admin_teams", team=DEFAULT_TEAM_ID))


@app.get("/admin/efficiency")
def admin_efficiency():
    _require_admin()
    team_id = _admin_team_id()
    teams = _all_teams()
    selected_name = next((t["name"] for t in teams if t["id"] == team_id), DEFAULT_TEAM_NAME)
    period = (request.args.get("period") or "day").strip().lower()
    if period not in {"day", "week", "month"}:
        period = "day"

    selected_date = request.args.get("date") or _today_str()
    try:
        selected_date = _parse_date(selected_date)
    except Exception:
        selected_date = _today_str()

    start_date, end_date = _period_range(selected_date, period)
    start_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_obj = datetime.strptime(end_date, "%Y-%m-%d").date()

    day_list: list[str] = []
    cursor = start_obj
    while cursor <= end_obj:
        day_list.append(cursor.isoformat())
        cursor = cursor + timedelta(days=1)

    rows = []
    task_breakdown_rows = []
    team_used = 0.0
    team_break = 0.0
    team_free = 0.0
    team_overtime = 0.0
    team_tasks = 0

    for member in _team_members(team_id):
        used_min = 0.0
        break_min = 0.0
        task_count = 0
        article_set: set[str] = set()
        login_days = 0
        per_task_total: dict[str, float] = {}
        per_task_count: dict[str, int] = {}
        per_task_articles: dict[str, set[str]] = {}

        for d in day_list:
            rec = _member_login_record_for_date(member, d, team_id)
            if rec and rec.get("login_at"):
                login_days += 1
            data = _build_member_day_entries(member, d, team_id)
            used_min += float(data["productive_min"])

            pauses = data["pauses"]
            now_dt = datetime.now()
            pause_seconds = 0
            for p in pauses:
                s = _dt_for_math(p.get("start"))
                if not s:
                    continue
                if p.get("end"):
                    e = _dt_for_math(p.get("end"))
                    if not e:
                        continue
                else:
                    e = now_dt if d == _today_str() else s
                if e > s:
                    pause_seconds += int((e - s).total_seconds())
            break_min += float(pause_seconds) / 60.0

            for e in data["entries"]:
                task_count += len(e["tasks"])
                akey = _article_key(str(e["article"]))
                if akey:
                    article_set.add(akey)
                for tid in e["tasks"]:
                    tmin = float(e["task_spent"].get(tid, 0.0))
                    per_task_total[tid] = per_task_total.get(tid, 0.0) + tmin
                    per_task_count[tid] = per_task_count.get(tid, 0) + 1
                    per_task_articles.setdefault(tid, set()).add(akey)

        capacity = login_days * float(WORKDAY_MINUTES)
        free_min = max(0.0, capacity - used_min - break_min)
        overtime_min = max(0.0, used_min + break_min - capacity)
        avg_task_min = (used_min / task_count) if task_count > 0 else 0.0

        rows.append(
            {
                "member": member,
                "login_days": login_days,
                "articles": len(article_set),
                "tasks": task_count,
                "avg_task_label": _minutes_label(avg_task_min),
                "avg_task_hours": _hours_value(avg_task_min),
                "used_label": _minutes_label(used_min),
                "used_hours": _hours_value(used_min),
                "break_label": _minutes_label(break_min),
                "break_hours": _hours_value(break_min),
                "free_label": _minutes_label(free_min),
                "free_hours": _hours_value(free_min),
                "overtime_label": _minutes_label(overtime_min),
                "overtime_hours": _hours_value(overtime_min),
            }
        )

        for tid, total in per_task_total.items():
            count = per_task_count.get(tid, 0)
            avg = (total / count) if count > 0 else 0.0
            task_breakdown_rows.append(
                {
                    "member": member,
                    "task": _task_label(tid, team_id),
                    "articles": len(per_task_articles.get(tid, set())),
                    "count": count,
                    "total_label": _minutes_label(total),
                    "total_hours": _hours_value(total),
                    "avg_label": _minutes_label(avg),
                    "avg_hours": _hours_value(avg),
                }
            )

        team_used += used_min
        team_break += break_min
        team_free += free_min
        team_overtime += overtime_min
        team_tasks += task_count

    avg_team_task = (team_used / team_tasks) if team_tasks > 0 else 0.0
    period_label = {"day": "Per Day", "week": "Per Week", "month": "Per Month"}[period]

    html = """
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Member Efficiency - {{ selected_name }}</title>
        <style>
          * { box-sizing: border-box; }
          body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0; padding: 28px 16px; color: #0f172a; background: radial-gradient(circle at 0% 0%, #eef4ff 0, #f8fbff 30%, #f6f8fc 100%); }
          .wrap { max-width: 1260px; margin: 0 auto; }
          .top { display:flex; justify-content: space-between; align-items: center; gap:10px; flex-wrap: wrap; }
          .card { border: 1px solid #dbe3ee; border-radius: 14px; padding: 16px; margin-top: 12px; background: #fff; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05); }
          .muted { color: #64748b; font-size: 12px; }
          .inline { display:flex; gap:10px; align-items: center; flex-wrap: wrap; }
          input, select { padding: 9px 10px; border: 1px solid #cbd5e1; border-radius: 10px; font-size: 14px; outline: none; background: #fff; }
          button { padding: 10px 12px; border: none; border-radius: 10px; cursor: pointer; background: #2563eb; color: #fff; font-size: 14px; font-weight: 600; }
          a { background: #e2e8f0; color: #0f172a; text-decoration: none; border-radius: 10px; padding: 8px 10px; font-weight: 600; border: 1px solid #cbd5e1; display: inline-block; }
          table { width: 100%; border-collapse: collapse; margin-top: 12px; }
          th, td { text-align: left; border-bottom: 1px solid #edf2f7; padding: 8px 6px; font-size: 13px; vertical-align: top; }
          th { font-size: 12px; color: #334155; background: #f8fafc; }
          tr:nth-child(even) td { background: #fcfdff; }
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="top">
            <div>
              <h1 style="font-size: 20px; margin: 0;">Member Efficiency</h1>
              <div class="muted">{{ period_label }} | Range: {{ start_date }} to {{ end_date }}</div>
            </div>
            <div class="inline">
              <select id="teamSel">{% for t in teams %}<option value="{{ t.id }}" {% if t.id == team_id %}selected{% endif %}>{{ t.name }}</option>{% endfor %}</select>
              <a href="{{ url_for('admin_report') }}?team={{ team_id }}">Back To Admin Report</a>
              <a href="{{ url_for('admin_tasks') }}?team={{ team_id }}">Tasks &amp; Estimation</a>
              <a href="{{ url_for('admin_search') }}?team={{ team_id }}">Admin Search</a>
              <a href="{{ url_for('home') }}?team={{ team_id }}">Home</a>
              <a href="#" id="logoutBtn">Logout</a>
            </div>
          </div>

          <div class="card">
            <form method="GET" action="{{ url_for('admin_efficiency') }}" class="inline">
              <input type="hidden" name="team" value="{{ team_id }}" />
              <input type="date" name="date" value="{{ selected_date }}" />
              <select name="period">
                <option value="day" {% if period == 'day' %}selected{% endif %}>Per Day</option>
                <option value="week" {% if period == 'week' %}selected{% endif %}>Per Week</option>
                <option value="month" {% if period == 'month' %}selected{% endif %}>Per Month</option>
              </select>
              <button type="submit">Load</button>
            </form>
          </div>

          <div class="card">
            <div class="muted">
              Team Used: <strong>{{ team_used_label }}</strong> ({{ team_used_hours }} hr) |
              Team Break: <strong>{{ team_break_label }}</strong> ({{ team_break_hours }} hr) |
              Team Free: <strong>{{ team_free_label }}</strong> ({{ team_free_hours }} hr) |
              Team Overtime: <strong>{{ team_overtime_label }}</strong> ({{ team_overtime_hours }} hr) |
              Team Avg/Task: <strong>{{ team_avg_task_label }}</strong> ({{ team_avg_task_hours }} hr)
            </div>
            <table>
              <thead>
                <tr>
                  <th>Member</th>
                  <th>Login Days</th>
                  <th>Articles Worked</th>
                  <th>Tasks Count</th>
                  <th>Avg Time/Task</th>
                  <th>Used</th>
                  <th>Break</th>
                  <th>Free</th>
                  <th>Overtime</th>
                </tr>
              </thead>
              <tbody>
                {% for r in rows %}
                  <tr>
                    <td>{{ r.member }}</td>
                    <td>{{ r.login_days }}</td>
                    <td>{{ r.articles }}</td>
                    <td>{{ r.tasks }}</td>
                    <td>{{ r.avg_task_label }} ({{ r.avg_task_hours }} hr)</td>
                    <td>{{ r.used_label }} ({{ r.used_hours }} hr)</td>
                    <td>{{ r.break_label }} ({{ r.break_hours }} hr)</td>
                    <td>{{ r.free_label }} ({{ r.free_hours }} hr)</td>
                    <td>{{ r.overtime_label }} ({{ r.overtime_hours }} hr)</td>
                  </tr>
                {% endfor %}
              </tbody>
            </table>
          </div>

          <div class="card">
            <h2 style="margin:0 0 8px; font-size: 16px;">Per Task Breakdown</h2>
            <div class="muted">Average time and unique articles per task per member.</div>
            <table>
              <thead>
                <tr>
                  <th>Member</th>
                  <th>Task</th>
                  <th>Unique Articles</th>
                  <th>Count</th>
                  <th>Total</th>
                  <th>Avg</th>
                </tr>
              </thead>
              <tbody>
                {% for r in task_rows %}
                  <tr>
                    <td>{{ r.member }}</td>
                    <td>{{ r.task }}</td>
                    <td>{{ r.articles }}</td>
                    <td>{{ r.count }}</td>
                    <td>{{ r.total_label }} ({{ r.total_hours }} hr)</td>
                    <td>{{ r.avg_label }} ({{ r.avg_hours }} hr)</td>
                  </tr>
                {% endfor %}
              </tbody>
            </table>
          </div>
        </div>
        <script>
          document.getElementById("teamSel").addEventListener("change", () => {
            const url = new URL(window.location.href);
            url.searchParams.set("team", document.getElementById("teamSel").value);
            window.location.href = url.toString();
          });
          document.getElementById("logoutBtn").addEventListener("click", async (e) => {
            e.preventDefault();
            await fetch("/admin/logout", { method: "POST" });
            window.location.href = "/admin/login";
          });
        </script>
      </body>
    </html>
    """

    return render_template_string(
        html,
        team_id=team_id,
        teams=teams,
        selected_name=selected_name,
        period=period,
        period_label=period_label,
        selected_date=selected_date,
        start_date=start_date,
        end_date=end_date,
        rows=rows,
        task_rows=sorted(task_breakdown_rows, key=lambda x: (x["member"], x["task"])),
        team_used_label=_minutes_label(team_used),
        team_used_hours=_hours_value(team_used),
        team_break_label=_minutes_label(team_break),
        team_break_hours=_hours_value(team_break),
        team_free_label=_minutes_label(team_free),
        team_free_hours=_hours_value(team_free),
        team_overtime_label=_minutes_label(team_overtime),
        team_overtime_hours=_hours_value(team_overtime),
        team_avg_task_label=_minutes_label(avg_team_task),
        team_avg_task_hours=_hours_value(avg_team_task),
    )


@app.get("/admin/report")
def admin_report():
    _require_admin()
    team_id = _admin_team_id()
    selected_date = request.args.get("date") or _today_str()
    try:
        selected_date = _parse_date(selected_date)
    except Exception:
        selected_date = _today_str()

    is_today = selected_date == _today_str()
    if not is_today:
        cache_key = f"{team_id}:{selected_date}"
        cached = _admin_report_cache.get(cache_key)
        if cached and (time.time() - cached[0]) < _ADMIN_REPORT_CACHE_TTL_SECONDS:
            return cached[1]
    now_dt = datetime.now()
    grouped = {}
    member_summary = {}
    team_used = 0.0
    team_break = 0.0

    members = _team_members(team_id)
    for member in members:
        data = _build_member_day_entries(member, selected_date, team_id)
        entries = data["entries"]
        used_min = float(data["productive_min"])
        pauses = data["pauses"]
        break_seconds = 0
        for p in pauses:
            s = _dt_for_math(p.get("start"))
            if not s:
                continue
            if p.get("end"):
                e = _dt_for_math(p.get("end"))
                if not e:
                    continue
            else:
                e = now_dt if is_today else s
            if e > s:
                break_seconds += int((e - s).total_seconds())
        break_min = float(break_seconds) / 60.0

        login_rec = _member_login_record_for_date(member, selected_date, team_id)
        login_at = login_rec.get("login_at") if login_rec else None
        logout_at = login_rec.get("logout_at") if login_rec else None

        current_timer = "-"
        if login_at and is_today:
            login_dt = _dt_for_math(login_at)
            end_dt = _dt_for_math(logout_at) if logout_at else now_dt
            if not login_dt or not end_dt:
                current_timer = "-"
            else:
                productive_sec = max(0, int((end_dt - login_dt).total_seconds()) - break_seconds)
                hh = productive_sec // 3600
                mm = (productive_sec % 3600) // 60
                ss = productive_sec % 60
                current_timer = f"{hh:02d}:{mm:02d}:{ss:02d}"

        free_min = max(0.0, float(WORKDAY_MINUTES) - used_min - break_min) if login_at else None

        grouped[member] = entries
        member_summary[member] = {
            "entries": len(entries),
            "used_label": _minutes_label(used_min),
            "used_hours": _hours_value(used_min),
            "break_label": _minutes_label(break_min),
            "break_hours": _hours_value(break_min),
            "free_label": _minutes_label(free_min) if free_min is not None else "Not Logged",
            "free_hours": _hours_value(free_min) if free_min is not None else None,
            "login_at_local": login_at.replace("T", " ") if login_at else "-",
            "logged_out": bool(logout_at),
            "current_timer": current_timer,
        }
        team_used += used_min
        team_break += break_min

    task_label_map = {t.id: t.label for t in _current_tasks(team_id=team_id, active_only=False)}
    for legacy_id, legacy_data in LEGACY_TASKS.items():
        task_label_map[legacy_id] = str(legacy_data["label"])

    html = """
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Admin Report</title>
        <style>
          :root { --bg-a:#fff8ea; --bg-b:#ecf5f4; --bg-c:#e8eef8; --panel:rgba(255,255,255,.92); --line:#d7e0ea; --text:#182133; --muted:#607086; --primary:#0f766e; --primary-strong:#115e59; --shadow:0 22px 50px rgba(24,33,51,.1); }
          * { box-sizing: border-box; }
          body { font-family: "Trebuchet MS", "Segoe UI", sans-serif; margin: 0; padding: 24px 16px 32px; color: var(--text); background: radial-gradient(circle at top left, var(--bg-a) 0, #f8f1e7 22%, var(--bg-b) 58%, var(--bg-c) 100%); }
          .wrap { max-width: 1280px; margin: 0 auto; }
          .top { display:flex; justify-content: space-between; align-items: end; gap: 14px; margin-bottom: 16px; flex-wrap: wrap; padding: 24px; border: 1px solid rgba(255,255,255,.58); border-radius: 26px; background: linear-gradient(145deg, rgba(255,255,255,.86), rgba(255,255,255,.68)); box-shadow: var(--shadow); backdrop-filter: blur(10px); }
          .muted { color: var(--muted); font-size: 12px; line-height: 1.55; }
          .card { border: 1px solid rgba(255,255,255,.66); border-radius: 24px; padding: 18px; margin-top: 14px; background: var(--panel); box-shadow: var(--shadow); backdrop-filter: blur(10px); }
          input[type=date], input[type=time], select { padding: 11px 12px; border: 1px solid var(--line); border-radius: 14px; font-size: 14px; outline: none; background: rgba(255,255,255,.96); color: var(--text); }
          input[type=date]:focus, input[type=time]:focus, select:focus { border-color: #86cfc5; box-shadow: 0 0 0 4px rgba(15,118,110,.12); }
          button { padding: 11px 14px; border: none; border-radius: 14px; cursor: pointer; background: linear-gradient(135deg, var(--primary), var(--primary-strong)); color: #fff; font-size: 14px; font-weight: 700; box-shadow: 0 18px 32px rgba(15,118,110,.24); }
          button.secondary { background: rgba(255,255,255,.88); color: var(--text); border: 1px solid var(--line); box-shadow: none; }
          button.secondary:hover { background: #fff; }
          a { color: var(--text); text-decoration: none; font-weight: 700; }
          .nav-link { border: 1px solid var(--line); border-radius: 14px; padding: 10px 12px; background: rgba(255,255,255,.86); }
          .inline { display:flex; gap:10px; align-items:center; flex-wrap: wrap; }
          table { width: 100%; border-collapse: collapse; margin-top: 12px; }
          th, td { text-align: left; border-bottom: 1px solid #e8edf3; padding: 10px 8px; font-size: 13px; vertical-align: top; }
          th { font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; background: rgba(248,250,252,.72); }
          tr:nth-child(even) td { background: rgba(252,253,255,.72); }
          .badge { display: inline-block; margin: 2px 4px 2px 0; font-size: 12px; color: #8a5a02; background: #fff7e8; padding: 5px 10px; border-radius: 999px; white-space: nowrap; font-weight: 700; cursor: pointer; border: 1px solid #f4d28a; }
          .entry-row.is-match td { background: #effaf8 !important; }
          .entry-row.is-dim td { opacity: 0.45; }
          .task-summary { margin-top: 12px; padding: 12px 14px; border: 1px dashed var(--line); border-radius: 16px; background: rgba(255,255,255,.74); font-size: 13px; display: none; }
          .btn-disabled { opacity: 0.45; pointer-events: none; }
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="top">
            <div>
              <h1 style="font-size: 18px; margin: 0;">Admin: Team Report</h1>
              <div class="muted">
                Date: <strong>{{ selected_date }}</strong> |
                Used: <strong id="teamUsed">{{ team_used_label }}</strong> ({{ team_used_hours }} hr) |
                Break: <strong id="teamBreak">{{ team_break_label }}</strong> ({{ team_break_hours }} hr)
              </div>
            </div>
            <div class="inline">
              <select id="teamSel">
                {% for t in teams %}
                  <option value="{{ t.id }}" {% if t.id == team_id %}selected{% endif %}>{{ t.name }}</option>
                {% endfor %}
              </select>
              <input id="d" type="date" value="{{ selected_date }}" max="{{ today }}" />
              <select id="statusFilter">
                <option value="all">All Status</option>
                <option value="completed">Completed</option>
                <option value="in_progress">In Progress</option>
                <option value="overtime">Overtime</option>
              </select>
              <button id="go" class="secondary">Load</button>
              <a class="nav-link" href="{{ url_for('admin_search') }}?team={{ team_id }}">Admin Search</a>
              <a class="nav-link" href="{{ url_for('admin_tasks') }}?team={{ team_id }}">Tasks &amp; Estimation</a>
              <a class="nav-link" href="{{ url_for('admin_efficiency') }}?team={{ team_id }}">Member Efficiency</a>
              <a class="nav-link" href="{{ url_for('admin_export_csv') }}?team={{ team_id }}&date={{ selected_date }}">Export CSV</a>
              <a class="nav-link" href="{{ url_for('admin_export_tasks_csv') }}?team={{ team_id }}&date={{ selected_date }}">Export Tasks CSV</a>
              <a class="nav-link" href="{{ url_for('admin_export_builder') }}?team={{ team_id }}&date={{ selected_date }}">Export Builder</a>
              <a class="nav-link" href="{{ url_for('admin_teams') }}?team={{ team_id }}">Teams</a>
              <a class="nav-link" href="{{ url_for('admin_db') }}?team={{ team_id }}">DB Backup</a>
              <a class="nav-link" href="{{ url_for('home') }}?team={{ team_id }}">Home</a>
              <button id="logout" class="secondary">Logout</button>
            </div>
          </div>

          {% for member, entries in grouped.items() %}
            <div class="card member-card" data-member="{{ member }}">
              <div style="display:flex; justify-content: space-between; align-items: baseline; gap: 10px; flex-wrap: wrap;">
                <div><strong>{{ member }}</strong></div>
                <div class="muted member-summary">
                  Entries: <span class="member-entry-count">{{ member_summary[member]["entries"] }}</span> |
                  Used: <span class="member-used">{{ member_summary[member]["used_label"] }}</span> ({{ member_summary[member]["used_hours"] }} hr) |
                  Break: <span class="member-break">{{ member_summary[member]["break_label"] }}</span> ({{ member_summary[member]["break_hours"] }} hr) |
                  Free: <span class="member-free">{{ member_summary[member]["free_label"] }}</span>{% if member_summary[member]["free_hours"] is not none %} ({{ member_summary[member]["free_hours"] }} hr){% endif %} |
                  Login: <span class="member-login">{{ member_summary[member]["login_at_local"] }}</span> |
                  Current: <span class="member-current">{{ member_summary[member]["current_timer"] }}</span>
                </div>
              </div>

              <div class="inline" style="margin-top:8px;">
                <input class="login-reset-time" type="time" />
                <button class="secondary login-reset-btn" data-member="{{ member }}">Reset Login</button>
                <button class="secondary logout-reset-btn {% if not member_summary[member]['logged_out'] %}btn-disabled{% endif %}" data-member="{{ member }}">Reset Logout</button>
              </div>

              <div class="task-summary"></div>

              {% if entries|length == 0 %}
                <div class="muted" style="margin-top:10px;">No entries.</div>
              {% else %}
                <table>
                  <thead>
                    <tr>
                      <th>Time</th>
                      <th>Article</th>
                      <th>Status</th>
                      <th>Tasks</th>
                      <th>Spent</th>
                    </tr>
                  </thead>
                  <tbody>
                    {% for e in entries %}
                      <tr class="entry-row" data-article="{{ e.article }}" data-task-ids="{{ e.tasks|join(',') }}" data-status="{{ 'completed' if e.completed else 'in_progress' }}" data-overtime="{{ '1' if e.overtime else '0' }}" data-spent-minutes="{{ e.spent_total }}" data-task-spent='{{ e.task_spent | tojson }}'>
                        <td>{{ e.created_at_local }}</td>
                        <td>{{ e.article }}</td>
                        <td>{{ "Completed" if e.completed else "In Progress" }}</td>
                        <td>
                          {% for tid in e.tasks %}
                            <span class="badge task-chip" data-task-id="{{ tid }}">{{ task_label_map.get(tid, tid) }}</span>
                          {% endfor %}
                        </td>
                        <td>{{ e.spent_total_label }}</td>
                      </tr>
                    {% endfor %}
                  </tbody>
                </table>
              {% endif %}
            </div>
          {% endfor %}
        </div>

        <script>
          function formatMinutesLabel(minutes) {
            const value = Math.round(Number(minutes) || 0);
            if (value < 60) return `${value} min`;
            const h = Math.floor(value / 60);
            const m = value % 60;
            if (m === 0) return `${h} hr`;
            return `${h}h ${m}m`;
          }

          function applyStatusFilter() {
            const value = (document.getElementById("statusFilter")?.value || "all");
            let teamUsed = 0;
            document.querySelectorAll(".member-card").forEach((card) => {
              const rows = Array.from(card.querySelectorAll(".entry-row"));
              let visibleCount = 0;
              let memberUsedMin = 0;
              rows.forEach((row) => {
                const status = row.dataset.status || "completed";
                const show = value === "all" || status === value || (value === "overtime" && row.dataset.overtime === "1");
                row.style.display = show ? "" : "none";
                row.classList.remove("is-match", "is-dim");
                if (show) {
                  visibleCount += 1;
                  memberUsedMin += Number(row.dataset.spentMinutes || 0);
                }
              });
              teamUsed += memberUsedMin;
              const countEl = card.querySelector(".member-entry-count");
              if (countEl) countEl.textContent = String(visibleCount);
              const usedEl = card.querySelector(".member-used");
              if (usedEl) usedEl.textContent = formatMinutesLabel(memberUsedMin);
              card.dataset.usedMin = String(memberUsedMin);
              const summaryBox = card.querySelector(".task-summary");
              if (summaryBox) { summaryBox.style.display = "none"; summaryBox.innerHTML = ""; }
            });
            document.getElementById("teamUsed").textContent = formatMinutesLabel(teamUsed);
          }

          function clearTaskHighlight(card) {
            card.querySelectorAll(".entry-row").forEach((row) => row.classList.remove("is-match", "is-dim"));
            card.querySelectorAll(".task-chip").forEach((chip) => chip.classList.remove("is-match"));
            const box = card.querySelector(".task-summary");
            if (box) { box.style.display = "none"; box.innerHTML = ""; }
          }

          function highlightTask(card, taskId, taskLabel) {
            const rows = Array.from(card.querySelectorAll(".entry-row")).filter((r) => r.style.display !== "none");
            let totalMin = 0;
            let count = 0;
            rows.forEach((row) => {
              const ids = (row.dataset.taskIds || "").split(",").filter(Boolean);
              const match = ids.includes(taskId);
              row.classList.toggle("is-match", match);
              row.classList.toggle("is-dim", !match);
              if (match) {
                count += 1;
                let spent = 0;
                try {
                  const m = JSON.parse(row.dataset.taskSpent || "{}");
                  spent = Number(m[taskId] || 0);
                } catch (e) { spent = 0; }
                totalMin += spent;
              }
            });
            const box = card.querySelector(".task-summary");
            if (!box) return;
            box.innerHTML = `<strong>Task:</strong> ${taskLabel}<br><strong>Total entries:</strong> ${count}<br><strong>Total hours used:</strong> ${formatMinutesLabel(totalMin)} (${(totalMin/60).toFixed(2)} hr) <div style="margin-top:8px;"><button class="secondary clear-task" style="padding:6px 10px;font-size:12px;">Clear</button></div>`;
            box.style.display = "block";
            box.querySelector(".clear-task").addEventListener("click", () => clearTaskHighlight(card));
          }

          document.getElementById("go").addEventListener("click", () => {
            const d = document.getElementById("d").value;
            const team = document.getElementById("teamSel").value;
            const url = new URL(window.location.href);
            url.searchParams.set("date", d);
            url.searchParams.set("team", team);
            window.location.href = url.toString();
          });
          document.getElementById("teamSel").addEventListener("change", () => {
            const d = document.getElementById("d").value;
            const team = document.getElementById("teamSel").value;
            const url = new URL(window.location.href);
            url.searchParams.set("team", team);
            url.searchParams.set("date", d);
            window.location.href = url.toString();
          });
          document.getElementById("statusFilter").addEventListener("change", applyStatusFilter);
          applyStatusFilter();

          document.addEventListener("click", async (event) => {
            const chip = event.target.closest(".task-chip");
            if (chip) {
              const card = chip.closest(".member-card");
              highlightTask(card, chip.dataset.taskId, chip.textContent || chip.dataset.taskId);
              return;
            }
            const loginBtn = event.target.closest(".login-reset-btn");
            if (loginBtn) {
              const member = loginBtn.dataset.member || "";
              const card = loginBtn.closest(".member-card");
              const timeVal = card.querySelector(".login-reset-time")?.value || "";
              const dateVal = document.getElementById("d").value;
              const team = document.getElementById("teamSel").value;
              if (!member || !timeVal) return;
              const res = await fetch("/admin/reset-login", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ team, member, date: dateVal, time: timeVal }) });
              if (res.ok) window.location.reload();
              return;
            }
            const logoutBtn = event.target.closest(".logout-reset-btn");
            if (logoutBtn) {
              if (logoutBtn.classList.contains("btn-disabled")) return;
              const member = logoutBtn.dataset.member || "";
              const dateVal = document.getElementById("d").value;
              const team = document.getElementById("teamSel").value;
              const res = await fetch("/admin/reset-logout", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ team, member, date: dateVal }) });
              if (res.ok) window.location.reload();
              return;
            }
          });

          document.getElementById("logout").addEventListener("click", async () => {
            await fetch("/admin/logout", { method: "POST" });
            window.location.href = "/admin/login";
          });
        </script>
      </body>
    </html>
    """

    rendered = render_template_string(
        html,
        team_id=team_id,
        teams=_all_teams(),
        selected_date=selected_date,
        today=_today_str(),
        grouped=grouped,
        member_summary=member_summary,
        team_used_label=_minutes_label(team_used),
        team_used_hours=_hours_value(team_used),
        team_break_label=_minutes_label(team_break),
        team_break_hours=_hours_value(team_break),
        task_label_map=task_label_map,
    )
    if not is_today:
        _admin_report_cache[f"{team_id}:{selected_date}"] = (time.time(), rendered)
    return rendered


@app.get("/admin/export.csv")
def admin_export_csv():
    _require_admin()
    team_id = _admin_team_id()
    anchor_date = request.args.get("date") or _today_str()
    try:
        anchor_date = _parse_date(anchor_date)
    except Exception:
        anchor_date = _today_str()

    start_date = request.args.get("start") or anchor_date
    end_date = request.args.get("end") or anchor_date
    try:
        start_date = _parse_date(start_date)
        end_date = _parse_date(end_date)
    except Exception:
        start_date = anchor_date
        end_date = anchor_date

    member_filter = (request.args.get("member") or "").strip()
    team_members = _team_members(team_id)
    members = team_members if not member_filter else ([member_filter] if member_filter in team_members else team_members)

    allowed = [
        "date",
        "member",
        "article",
        "status",
        "tasks",
        "spent_total_min",
        "spent_total_label",
        "break_total_min",
        "break_total_label",
        "first_at",
        "last_at",
    ]
    default_cols = list(allowed)
    raw_cols = request.args.getlist("cols")
    if not raw_cols:
        raw_cols = [c.strip() for c in (request.args.get("cols") or "").split(",") if c.strip()]
    cols = [c for c in raw_cols if c in allowed] or default_cols

    output: list[list[str]] = [cols]
    day_list = _date_range_list(start_date, end_date)
    for d in day_list:
        now_dt = datetime.now()
        for member in members:
            data = _build_member_day_entries(member, d, team_id)
            break_min = _break_minutes_for_pauses(data["pauses"], d, now_dt)
            for e in data["entries"]:
                task_labels = [_task_label(str(t), team_id) for t in e["tasks"]]
                row = {
                    "date": d,
                    "member": member,
                    "article": str(e["article"]),
                    "status": "Completed" if bool(e["completed"]) else "In Progress",
                    "tasks": ", ".join(task_labels),
                    "spent_total_min": str(round(float(e["spent_total"]), 2)),
                    "spent_total_label": str(e["spent_total_label"]),
                    "break_total_min": str(round(float(break_min), 2)),
                    "break_total_label": _minutes_label(break_min),
                    "first_at": str(e["created_at_local"]),
                    "last_at": str(e.get("last_at_local") or e["created_at_local"]),
                }
                output.append([row.get(c, "") for c in cols])

    filename = f"team_{start_date}_to_{end_date}.csv" if start_date != end_date else f"team_{start_date}.csv"
    return _csv_response(output, filename)


@app.get("/admin/export_tasks.csv")
def admin_export_tasks_csv():
    _require_admin()
    team_id = _admin_team_id()
    anchor_date = request.args.get("date") or _today_str()
    try:
        anchor_date = _parse_date(anchor_date)
    except Exception:
        anchor_date = _today_str()

    start_date = request.args.get("start") or anchor_date
    end_date = request.args.get("end") or anchor_date
    try:
        start_date = _parse_date(start_date)
        end_date = _parse_date(end_date)
    except Exception:
        start_date = anchor_date
        end_date = anchor_date

    member_filter = (request.args.get("member") or "").strip()
    team_members = _team_members(team_id)
    members = team_members if not member_filter else ([member_filter] if member_filter in team_members else team_members)

    allowed = [
        "date",
        "member",
        "article",
        "task_id",
        "task",
        "task_spent_min",
        "task_spent_label",
        "status",
        "break_total_min",
        "break_total_label",
        "first_at",
        "last_at",
    ]
    default_cols = list(allowed)
    raw_cols = request.args.getlist("cols")
    if not raw_cols:
        raw_cols = [c.strip() for c in (request.args.get("cols") or "").split(",") if c.strip()]
    cols = [c for c in raw_cols if c in allowed] or default_cols

    output: list[list[str]] = [cols]
    day_list = _date_range_list(start_date, end_date)
    for d in day_list:
        now_dt = datetime.now()
        for member in members:
            data = _build_member_day_entries(member, d, team_id)
            break_min = _break_minutes_for_pauses(data["pauses"], d, now_dt)
            for e in data["entries"]:
                spent_map = e.get("task_spent") or {}
                for tid in e.get("tasks") or []:
                    task_id = str(tid)
                    tmin = float(spent_map.get(task_id, 0.0))
                    row = {
                        "date": d,
                        "member": member,
                        "article": str(e["article"]),
                        "task_id": task_id,
                        "task": _task_label(task_id, team_id),
                        "task_spent_min": str(round(float(tmin), 2)),
                        "task_spent_label": _minutes_label(tmin),
                        "status": "Completed" if bool(e["completed"]) else "In Progress",
                        "break_total_min": str(round(float(break_min), 2)),
                        "break_total_label": _minutes_label(break_min),
                        "first_at": str(e["created_at_local"]),
                        "last_at": str(e.get("last_at_local") or e["created_at_local"]),
                    }
                    output.append([row.get(c, "") for c in cols])

    filename = f"tasks_{start_date}_to_{end_date}.csv" if start_date != end_date else f"tasks_{start_date}.csv"
    return _csv_response(output, filename)


@app.get("/admin/export")
def admin_export_builder():
    _require_admin()
    team_id = _admin_team_id()
    selected_date = request.args.get("date") or _today_str()
    try:
        selected_date = _parse_date(selected_date)
    except Exception:
        selected_date = _today_str()

    html = """
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Export Builder</title>
        <style>
          * { box-sizing: border-box; }
          body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0; padding: 22px 14px; color: #0f172a; background: radial-gradient(circle at 0% 0%, #eef4ff 0, #f8fbff 30%, #f6f8fc 100%); }
          .wrap { max-width: 1100px; margin: 0 auto; }
          .top { display:flex; justify-content: space-between; align-items: center; gap:10px; flex-wrap: wrap; }
          .card { border: 1px solid #dbe3ee; border-radius: 16px; padding: 16px; margin-top: 12px; background: #fff; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05); }
          .muted { color: #64748b; font-size: 12px; line-height: 1.55; }
          form { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
          label { display:block; font-size: 12px; font-weight: 700; color: #334155; margin-bottom: 6px; }
          input, select { width: 100%; padding: 10px 12px; border: 1px solid #cbd5e1; border-radius: 12px; font-size: 14px; outline: none; }
          input:focus, select:focus { border-color: #86cfc5; box-shadow: 0 0 0 4px rgba(15,118,110,.12); }
          .row { display:flex; gap: 10px; flex-wrap: wrap; align-items: center; }
          .cols { display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; margin-top: 10px; }
          .col { display:flex; gap: 10px; align-items: center; padding: 10px 12px; border: 1px solid #e2e8f0; border-radius: 12px; background: #fbfdff; }
          button { padding: 11px 14px; border: none; border-radius: 12px; cursor: pointer; background: linear-gradient(135deg, #0f766e, #115e59); color: #fff; font-size: 14px; font-weight: 800; }
          a { background: #e2e8f0; color: #0f172a; text-decoration: none; border-radius: 12px; padding: 10px 12px; font-weight: 700; border: 1px solid #cbd5e1; display: inline-block; }
          @media (max-width: 860px) { form { grid-template-columns: 1fr; } .cols { grid-template-columns: 1fr; } }
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="top">
            <div>
              <h1 style="font-size: 20px; margin: 0;">Export Builder</h1>
              <div class="muted">Build a CSV export for Excel: pick date range, member, export type, and columns.</div>
            </div>
            <div class="row">
              <a href="{{ url_for('admin_report') }}?team={{ team_id }}">Back To Admin Report</a>
              <a href="{{ url_for('home') }}?team={{ team_id }}">Home</a>
            </div>
          </div>

          <div class="card">
            <form method="GET" action="{{ url_for('admin_export_download') }}">
              <input type="hidden" name="team" value="{{ team_id }}" />
              <div>
                <label>Start date</label>
                <input type="date" name="start" value="{{ selected_date }}" max="{{ today }}" />
              </div>
              <div>
                <label>End date</label>
                <input type="date" name="end" value="{{ selected_date }}" max="{{ today }}" />
              </div>
              <div>
                <label>Member</label>
                <select name="member">
                  <option value="">All members</option>
                  {% for m in members %}<option value="{{ m }}">{{ m }}</option>{% endfor %}
                </select>
              </div>
              <div>
                <label>Export type</label>
                <select name="type" id="type">
                  <option value="summary">Summary (per article)</option>
                  <option value="tasks">Tasks (one row per task)</option>
                </select>
              </div>

              <div style="grid-column: 1 / -1;">
                <label>Columns</label>
                <div class="muted">Choose the columns you want. If you leave everything unchecked, the export includes all default columns.</div>
                <div class="cols" id="summaryCols">
                  {% for c in summary_cols %}
                    <label class="col"><input type="checkbox" name="cols" value="{{ c }}" checked /> <span>{{ c }}</span></label>
                  {% endfor %}
                </div>
                <div class="cols" id="taskCols" style="display:none;">
                  {% for c in task_cols %}
                    <label class="col"><input type="checkbox" name="task_cols_sel" value="{{ c }}" checked /> <span>{{ c }}</span></label>
                  {% endfor %}
                </div>
              </div>

              <div style="grid-column: 1 / -1; display:flex; gap:10px; flex-wrap:wrap; align-items:center;">
                <button type="submit">Download CSV</button>
                <div class="muted">Tip: Excel can open CSV directly.</div>
              </div>
            </form>
          </div>
        </div>
        <script>
          const typeSel = document.getElementById("type");
          const summaryCols = document.getElementById("summaryCols");
          const taskCols = document.getElementById("taskCols");
          function sync(){
            const t = typeSel.value;
            summaryCols.style.display = (t === "summary") ? "" : "none";
            taskCols.style.display = (t === "tasks") ? "" : "none";
          }
          typeSel.addEventListener("change", sync);
          sync();
        </script>
      </body>
    </html>
    """
    return render_template_string(
        html,
        team_id=team_id,
        selected_date=selected_date,
        today=_today_str(),
        members=_team_members(team_id),
        summary_cols=[
            "date",
            "member",
            "article",
            "status",
            "tasks",
            "spent_total_min",
            "spent_total_label",
            "break_total_min",
            "break_total_label",
            "first_at",
            "last_at",
        ],
        task_cols=[
            "date",
            "member",
            "article",
            "task_id",
            "task",
            "task_spent_min",
            "task_spent_label",
            "status",
            "break_total_min",
            "break_total_label",
            "first_at",
            "last_at",
        ],
    )


@app.get("/admin/export/download.csv")
def admin_export_download():
    _require_admin()
    team_id = _admin_team_id()
    export_type = (request.args.get("type") or "summary").strip().lower()
    start_date = request.args.get("start") or _today_str()
    end_date = request.args.get("end") or start_date
    member = (request.args.get("member") or "").strip()

    if export_type == "tasks":
        cols = request.args.getlist("task_cols_sel")
        if not cols:
            cols = request.args.getlist("cols")
        args = {"team": team_id, "start": start_date, "end": end_date, "member": member}
        if cols:
            args["cols"] = ",".join(cols)
        return redirect(url_for("admin_export_tasks_csv", **{k: v for k, v in args.items() if v}))

    cols = request.args.getlist("cols")
    args = {"team": team_id, "start": start_date, "end": end_date, "member": member}
    if cols:
        args["cols"] = ",".join(cols)
    return redirect(url_for("admin_export_csv", **{k: v for k, v in args.items() if v}))


@app.get("/admin/db")
def admin_db():
    _require_admin()
    team_id = _admin_team_id()
    mode = "Postgres" if _use_postgres() else "SQLite"
    restore_enabled = _sqlite_restore_enabled()
    teams = _all_teams()
    selected_name = next((t["name"] for t in teams if t["id"] == team_id), DEFAULT_TEAM_NAME)
    html = """
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Database Backup</title>
        <style>
          * { box-sizing: border-box; }
          body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0; padding: 22px 14px; color: #0f172a; background: radial-gradient(circle at 0% 0%, #eef4ff 0, #f8fbff 30%, #f6f8fc 100%); }
          .wrap { max-width: 980px; margin: 0 auto; }
          .top { display:flex; justify-content: space-between; align-items: center; gap:10px; flex-wrap: wrap; }
          .card { border: 1px solid #dbe3ee; border-radius: 16px; padding: 16px; margin-top: 12px; background: #fff; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05); }
          .muted { color: #64748b; font-size: 12px; line-height: 1.55; }
          .row { display:flex; gap: 10px; flex-wrap: wrap; align-items: center; }
          a.btn { background: linear-gradient(135deg, #0f766e, #115e59); color: #fff; text-decoration: none; border-radius: 12px; padding: 10px 12px; font-weight: 800; border: 0; display: inline-block; }
          a.btn.secondary { background: #e2e8f0; color: #0f172a; border: 1px solid #cbd5e1; font-weight: 700; }
          input[type=file] { width: 100%; padding: 10px 12px; border: 1px solid #cbd5e1; border-radius: 12px; font-size: 14px; background: #fff; }
          button { padding: 10px 12px; border: none; border-radius: 12px; cursor: pointer; background: linear-gradient(135deg, #0f766e, #115e59); color: #fff; font-size: 14px; font-weight: 800; }
          .pill { display:inline-flex; gap:8px; align-items:center; padding: 8px 12px; border-radius: 999px; border: 1px solid #e2e8f0; background: #fbfdff; font-weight: 800; font-size: 12px; }
          .warn { border-color: #f4d28a; background: #fff7e8; color: #8a5a02; }
        </style>
      </head>
      <body>
        <div class="wrap">
          <div class="top">
            <div>
              <h1 style="font-size: 20px; margin: 0;">Database Backup</h1>
              <div class="muted">Export a master backup. Optional restore is available only when explicitly enabled for local mode.</div>
            </div>
            <div class="row">
              <a class="btn secondary" href="{{ url_for('admin_report') }}?team={{ team_id }}">Back To Admin Report</a>
              <a class="btn secondary" href="{{ url_for('home') }}?team={{ team_id }}">Home</a>
            </div>
          </div>

          <div class="card">
            <div class="row" style="justify-content: space-between;">
              <div class="pill">DB Mode: {{ mode }}</div>
              <div class="pill">Timezone: {{ timezone }}</div>
            </div>
            <div class="row" style="margin-top: 10px; justify-content: space-between;">
              <div class="pill">Team: {{ selected_name }}</div>
              <form method="GET" action="{{ url_for('admin_db') }}" class="row">
                <select name="team">
                  {% for t in teams %}
                    <option value="{{ t.id }}" {% if t.id == team_id %}selected{% endif %}>{{ t.name }}</option>
                  {% endfor %}
                </select>
                <button type="submit">Switch</button>
              </form>
            </div>
            <div class="muted" style="margin-top: 10px;">
              For local shared-folder mode, set SQLITE_PATH to a shared folder and run the app. Then you can import/restore the master DB file if enabled.
            </div>
            <div class="row" style="margin-top: 12px;">
              <a class="btn" href="{{ url_for('admin_db_export') }}?team={{ team_id }}">Download Team Backup</a>
              <a class="btn secondary" href="{{ url_for('admin_db_export') }}?mode=full">Download Full Backup</a>
            </div>
          </div>

          <div class="card">
            <div class="row" style="justify-content: space-between;">
              <div class="pill {% if not restore_enabled %}warn{% endif %}">Restore: {{ "Enabled" if restore_enabled else "Disabled" }}</div>
              <div class="muted">Env flag required: ALLOW_DB_RESTORE=1 (SQLite only)</div>
            </div>
            {% if restore_enabled %}
              <form method="POST" action="{{ url_for('admin_db_restore') }}" enctype="multipart/form-data" style="margin-top: 12px;">
                <div class="muted">Upload a SQLite master database file. The app creates a timestamped backup of the current DB before swapping.</div>
                <div style="margin-top: 10px;">
                  <input type="file" name="db" accept=".sqlite3,.db,application/octet-stream" required />
                </div>
                <div class="row" style="margin-top: 10px;">
                  <button type="submit">Restore / Switch DB</button>
                </div>
              </form>
            {% else %}
              <div class="muted" style="margin-top: 12px;">Restore is disabled by default to prevent accidental data loss in production.</div>
            {% endif %}
          </div>
        </div>
      </body>
    </html>
    """
    return render_template_string(
        html,
        team_id=team_id,
        teams=teams,
        selected_name=selected_name,
        mode=mode,
        timezone=str(_APP_TZ),
        restore_enabled=restore_enabled,
    )


@app.get("/admin/db/export")
def admin_db_export():
    _require_admin()
    mode = (request.args.get("mode") or "").strip().lower()
    team_id = (request.args.get("team") or "").strip()
    if team_id and not _team_exists(team_id):
        team_id = DEFAULT_TEAM_ID
    if not _use_postgres() and mode == "full":
        return _stream_sqlite_snapshot("master.sqlite3")
    payload = _db_backup_payload(team_id=team_id if mode != "full" else None)
    stamp = datetime.now(_APP_TZ).strftime("%Y%m%d_%H%M%S")
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    name = f"team_{team_id}_" if team_id and mode != "full" else "full_"
    return Response(
        data,
        mimetype="application/json",
        headers={"Content-Disposition": f'attachment; filename="{name}backup_{stamp}.json"'},
    )


@app.post("/admin/db/restore")
def admin_db_restore():
    _require_admin()
    if not _sqlite_restore_enabled():
        return jsonify({"error": "Restore is disabled."}), 403
    uploaded = request.files.get("db")
    if not uploaded:
        return jsonify({"error": "Missing file."}), 400
    os.makedirs(os.path.dirname(SQLITE_PATH), exist_ok=True)
    stamp = datetime.now(_APP_TZ).strftime("%Y%m%d_%H%M%S")
    upload_path = f"{SQLITE_PATH}.upload_{stamp}"
    uploaded.save(upload_path)
    try:
        with open(upload_path, "rb") as f:
            head = f.read(16)
        if not head.startswith(b"SQLite format 3\x00"):
            try:
                os.remove(upload_path)
            except Exception:
                pass
            return jsonify({"error": "Invalid SQLite database file."}), 400

        backup_path = f"{SQLITE_PATH}.bak_{stamp}"
        with _sqlite_swap_lock:
            if os.path.exists(SQLITE_PATH):
                shutil.copy2(SQLITE_PATH, backup_path)
            os.replace(upload_path, SQLITE_PATH)
            _task_cache.clear()
            _admin_report_cache.clear()
            global _db_initialized
            _db_initialized = False
    finally:
        try:
            if os.path.exists(upload_path):
                os.remove(upload_path)
        except Exception:
            pass

    return redirect(url_for("admin_db"))


@app.errorhandler(401)
def unauthorized(_):
    return redirect(url_for("admin_login_page"))


def _init_db_startup() -> None:
    last_error = None
    for _ in range(25):
        try:
            _init_db()
            return
        except Exception as e:
            last_error = e
            time.sleep(1.0)
    if last_error:
        raise last_error

@app.before_request
def init():
    global _db_initialized
    if _db_initialized:
        return
    with _db_init_lock:
        if _db_initialized:
            return
        try:
            _init_db()
            _db_initialized = True
        except Exception as e:
            # Log but don't crash - database will retry on next request
            import sys
            print(f"[WARNING] Database initialization failed: {e}", file=sys.stderr)
            pass

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = (os.environ.get("FLASK_DEBUG") or "").strip() in {"1", "true", "True"}
    app.run(host="0.0.0.0", port=port, debug=debug)
