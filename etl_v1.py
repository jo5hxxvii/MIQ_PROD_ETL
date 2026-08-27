#!/usr/bin/env python3
"""
DLD End-to-End ETL Script
=========================
Phases:
  1. Download CSVs from SFTP
  2. Backup rotation (skipped if last run was incomplete)
  3. Load CSVs into Postgres
  4. Refresh validated tables
  5. Run MIQ component functions
  6. Run monthly snapshot (1st of month only, non-fatal)

Run tracking:
  Every run is recorded in etl_run_log (public schema).
  A run is marked 'complete' only if all phases 1-5 succeed
  with zero failed downloads or table loads.
  Snapshot result is recorded separately and never affects run status.
  If the last run was not 'complete', backup rotation is skipped
  to avoid backing up a broken state.

Slack notifications:
  Set DLD_SLACK_WEBHOOK env var to enable.
  Sends on: run complete, run failed, phase failure, snapshot failed.

Usage:
  python dld_etl.py

Config:
  Set credentials via environment variables (see CONFIG section).
  Never hardcode passwords in this file.
"""

import gc
import json
import logging
import os
import urllib.request
from datetime import date, datetime
from functions.config import _config

import pandas as pd
import paramiko
from sqlalchemy import create_engine, text, types
from sqlalchemy.dialects.postgresql import TIMESTAMP

# # ── CONFIG ────────────────────────────────────────────────────
# SFTP_HOST = os.environ.get("DLD_SFTP_HOST", "144.126.255.45")
# SFTP_PORT = int(os.environ.get("DLD_SFTP_PORT", "22"))
# SFTP_USERNAME = os.environ.get("DLD_SFTP_USERNAME", "aiq")
# SFTP_PASSWORD = os.environ.get("DLD_SFTP_PASSWORD", "fvaMkCsu3tboNotZcBB")
# SFTP_KEY_PATH = os.environ.get("DLD_SFTP_KEY_PATH", None)
# SFTP_FOLDER = os.environ.get("DLD_SFTP_FOLDER", "/uploads")

# PG_URL = os.environ.get(
#     "DLD_PG_URL",
#     "postgresql://doadmin:AVNS_yH4rB22omDE4Q93wusT@miq-db-postgresql-v1-do-user-23625062-0.g.db.ondigitalocean.com:25060/defaultdb",
# )

# # Slack webhook — set via environment variable
# # export DLD_SLACK_WEBHOOK="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
# # Leave empty to disable Slack notifications silently
# SLACK_WEBHOOK = os.environ.get(
#     "DLD_SLACK_WEBHOOK",
#     "https://hooks.slack.com/services/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",
# )

# LOCAL_DOWNLOAD_FOLDER = "./dld_downloads"
# BACKUP_SCHEMA_COUNT = 3
# CHUNK_SIZE = 20000
# DATE_KEYWORDS = ["at", "date", "time", "timestamp", "created", "updated"]
#
SFTP_HOST = _config.SFTP_HOST
SFTP_PORT = int(os.environ.get("DLD_SFTP_PORT", "22"))
SFTP_USERNAME = os.environ.get("DLD_SFTP_USERNAME", "aiq")
SFTP_PASSWORD = os.environ.get("DLD_SFTP_PASSWORD", "fvaMkCsu3tboNotZcBB")
SFTP_KEY_PATH = os.environ.get("DLD_SFTP_KEY_PATH", None)
SFTP_FOLDER = os.environ.get("DLD_SFTP_FOLDER", "/uploads")

PG_URL = os.environ.get(
    "DLD_PG_URL",
    "postgresql://doadmin:AVNS_yH4rB22omDE4Q93wusT@miq-db-postgresql-v1-do-user-23625062-0.g.db.ondigitalocean.com:25060/defaultdb",
)

# Slack webhook — set via environment variable
# export DLD_SLACK_WEBHOOK="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
# Leave empty to disable Slack notifications silently
SLACK_WEBHOOK = os.environ.get(
    "DLD_SLACK_WEBHOOK",
    "https://hooks.slack.com/services/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",
)

LOCAL_DOWNLOAD_FOLDER = "./dld_downloads"
BACKUP_SCHEMA_COUNT = 3
CHUNK_SIZE = 20000
DATE_KEYWORDS = ["at", "date", "time", "timestamp", "created", "updated"]


# ── LOGGING ───────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"logs/dld_etl_{run_timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# SLACK NOTIFICATIONS
# ─────────────────────────────────────────────────────────────


def send_slack(message: str, is_error: bool = False):
    """
    Post a message to Slack via webhook.
    Silently skips if SLACK_WEBHOOK is not set.
    Never raises — Slack failure should not affect ETL.
    """
    if not SLACK_WEBHOOK:
        return

    icon = ":x:" if is_error else ":white_check_mark:"
    payload = json.dumps({"text": f"{icon} *MIQ ETL* — {message}"}).encode()

    try:
        req = urllib.request.Request(
            SLACK_WEBHOOK,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                log.warning(f"Slack returned HTTP {resp.status}")
    except Exception as e:
        log.warning(f"Slack notification failed (non-fatal): {e}")


def slack_success(
    run_id, elapsed, rows_loaded, miq_summary, snapshot_status, snapshot_note, phases_complete
):
    total_rows = sum(rows_loaded.values())
        tables     = len(rows_loaded)

        # Tables loaded
        table_lines = [f"  {tname}: {n:,}" for tname, n in rows_loaded.items()]
        tables_block = "\n".join(table_lines) if table_lines else "  (none)"

        # MIQ component results — all functions, all tables
        miq_lines = []
        for fn, result in miq_summary.items():
            for tname, n in result.items():
                miq_lines.append(f"  {tname}: {n:,}")
        miq_block = "\n".join(miq_lines) if miq_lines else "  (no MIQ data)"

        # Phases
        phases_block = ", ".join(phases_complete) if phases_complete else "none"

        # Snapshot
        snap_line = snapshot_status or "not reached"
        if snapshot_note:
            snap_line += f" — {snapshot_note}"

        msg = (
            f"Run `{run_id}` completed successfully in {elapsed / 60:.1f} minutes\n"
            f"\n"
            f"*Phases complete:* {phases_block}\n"
            f"\n"
            f"*Tables loaded:* {tables} ({total_rows:,} total rows)\n"
            f"```\n{tables_block}\n```\n"
            f"*MIQ component results:*\n"
            f"```\n{miq_block}\n```\n"
            f"*Snapshot:* {snap_line}\n"
            f"*Log file:* `{log_filename}`"
        )
        send_slack(msg, is_error=False)


def slack_failure(
    run_id, elapsed, phases_complete, failed_downloads, failed_tables, error_message
):
    """Send a failure summary to Slack."""
    phases = ", ".join(phases_complete) if phases_complete else "none"
    lines = [
        f"Run `{run_id}` *FAILED* after {elapsed / 60:.1f} minutes",
        f"*Phases complete:* {phases}",
        f"*Error:* {error_message}",
    ]
    if failed_downloads:
        lines.append(f"*Failed downloads:* {', '.join(failed_downloads)}")
    if failed_tables:
        lines.append(f"*Failed tables:* {', '.join(failed_tables)}")

    send_slack("\n".join(lines), is_error=True)


def slack_file_size_failure(all_checks, failed_checks):
    lines = [
        "File size pre-check failed — ETL aborted before any download.",
        "",
        "*File size comparison:*",
        "```",
        f"{'File':<40} {'Now (KB)':>10} {'Prev (KB)':>10} {'Change':>8}  Status",
        "-" * 78,
    ]
    for c in all_checks:
        status = "FAIL ✗" if c["failed"] else "ok"
        lines.append(
            f"{c['file']:<40} {c['current_kb']:>10.1f} "
            f"{c['previous_kb']:>10.1f} {c['pct_change']:>+7.1f}%  {status}"
        )
    lines.append("```")
    lines.append(f"*{len(failed_checks)} file(s) failed the 10% threshold.*")
    send_slack("\n".join(lines), is_error=True)


def slack_phase_failure(phase_name, detail):
    """Send an alert for a specific phase failure."""
    send_slack(f"Phase failure in *{phase_name}*: {detail}", is_error=True)


def slack_snapshot_failure(error_message):
    """Send an alert for snapshot failure (run status unaffected)."""
    send_slack(
        f"Monthly snapshot failed (run status unaffected): {error_message}",
        is_error=True,
    )


# ─────────────────────────────────────────────────────────────
# RUN LOG TABLE
# ─────────────────────────────────────────────────────────────


def ensure_run_log_table(engine):
    """Create etl_run_log if it doesn't exist."""
    with engine.connect() as conn:
        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS etl_run_log (
                run_id              SERIAL PRIMARY KEY,
                run_started_at      TIMESTAMP NOT NULL,
                run_completed_at    TIMESTAMP,
                status              TEXT NOT NULL DEFAULT 'running',
                phases_complete     TEXT[],
                failed_downloads    TEXT[],
                failed_tables       TEXT[],
                rows_loaded         JSONB,
                miq_summary         JSONB,
                snapshot_status     TEXT,
                snapshot_note       TEXT,
                error_message       TEXT,
                log_file            TEXT
            )
        """)
        )
        conn.commit()


def start_run(engine):
    """Insert a new run record and return its run_id."""
    with engine.connect() as conn:
        result = conn.execute(
            text("""
            INSERT INTO etl_run_log (run_started_at, status, log_file)
            VALUES (:started_at, 'running', :log_file)
            RETURNING run_id
        """),
            {
                "started_at": datetime.now(),
                "log_file": log_filename,
            },
        )
        run_id = result.fetchone()[0]
        conn.commit()
    log.info(f"Run ID: {run_id}")
    return run_id


def update_run(engine, run_id, **kwargs):
    """Update any column(s) on the run record."""
    if not kwargs:
        return
    sets = ", ".join(f"{k} = :{k}" for k in kwargs)
    params = {"run_id": run_id, **kwargs}
    with engine.connect() as conn:
        conn.execute(
            text(f"UPDATE etl_run_log SET {sets} WHERE run_id = :run_id"), params
        )
        conn.commit()


def complete_run(
    engine,
    run_id,
    rows_loaded,
    miq_summary,
    snapshot_status,
    snapshot_note,
    failed_downloads,
    failed_tables,
    file_sizes=None,
):
    with engine.connect() as conn:
        conn.execute(
            text("""
            UPDATE etl_run_log SET
                run_completed_at = :completed_at,
                status           = 'complete',
                phases_complete  = :phases,
                failed_downloads = :failed_dl,
                failed_tables    = :failed_tb,
                rows_loaded      = :rows_loaded,
                miq_summary      = :miq_summary,
                snapshot_status  = :snap_status,
                snapshot_note    = :snap_note,
                file_sizes       = :file_sizes
            WHERE run_id = :run_id
        """),
            {
                "completed_at": datetime.now(),
                "phases": [
                    "download",
                    "rowcheck",
                    "backup",
                    "load",
                    "validated",
                    "miq",
                ],
                "failed_dl": failed_downloads,
                "failed_tb": failed_tables,
                "rows_loaded": json.dumps(rows_loaded),
                "miq_summary": json.dumps(miq_summary),
                "snap_status": snapshot_status,
                "snap_note": snapshot_note,
                "file_sizes": json.dumps(file_sizes) if file_sizes else None,
                "run_id": run_id,
            },
        )
        conn.commit()


def fail_run(
    engine,
    run_id,
    error_message,
    phases_complete,
    failed_downloads=None,
    failed_tables=None,
):
    """Mark run as failed."""
    with engine.connect() as conn:
        conn.execute(
            text("""
            UPDATE etl_run_log SET
                run_completed_at = :completed_at,
                status           = 'failed',
                phases_complete  = :phases,
                failed_downloads = :failed_dl,
                failed_tables    = :failed_tb,
                error_message    = :error_msg
            WHERE run_id = :run_id
        """),
            {
                "completed_at": datetime.now(),
                "phases": phases_complete,
                "failed_dl": failed_downloads or [],
                "failed_tb": failed_tables or [],
                "error_msg": error_message,
                "run_id": run_id,
            },
        )
        conn.commit()


def get_last_run_status(engine):
    """
    Returns (status, run_id) of the most recent completed or failed run.
    Returns (None, None) if no previous runs exist.
    """
    with engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT status, run_id
            FROM etl_run_log
            WHERE status IN ('complete', 'failed')
            ORDER BY run_started_at DESC
            LIMIT 1
        """)
        ).fetchone()
    if result:
        return result[0], result[1]
    return None, None


# ─────────────────────────────────────────────────────────────
# PHASE 0 — File size pre-check (runs before any download)
# ─────────────────────────────────────────────────────────────


def phase_0_file_size_check(sftp, engine):
    """
    Compares SFTP file sizes against the last successful run's sizes
    stored in etl_run_log.file_sizes. Aborts if any file is >10% smaller.

    - Uses sftp.stat() — instant, no data transfer
    - First run (no baseline) skips the check automatically
    - New files with no previous baseline are skipped individually

    Returns:
      passed        bool   — True if all checks passed
      sftp_files    list   — list of CSV filenames found on SFTP
      current_sizes dict   — {filename: size_in_bytes}
    """
    log.info("=" * 60)
    log.info("PHASE 0 — File size pre-check")
    log.info("Threshold: abort if any file is >10% smaller than last run")
    log.info("=" * 60)

    # Get all CSV filenames + sizes from SFTP — instant
    sftp_files = [f for f in sftp.listdir(SFTP_FOLDER) if f.endswith(".csv")]
    current_sizes = {}

    log.info(f"Found {len(sftp_files)} CSV files on SFTP:")
    for filename in sftp_files:
        stat = sftp.stat(f"{SFTP_FOLDER}/{filename}")
        current_sizes[filename] = stat.st_size
        log.info(f"  {filename:<45} {stat.st_size / 1024:>10.1f} KB")

    # Get last successful run's file sizes from etl_run_log
    with engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT file_sizes FROM etl_run_log
            WHERE status    = 'complete'
            AND   file_sizes IS NOT NULL
            ORDER BY run_started_at DESC
            LIMIT 1
        """)
        ).fetchone()

    if not result or not result[0]:
        log.info("No previous run baseline found — skipping file size check.")
        return True, sftp_files, current_sizes

    previous_sizes = result[0]
    log.info("")
    log.info(f"{'File':<45} {'Now (KB)':>10} {'Prev (KB)':>10} {'Change':>8}  Status")
    log.info("-" * 80)

    all_checks = []
    failed_checks = []

    for filename, current_bytes in current_sizes.items():
        prev_bytes = previous_sizes.get(filename)

        if prev_bytes is None:
            log.info(
                f"  {filename:<43} {current_bytes / 1024:>10.1f} "
                f"{'(new)':>10} {'N/A':>8}  skipped"
            )
            continue

        pct_change = ((current_bytes - prev_bytes) / prev_bytes) * 100
        failed = pct_change < -10.0
        status_str = "FAIL ✗" if failed else "ok"

        log.info(
            f"  {filename:<43} {current_bytes / 1024:>10.1f} "
            f"{prev_bytes / 1024:>10.1f} {pct_change:>+7.1f}%  {status_str}"
        )

        check = {
            "file": filename,
            "current_kb": round(current_bytes / 1024, 1),
            "previous_kb": round(prev_bytes / 1024, 1),
            "pct_change": round(pct_change, 1),
            "failed": failed,
        }
        all_checks.append(check)
        if failed:
            failed_checks.append(check)

    if failed_checks:
        log.error("")
        log.error(
            f"File size check FAILED — {len(failed_checks)} file(s) are "
            f">10% smaller than last run. Aborting before any download."
        )
        for c in failed_checks:
            log.error(
                f"  {c['file']}: {c['current_kb']} KB now vs "
                f"{c['previous_kb']} KB previously ({c['pct_change']:+.1f}%)"
            )
        slack_file_size_failure(all_checks, failed_checks)
        return False, sftp_files, current_sizes

    log.info("")
    log.info(f"File size check passed — all {len(all_checks)} files within threshold.")
    return True, sftp_files, current_sizes


# ─────────────────────────────────────────────────────────────
# PHASE 1 — Download from SFTP
# ─────────────────────────────────────────────────────────────


def phase_1_download(sftp, sftp_files):
    """
    Downloads all CSV files from SFTP to LOCAL_DOWNLOAD_FOLDER.
    Accepts an already-open sftp connection and the file list
    from phase_0 — no reconnection needed.

    Returns:
      downloaded_files  list of (filename, local_path)
      failed_downloads  list of filenames that failed
    """
    log.info("=" * 60)
    log.info("PHASE 1 — Downloading files from SFTP")
    log.info("=" * 60)

    os.makedirs(LOCAL_DOWNLOAD_FOLDER, exist_ok=True)
    downloaded_files = []
    failed_downloads = []

    for filename in sftp_files:
        sftp_path = f"{SFTP_FOLDER}/{filename}"
        local_path = os.path.join(LOCAL_DOWNLOAD_FOLDER, filename)

        try:
            log.info(f"  Downloading: {filename}")
            start = datetime.now()
            sftp.get(sftp_path, local_path)
            elapsed = (datetime.now() - start).total_seconds()
            size_mb = os.path.getsize(local_path) / (1024 * 1024)
            log.info(f"  → {size_mb:.1f} MB in {elapsed:.1f}s")
            downloaded_files.append((filename, local_path))

        except Exception as e:
            msg = f"Failed to download {filename}: {e}"
            log.error(f"  {msg}")
            failed_downloads.append(filename)
            slack_phase_failure("Download", msg)

    log.info(
        f"Downloads: {len(downloaded_files)} succeeded, {len(failed_downloads)} failed"
    )
    if failed_downloads:
        log.error(f"Failed downloads: {failed_downloads}")

    return downloaded_files, failed_downloads


def phase_1_download_v1():
    log.info("=" * 60)
    log.info("PHASE 1 — Downloading files from SFTP")
    log.info("=" * 60)

    os.makedirs(LOCAL_DOWNLOAD_FOLDER, exist_ok=True)
    downloaded_files = []
    failed_downloads = []
    ssh = None
    sftp = None

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if SFTP_KEY_PATH:
            private_key = paramiko.RSAKey.from_private_key_file(SFTP_KEY_PATH)
            ssh.connect(
                SFTP_HOST, port=SFTP_PORT, username=SFTP_USERNAME, pkey=private_key
            )
        else:
            ssh.connect(
                SFTP_HOST,
                port=SFTP_PORT,
                username=SFTP_USERNAME,
                password=SFTP_PASSWORD,
            )

        sftp = ssh.open_sftp()
        log.info(f"Connected to SFTP: {SFTP_HOST}")

        sftp_files = [f for f in sftp.listdir(SFTP_FOLDER) if f.endswith(".csv")]
        log.info(f"Found {len(sftp_files)} CSV files: {sftp_files}")

        for filename in sftp_files:
            sftp_path = f"{SFTP_FOLDER}/{filename}"
            local_path = os.path.join(LOCAL_DOWNLOAD_FOLDER, filename)
            try:
                log.info(f"  Downloading: {filename}")
                start = datetime.now()
                sftp.get(sftp_path, local_path)
                elapsed = (datetime.now() - start).total_seconds()
                size_mb = os.path.getsize(local_path) / (1024 * 1024)
                log.info(f"  → {size_mb:.1f} MB in {elapsed:.1f}s")
                downloaded_files.append((filename, local_path))
            except Exception as e:
                msg = f"Failed to download {filename}: {e}"
                log.error(f"  {msg}")
                failed_downloads.append(filename)
                slack_phase_failure("Download", msg)

    finally:
        if sftp:
            try:
                sftp.close()
            except Exception:
                pass
        if ssh:
            try:
                ssh.close()
            except Exception:
                pass
        log.info("SFTP connection closed.")

    log.info(
        f"Downloads: {len(downloaded_files)} succeeded, {len(failed_downloads)} failed"
    )
    if failed_downloads:
        log.error(f"Failed downloads: {failed_downloads}")

    return downloaded_files, failed_downloads


# ─────────────────────────────────────────────────────────────
# PHASE 2 — Backup schema rotation
# ─────────────────────────────────────────────────────────────


def phase_2_backup_rotation(engine):
    """
    Keeps last BACKUP_SCHEMA_COUNT backup schemas.
    backup_run_1 = most recent, backup_run_N = oldest.

    On each run:
      1. Drop backup_run_N (oldest)
      2. Rotate: backup_run_(N-1) → N ... backup_run_1 → 2
      3. Create backup_run_1
      4. Copy all public tables → backup_run_1
    """
    log.info("=" * 60)
    log.info("PHASE 2 — Backup schema rotation")
    log.info("=" * 60)

    n = BACKUP_SCHEMA_COUNT

    with engine.connect() as conn:
        # Drop oldest
        oldest = f"backup_run_{n}"
        log.info(f"Dropping oldest backup if exists: {oldest}")
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{oldest}" CASCADE'))
        conn.commit()

        # Rotate
        for i in range(n - 1, 0, -1):
            src = f"backup_run_{i}"
            dst = f"backup_run_{i + 1}"
            exists = conn.execute(
                text(
                    "SELECT 1 FROM information_schema.schemata WHERE schema_name = :s"
                ),
                {"s": src},
            ).fetchone()

            if exists:
                log.info(f"  Renaming {src} → {dst}")
                conn.execute(text(f'ALTER SCHEMA "{src}" RENAME TO "{dst}"'))
                conn.commit()
            else:
                log.info(f"  {src} does not exist — skipping")

        # Create fresh backup_run_1
        log.info("Creating backup_run_1")
        conn.execute(text('CREATE SCHEMA IF NOT EXISTS "backup_run_1"'))
        conn.commit()

        # Copy all public tables
        tables = conn.execute(
            text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        ).fetchall()

        if not tables:
            log.info("No tables in public schema to back up.")
        else:
            log.info(f"Backing up {len(tables)} tables to backup_run_1...")
            for (tname,) in tables:
                try:
                    conn.execute(
                        text(
                            f'CREATE TABLE "backup_run_1"."{tname}" '
                            f'AS SELECT * FROM public."{tname}"'
                        )
                    )
                    conn.commit()
                    log.info(f"  ✓ {tname}")
                except Exception as e:
                    log.warning(f"  Could not back up {tname}: {e}")
                    conn.rollback()

    log.info("Backup rotation complete.")


# ─────────────────────────────────────────────────────────────
# PHASE 3 — Load CSVs into Postgres
# ─────────────────────────────────────────────────────────────


def phase_3_load(engine, downloaded_files):
    log.info("=" * 60)
    log.info("PHASE 3 — Loading files into database")
    log.info("=" * 60)

    successful_tables = []
    failed_tables = []
    rows_loaded = {}

    for filename, local_path in downloaded_files:
        table_name = filename.replace(".csv", "").lower()
        first_chunk = True
        total_rows = 0

        log.info(f"Processing: {filename} → {table_name}")
        start = datetime.now()

        try:
            for chunk_num, chunk in enumerate(
                pd.read_csv(local_path, chunksize=CHUNK_SIZE, dtype=str), start=1
            ):
                dtype_map = {}
                for col in chunk.columns:
                    if any(kw in col.lower() for kw in DATE_KEYWORDS):
                        chunk[col] = pd.to_datetime(chunk[col], errors="coerce")
                        dtype_map[col] = TIMESTAMP(precision=6)
                    else:
                        dtype_map[col] = types.Text()

                chunk.columns = [c.lower() for c in chunk.columns]

                if first_chunk:
                    with engine.connect() as conn:
                        conn.execute(
                            text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
                        )
                        conn.commit()
                    log.info(f"  Dropped existing table: {table_name}")
                    mode = "replace"
                else:
                    mode = "append"

                chunk.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists=mode,
                    index=False,
                    dtype=dtype_map,
                )

                total_rows += len(chunk)
                first_chunk = False

                if chunk_num % 10 == 0:
                    log.info(f"  Chunk {chunk_num} — {total_rows:,} rows so far")

            elapsed = (datetime.now() - start).total_seconds()
            log.info(f"  ✓ {table_name} — {total_rows:,} rows in {elapsed:.1f}s")
            successful_tables.append(table_name)
            rows_loaded[table_name] = total_rows

        except Exception as e:
            msg = f"Failed to load {table_name}: {e}"
            log.error(f"  {msg}")
            failed_tables.append(table_name)
            slack_phase_failure("Load", msg)

        finally:
            try:
                del chunk
            except NameError:
                pass
            gc.collect()

    log.info(
        f"Load complete — {len(successful_tables)} succeeded, "
        f"{len(failed_tables)} failed"
    )
    if failed_tables:
        log.error(f"Failed tables: {failed_tables}")

    return successful_tables, failed_tables, rows_loaded


# ─────────────────────────────────────────────────────────────
# PHASE 4 — Refresh validated tables
# ─────────────────────────────────────────────────────────────


def phase_4_validated_tables(engine):
    log.info("=" * 60)
    log.info("PHASE 4 — Refreshing validated tables")
    log.info("=" * 60)

    with engine.connect() as conn:
        log.info("Calling refresh_validated_tables()...")
        start = datetime.now()
        conn.execute(text("CALL refresh_validated_tables()"))
        conn.commit()
        elapsed = (datetime.now() - start).total_seconds()
        log.info(f"✓ refresh_validated_tables() completed in {elapsed:.1f}s")


# ─────────────────────────────────────────────────────────────
# PHASE 5 — Run MIQ component functions
# ─────────────────────────────────────────────────────────────


def phase_5_miq_components(engine):
    log.info("=" * 60)
    log.info("PHASE 5 — Running MIQ component functions")
    log.info("=" * 60)

    functions = [
        ("run_miq_cap_app", "Capital Appreciation"),
        ("run_miq_occupancy", "Occupancy Rate"),
        ("run_miq_resale", "Resale Index"),
        ("run_miq_roi", "Rental ROI"),
        ("run_miq_score", "MIQ Score"),
    ]

    miq_summary = {}

    for fn_name, label in functions:
        log.info(f"Running {label} ({fn_name})...")
        start = datetime.now()
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(f"SELECT * FROM {fn_name}()")).fetchall()
                conn.commit()

            elapsed = (datetime.now() - start).total_seconds()
            fn_result = {}

            for row in rows:
                fn_result[row[0]] = row[1]
                if fn_name == "run_miq_score":
                    log.info(
                        f"  {row[0]}: total={row[1]:,} scored={row[2]:,} "
                        f"avg={row[3]} full={row[4]:,} fallback={row[5]:,} "
                        f"offplan={row[6]:,} insufficient={row[7]:,}"
                    )
                else:
                    log.info(f"  {row[0]}: {row[1]:,} rows")

            log.info(f"  ✓ {label} complete in {elapsed:.1f}s")
            miq_summary[fn_name] = fn_result

        except Exception as e:
            msg = f"{label} ({fn_name}) failed: {e}"
            log.error(f"  {msg}")
            slack_phase_failure("MIQ Components", msg)
            raise

    return miq_summary


# ─────────────────────────────────────────────────────────────
# PHASE 6 — Monthly snapshot (non-fatal)
# ─────────────────────────────────────────────────────────────


def phase_6_snapshot(engine):
    log.info("=" * 60)
    log.info("PHASE 6 — Monthly snapshot")
    log.info("=" * 60)

    today = date.today()

    if today.day != 1:
        msg = (
            f"Today is the {today.day}th — "
            f"snapshot runs on 1st of month only. Skipping."
        )
        log.info(msg)
        return "skipped", msg

    log.info(f"Today is the 1st — running snapshot for {today}...")
    start = datetime.now()

    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM run_miq_snapshot()")).fetchall()
            conn.commit()

        elapsed = (datetime.now() - start).total_seconds()
        for row in rows:
            log.info(f"  {row[0]}: {row[1]:,} rows inserted")
        log.info(f"✓ Snapshot complete in {elapsed:.1f}s")
        return "complete", None

    except Exception as e:
        note = str(e)
        log.error(f"Snapshot FAILED: {note}")
        log.warning(
            "Snapshot failed — run status is unaffected. "
            "Trigger run_miq_snapshot() manually to retry."
        )
        slack_snapshot_failure(note)
        return "failed", note


# ─────────────────────────────────────────────────────────────
# SUMMARY LOG
# ─────────────────────────────────────────────────────────────


def log_summary(
    run_id,
    status,
    phases_complete,
    failed_downloads,
    failed_tables,
    rows_loaded,
    miq_summary,
    snapshot_status,
    snapshot_note,
    overall_elapsed,
):
    log.info("")
    log.info("=" * 60)
    log.info("ETL RUN SUMMARY")
    log.info("=" * 60)
    log.info(f"  Run ID:          {run_id}")
    log.info(f"  Status:          {status.upper()}")
    log.info(f"  Total runtime:   {overall_elapsed:.1f}s")
    log.info(
        f"  Phases complete: "
        f"{', '.join(phases_complete) if phases_complete else 'none'}"
    )
    log.info("")

    if failed_downloads:
        log.error(f"  Failed downloads ({len(failed_downloads)}):")
        for f in failed_downloads:
            log.error(f"    - {f}")

    if failed_tables:
        log.error(f"  Failed table loads ({len(failed_tables)}):")
        for t in failed_tables:
            log.error(f"    - {t}")

    if rows_loaded:
        log.info(f"  Tables loaded ({len(rows_loaded)}):")
        for tname, n in rows_loaded.items():
            log.info(f"    {tname}: {n:,} rows")

    if miq_summary:
        log.info("")
        log.info("  MIQ component results:")
        for fn, result in miq_summary.items():
            for tname, n in result.items():
                log.info(f"    {tname}: {n:,} rows")

    log.info("")
    log.info(
        f"  Snapshot: {snapshot_status or 'not reached'}"
        + (f" — {snapshot_note}" if snapshot_note else "")
    )
    log.info(f"  Log file: {log_filename}")
    log.info("=" * 60)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────


def main():
    log.info("=" * 60)
    log.info("DLD END-TO-END ETL STARTED")
    log.info(f"Run date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    overall_start = datetime.now()
    engine = create_engine(PG_URL)
    phases_complete = []
    failed_downloads = []
    failed_tables = []
    rows_loaded = {}
    miq_summary = {}
    snapshot_status = None
    snapshot_note = None
    current_sizes = {}
    run_id = None
    ssh = None
    sftp = None

    try:
        ensure_run_log_table(engine)
        run_id = start_run(engine)

        last_status, last_run_id = get_last_run_status(engine)
        log.info(
            f"Last run status: {last_status or 'no previous runs'}"
            + (f" (run_id={last_run_id})" if last_run_id else "")
        )

        # ── Connect to SFTP once — shared across phase 0 and phase 1 ──
        log.info("Connecting to SFTP...")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if SFTP_KEY_PATH:
            pkey = paramiko.RSAKey.from_private_key_file(SFTP_KEY_PATH)
            ssh.connect(SFTP_HOST, port=SFTP_PORT, username=SFTP_USERNAME, pkey=pkey)
        else:
            ssh.connect(
                SFTP_HOST,
                port=SFTP_PORT,
                username=SFTP_USERNAME,
                password=SFTP_PASSWORD,
            )
        sftp = ssh.open_sftp()
        log.info(f"Connected to SFTP: {SFTP_HOST}")

        # ── Phase 0: File size pre-check ───────────────────────
        passed, sftp_files, current_sizes = phase_0_file_size_check(sftp, engine)

        if not passed:
            raise RuntimeError(
                "File size pre-check failed — one or more files are >10% "
                "smaller than the last run. See Slack for details. "
                "No data has been downloaded or loaded."
            )

        phases_complete.append("filecheck")
        update_run(engine, run_id, phases_complete=phases_complete)

        # ── Phase 1: Download ──────────────────────────────────
        downloaded_files, failed_downloads = phase_1_download(sftp, sftp_files)

        # Close SFTP — no longer needed after download
        sftp.close()
        ssh.close()
        sftp = None
        ssh = None
        log.info("SFTP connection closed.")

        if failed_downloads:
            raise RuntimeError(
                f"Phase 1 incomplete — {len(failed_downloads)} file(s) "
                f"failed to download: {failed_downloads}"
            )

        if not downloaded_files:
            raise RuntimeError("No files downloaded — nothing to load.")

        phases_complete.append("download")
        update_run(engine, run_id, phases_complete=phases_complete)

        # ── Phase 2: Backup rotation ───────────────────────────
        if last_status is None:
            log.info("No previous runs — skipping backup rotation.")
        elif last_status != "complete":
            log.warning(
                f"Last run (run_id={last_run_id}) status is '{last_status}' "
                f"— skipping backup rotation to avoid backing up a broken state."
            )
        else:
            phase_2_backup_rotation(engine)

        phases_complete.append("backup")
        update_run(engine, run_id, phases_complete=phases_complete)

        # ── Phase 3: Load ──────────────────────────────────────
        successful_tables, failed_tables, rows_loaded = phase_3_load(
            engine, downloaded_files
        )

        if failed_tables:
            raise RuntimeError(
                f"Phase 3 incomplete — {len(failed_tables)} table(s) "
                f"failed to load: {failed_tables}"
            )

        phases_complete.append("load")
        update_run(
            engine,
            run_id,
            phases_complete=phases_complete,
            rows_loaded=json.dumps(rows_loaded),
        )

        # ── Phase 4: Validated tables ──────────────────────────
        phase_4_validated_tables(engine)
        phases_complete.append("validated")
        update_run(engine, run_id, phases_complete=phases_complete)

        # ── Phase 5: MIQ components ────────────────────────────
        miq_summary = phase_5_miq_components(engine)
        phases_complete.append("miq")
        update_run(
            engine,
            run_id,
            phases_complete=phases_complete,
            miq_summary=json.dumps(miq_summary),
        )

        # ── Phase 6: Snapshot (non-fatal) ──────────────────────
        snapshot_status, snapshot_note = phase_6_snapshot(engine)

        # ── Mark run complete (store file sizes for next run) ──
        complete_run(
            engine,
            run_id,
            rows_loaded=rows_loaded,
            miq_summary=miq_summary,
            snapshot_status=snapshot_status,
            snapshot_note=snapshot_note,
            failed_downloads=failed_downloads,
            failed_tables=failed_tables,
            file_sizes=current_sizes,
        )

        overall_elapsed = (datetime.now() - overall_start).total_seconds()
        log_summary(
            run_id,
            "complete",
            phases_complete,
            failed_downloads,
            failed_tables,
            rows_loaded,
            miq_summary,
            snapshot_status,
            snapshot_note,
            overall_elapsed,
        )

        slack_success(
            run_id,
            overall_elapsed,
            rows_loaded,
            miq_summary,
            snapshot_status,
            snapshot_note,
            phases_complete
        )

    except Exception as e:
        overall_elapsed = (datetime.now() - overall_start).total_seconds()
        error_message = str(e)
        log.error(f"ETL FAILED: {error_message}")

        if run_id:
            fail_run(
                engine,
                run_id,
                error_message=error_message,
                phases_complete=phases_complete,
                failed_downloads=failed_downloads,
                failed_tables=failed_tables,
            )

        log_summary(
            run_id,
            "failed",
            phases_complete,
            failed_downloads,
            failed_tables,
            rows_loaded,
            miq_summary,
            snapshot_status,
            snapshot_note,
            overall_elapsed,
        )
        slack_failure(
            run_id,
            overall_elapsed,
            phases_complete,
            failed_downloads,
            failed_tables,
            error_message,
        )
        raise

    finally:
        # Ensure SFTP is always closed even if an error occurs mid-download
        if sftp:
            try:
                sftp.close()
            except Exception:
                pass
        if ssh:
            try:
                ssh.close()
            except Exception:
                pass
        engine.dispose()


def main_v1():
    log.info("=" * 60)
    log.info("DLD END-TO-END ETL STARTED")
    log.info(f"Run date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    overall_start = datetime.now()
    engine = create_engine(
        PG_URL, connect_args={"sslmode": "require", "sslrootcert": "ca.crt"}
    )
    phases_complete = []
    failed_downloads = []
    failed_tables = []
    rows_loaded = {}
    miq_summary = {}
    snapshot_status = None
    snapshot_note = None
    run_id = None

    try:
        # Set up run log table and register this run
        ensure_run_log_table(engine)
        run_id = start_run(engine)

        # Check last run status
        last_status, last_run_id = get_last_run_status(engine)
        log.info(
            f"Last run status: {last_status or 'no previous runs'}"
            + (f" (run_id={last_run_id})" if last_run_id else "")
        )

        # ── Phase 1: Download ──────────────────────────────────
        downloaded_files, failed_downloads = phase_1_download()

        if failed_downloads:
            raise RuntimeError(
                f"Phase 1 incomplete — {len(failed_downloads)} file(s) "
                f"failed to download: {failed_downloads}"
            )

        phases_complete.append("download")
        update_run(engine, run_id, phases_complete=phases_complete)

        if not downloaded_files:
            raise RuntimeError("No files downloaded — nothing to load.")

        # ── Phase 2: Backup rotation ───────────────────────────
        if last_status is None:
            log.info("No previous runs — skipping backup rotation.")
        elif last_status != "complete":
            log.warning(
                f"Last run (run_id={last_run_id}) status is '{last_status}' "
                f"— skipping backup rotation to avoid backing up a broken state."
            )
        else:
            phase_2_backup_rotation(engine)

        phases_complete.append("backup")
        update_run(engine, run_id, phases_complete=phases_complete)

        # ── Phase 3: Load ──────────────────────────────────────
        successful_tables, failed_tables, rows_loaded = phase_3_load(
            engine, downloaded_files
        )

        if failed_tables:
            raise RuntimeError(
                f"Phase 3 incomplete — {len(failed_tables)} table(s) "
                f"failed to load: {failed_tables}"
            )

        phases_complete.append("load")
        update_run(
            engine,
            run_id,
            phases_complete=phases_complete,
            rows_loaded=json.dumps(rows_loaded),
        )

        # ── Phase 4: Validated tables ──────────────────────────
        phase_4_validated_tables(engine)
        phases_complete.append("validated")
        update_run(engine, run_id, phases_complete=phases_complete)

        # ── Phase 5: MIQ components ────────────────────────────
        miq_summary = phase_5_miq_components(engine)
        phases_complete.append("miq")
        update_run(
            engine,
            run_id,
            phases_complete=phases_complete,
            miq_summary=json.dumps(miq_summary),
        )

        # ── Phase 6: Snapshot (non-fatal) ──────────────────────
        snapshot_status, snapshot_note = phase_6_snapshot(engine)

        # ── Mark run complete ──────────────────────────────────
        complete_run(
            engine,
            run_id,
            rows_loaded=rows_loaded,
            miq_summary=miq_summary,
            snapshot_status=snapshot_status,
            snapshot_note=snapshot_note,
            failed_downloads=failed_downloads,
            failed_tables=failed_tables,
        )

        overall_elapsed = (datetime.now() - overall_start).total_seconds()

        log_summary(
            run_id,
            "complete",
            phases_complete,
            failed_downloads,
            failed_tables,
            rows_loaded,
            miq_summary,
            snapshot_status,
            snapshot_note,
            overall_elapsed,
        )

        # ── Slack: success ─────────────────────────────────────
        slack_success(run_id, overall_elapsed, rows_loaded, miq_summary)

    except Exception as e:
        overall_elapsed = (datetime.now() - overall_start).total_seconds()
        error_message = str(e)
        log.error(f"ETL FAILED: {error_message}")

        if run_id:
            fail_run(
                engine,
                run_id,
                error_message=error_message,
                phases_complete=phases_complete,
                failed_downloads=failed_downloads,
                failed_tables=failed_tables,
            )

        log_summary(
            run_id,
            "failed",
            phases_complete,
            failed_downloads,
            failed_tables,
            rows_loaded,
            miq_summary,
            snapshot_status,
            snapshot_note,
            overall_elapsed,
        )

        # ── Slack: failure ─────────────────────────────────────
        slack_failure(
            run_id,
            overall_elapsed,
            phases_complete,
            failed_downloads,
            failed_tables,
            error_message,
        )

        raise

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
