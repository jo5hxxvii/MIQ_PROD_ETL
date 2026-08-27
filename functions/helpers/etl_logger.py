import json
from datetime import datetime

from sqlalchemy import text

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


def start_run(engine, log_filename, log):
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
# SUMMARY LOG
# ─────────────────────────────────────────────────────────────


def log_summary(
    log,
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
    log_filename,
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
                val = f"{n:,}" if isinstance(n, int) else str(n)
                log.info(f"    {tname}: {val} rows")

    # if miq_summary:
    #     log.info("")
    #     log.info("  MIQ component results:")
    #     for fn, result in miq_summary.items():
    #         for tname, n in result.items():
    #             log.info(f"    {tname}: {n:,} rows")

    log.info("")
    log.info(
        f"  Snapshot: {snapshot_status or 'not reached'}"
        + (f" — {snapshot_note}" if snapshot_note else "")
    )
    log.info(f"  Log file: {log_filename}")
    log.info("=" * 60)
