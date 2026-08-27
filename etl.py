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

import json
import logging
import os
from datetime import datetime

# from pathlib import Path
import paramiko

# from dotenv import load_dotenv
from functions.config import _config
from functions.etl_phases.phases import (
    phase_0_file_size_check,
    phase_1_download,
    phase_2_backup_rotation,
    phase_3_load,
    phase_4_validated_tables,
    phase_4b_enrich_area_names,
    phase_5_miq_components,
    phase_5b_showcase_categories,
    phase_5c_entity_lookup_refresh,
    phase_6_snapshot,
    phase_7_build_transactions_cache,
)
from functions.helpers.etl_logger import (
    complete_run,
    ensure_run_log_table,
    fail_run,
    get_last_run_status,
    log_summary,
    start_run,
    update_run,
)
from functions.helpers.slack_helper import (
    slack_failure,
    slack_success,
)
from sqlalchemy import create_engine

# BASE_DIR = Path(__file__).resolve().parent
# ENV_PATH = BASE_DIR / ".env"

# load_dotenv(dotenv_path=ENV_PATH)

SFTP_HOST = _config.SFTP_HOST
SFTP_PORT = _config.SFTP_PORT
SFTP_USERNAME = _config.SFTP_USERNAME
SFTP_PASSWORD = _config.SFTP_PASSWORD
SFTP_KEY_PATH = _config.SFTP_KEY_PATH
SFTP_FOLDER = _config.SFTP_FOLDER

PG_URL = _config.PG_URL

SLACK_WEBHOOK = _config.SLACK_WEBHOOK

LOCAL_DOWNLOAD_FOLDER = _config.LOCAL_DOWNLOAD_FOLDER
BACKUP_SCHEMA_COUNT = _config.BACKUP_SCHEMA_COUNT
CHUNK_SIZE = _config.CHUNK_SIZE
DATE_KEYWORDS = _config.DATE_KEYWORDS


# ── LOGGING ───────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"logs/miq_etl_{run_timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# ── ETL FUNCTION ───────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("MIQ ETL STARTED")
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
        run_id = start_run(engine, log_filename, log)

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
        passed, sftp_files, current_sizes = phase_0_file_size_check(sftp, engine, log)

        if not passed:
            raise RuntimeError(
                "File size pre-check failed — one or more files are >10% "
                "smaller than the last run. See Slack for details. "
                "No data has been downloaded or loaded."
            )

        phases_complete.append("filecheck")
        update_run(engine, run_id, phases_complete=phases_complete)

        # ── Phase 1: Download ──────────────────────────────────
        downloaded_files, failed_downloads = phase_1_download(sftp, sftp_files, log)

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
            phase_2_backup_rotation(engine, log)

        phases_complete.append("backup")
        update_run(engine, run_id, phases_complete=phases_complete)

        # ── Phase 3: Load ──────────────────────────────────────
        successful_tables, failed_tables, rows_loaded = phase_3_load(
            engine, downloaded_files, log
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
        phase_4_validated_tables(engine, log)
        phases_complete.append("validated")
        update_run(engine, run_id, phases_complete=phases_complete)

        # ── Phase 4b: Enrich area names ────────────────────────
        phase_4b_enrich_area_names(engine, log)
        phases_complete.append("area_enrichment")
        update_run(engine, run_id, phases_complete=phases_complete)

        # ── Phase 5: MIQ components ────────────────────────────
        miq_summary = phase_5_miq_components(engine, log)
        phases_complete.append("miq")
        update_run(
            engine,
            run_id,
            phases_complete=phases_complete,
            miq_summary=json.dumps(miq_summary),
        )

        # ── Phase 5b: Showcase Categories ───────────────────
        phase_5b_showcase_categories(engine, log)
        phases_complete.append("miq showcase categories")
        update_run(engine, run_id, phases_complete=phases_complete)

        # ── Phase 5c: Entity lookup refresh ───────────────────
        entity_lookup_summary = phase_5c_entity_lookup_refresh(engine, log)
        miq_summary.update(entity_lookup_summary)
        phases_complete.append("entity_lookup_refresh")
        update_run(
            engine,
            run_id,
            phases_complete=phases_complete,
            miq_summary=json.dumps(miq_summary),
        )

        # ── Phase 6: Snapshot (non-fatal) ──────────────────────
        snapshot_status, snapshot_note = phase_6_snapshot(engine, log)

        # __ Phase 7: Building transaction Cache
        transaction_cache = phase_7_build_transactions_cache(engine, log)

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
            log,
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
            log_filename,
        )

        slack_success(
            run_id,
            overall_elapsed,
            rows_loaded,
            miq_summary,
            snapshot_status,
            snapshot_note,
            phases_complete,
            log_filename,
            log,
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
            log,
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
            log_filename,
        )
        slack_failure(
            run_id,
            overall_elapsed,
            phases_complete,
            failed_downloads,
            failed_tables,
            error_message,
            log_filename,
            log,
        )
        raise

    finally:
        # Ensure SFTP and Database Connection are always closed even if an error occurs
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


if __name__ == "__main__":
    main()
