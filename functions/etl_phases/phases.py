import gc
import json
import os
import threading
import time
from datetime import date, datetime

import pandas as pd
from functions.config import _config
from functions.helpers.location_enquiry import generate_location_report_openai as le
from functions.helpers.slack_helper import (
    slack_file_size_failure,
    slack_phase_failure,
    slack_snapshot_failure,
)
from sqlalchemy import text, types
from sqlalchemy.dialects.postgresql import TIMESTAMP

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
AREA_NICKNAMES_PATH = _config.AREA_NICKNAMES_PATH
MAX_RETRIES = 3
RETRY_DELAY = 600  # 10 minutes
BLOCKING_CLIENT_IP = _config.BLOCKING_CLIENT_IP


def _is_deadlock(e):
    return "deadlock" in str(e).lower()


def _query_killer(stop_event, log, interval=0.5, engine_url=PG_URL):
    """
    Background thread — runs for the duration of phase_4_validated_tables.
    Kills leader backends from BLOCKING_CLIENT_IP querying validated_* tables.
    """
    from sqlalchemy import create_engine

    killer_engine = create_engine(engine_url)
    all_killed_pids = []

    while not stop_event.is_set():
        try:
            with killer_engine.connect() as kconn:
                rows = kconn.execute(
                    text("""
                    SELECT pid, pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE (
                        query ILIKE '%actual_worth%'
                        OR query ILIKE '%validated_properties%'
                        OR query ILIKE '%validated_transactions%'
                        OR query ILIKE '%validated_tenancy%'
                    )
                    AND pid        != pg_backend_pid()
                    AND leader_pid IS NULL
                    AND client_addr = :ip
                """),
                    {"ip": BLOCKING_CLIENT_IP},
                ).fetchall()

                killed = [r[0] for r in rows if r[1]]
                if killed:
                    all_killed_pids.extend(killed)

        except Exception as e:
            log.warning(f"Killer thread error (non-fatal): {e}")

        time.sleep(interval)

    killer_engine.dispose()
    log.info(f"Killer thread stopped — {len(all_killed_pids)} leader pid(s)")


# ─────────────────────────────────────────────────────────────
# PHASE 0 — File size pre-check (runs before any download)
# ─────────────────────────────────────────────────────────────


def phase_0_file_size_check(sftp, engine, log):
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
        slack_file_size_failure(all_checks, failed_checks, log)
        return False, sftp_files, current_sizes

    log.info("")
    log.info(f"File size check passed — all {len(all_checks)} files within threshold.")
    return True, sftp_files, current_sizes


# ─────────────────────────────────────────────────────────────
# PHASE 1 — Download from SFTP
# ─────────────────────────────────────────────────────────────


def phase_1_download(sftp, sftp_files, log):
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
            slack_phase_failure("Download", msg, log)

    log.info(
        f"Downloads: {len(downloaded_files)} succeeded, {len(failed_downloads)} failed"
    )
    if failed_downloads:
        log.error(f"Failed downloads: {failed_downloads}")

    return downloaded_files, failed_downloads


# ─────────────────────────────────────────────────────────────
# PHASE 2 — Backup schema rotation
# ─────────────────────────────────────────────────────────────


def phase_2_backup_rotation(engine, log):
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


# ──────────
# ───────────────────────────────────────────────────
# PHASE 3 — Load CSVs into Postgres
# ─────────────────────────────────────────────────────────────


def phase_3_load(engine, downloaded_files, log):
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
            slack_phase_failure("Load", msg, log)

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


# ──────────
# # ───────────────────────────────────────────────────
# # PHASE 4 — Refresh validated tables & Data Enrichment
# # ─────────────────────────────────────────────────────────────


# def phase_4_validated_tables(engine, log):
#     log.info("=" * 60)
#     log.info("PHASE 4 — Refreshing validated tables")
#     log.info("=" * 60)

#     with engine.connect() as conn:
#         log.info("Calling refresh_validated_tables()...")
#         start = datetime.now()
#         conn.execute(text("CALL refresh_validated_tables()"))
#         conn.commit()
#         elapsed = (datetime.now() - start).total_seconds()
#         log.info(f"✓ refresh_validated_tables() completed in {elapsed:.1f}s")


# def phase_4b_enrich_area_names(engine, log):
#     log.info("=" * 60)
#     log.info("PHASE 4b — Enriching area names")
#     log.info("=" * 60)

#     start = datetime.now()

#     if not os.path.exists(AREA_NICKNAMES_PATH):
#         log.warning(
#             f"Area nicknames file not found at {AREA_NICKNAMES_PATH} — skipping."
#         )
#         return

#     with open(AREA_NICKNAMES_PATH, "r", encoding="utf-8") as f:
#         areas = json.load(f)

#     log.info(f"Loaded {len(areas)} area entries from nicknames file.")

#     updated_by_id = 0
#     updated_by_name = 0
#     skipped = 0
#     not_found = 0

#     with engine.connect() as conn:
#         for area in areas:
#             area_id = area.get("AREA_ID")
#             name_en = (area.get("NAME_EN") or "").strip()
#             nickname = (area.get("nicknames") or "").strip()
#             shortname = (area.get("shortnames") or "").strip()

#             if not nickname and not shortname:
#                 skipped += 1
#                 continue

#             if nickname and shortname:
#                 suffix = f"({nickname} - {shortname})"
#             elif nickname:
#                 suffix = f"({nickname})"
#             else:
#                 suffix = f"({shortname})"

#             try:
#                 result = conn.execute(
#                     text("""
#                     UPDATE areas
#                     SET name_en = name_en || ' ' || :suffix
#                     WHERE area_id = :area_id
#                     AND   name_en NOT LIKE :suffix_check
#                 """),
#                     {
#                         "area_id": str(area_id),
#                         "suffix": suffix,
#                         "suffix_check": f"%{suffix}",
#                     },
#                 )
#                 conn.commit()

#                 if result.rowcount > 0:
#                     updated_by_id += 1
#                     continue

#                 if not name_en:
#                     not_found += 1
#                     continue

#                 result = conn.execute(
#                     text("""
#                     UPDATE areas
#                     SET name_en = name_en || ' ' || :suffix
#                     WHERE name_en = :name_en
#                     AND   name_en NOT LIKE :suffix_check
#                 """),
#                     {
#                         "name_en": name_en,
#                         "suffix": suffix,
#                         "suffix_check": f"%{suffix}",
#                     },
#                 )
#                 conn.commit()

#                 if result.rowcount > 0:
#                     updated_by_name += 1
#                 else:
#                     not_found += 1

#             except Exception as e:
#                 conn.rollback()

#     elapsed = (datetime.now() - start).total_seconds()
#     log.info(
#         f"✓ Area name enrichment complete in {elapsed:.1f}s — "
#         f"{updated_by_id} updated by ID, "
#         f"{updated_by_name} updated by name, "
#         f"{skipped} skipped (no nickname/shortname), "
#         f"{not_found} not found in DB."
#     )


# def phase_4c_get_location_amenities(engine, log):
#     log.info("=" * 60)
#     log.info("PHASE 4c — Loading location amenities")
#     log.info("=" * 60)
#     start = datetime.now()

#     CATEGORY_MAP = {
#         "lm": "landmarks",
#         "sch": "schools",
#         "hc": "healthcare",
#         "sup": "supermarkets",
#         "mal": "malls",
#         "res": "restaurants",
#         "pk": "parks",
#     }

#     # --- 1. Pull buildings and projects from DB ---
#     with engine.connect() as conn:
#         buildings = (
#             conn.execute(
#                 text("""
#             SELECT distinct entity_id, 'building'   AS entity_type, b.entity_name || ', ' || a.name_en AS search_query, b.area_id
#             FROM miq_miqs_building b
#             LEFT JOIN areas a ON a.area_id = b.area_id
#             WHERE b.entity_name IS NOT NULL
#         """)
#             )
#             .mappings()
#             .all()
#         )

#         projects = (
#             conn.execute(
#                 text("""
#             SELECT distinct entity_id, 'project'   AS entity_type, b.entity_name || ', ' || a.name_en AS search_query, b.area_id
#             FROM miq_miqs_project b
#             LEFT JOIN areas a ON a.area_id = b.area_id
#             WHERE b.entity_name IS NOT NULL
#         """)
#             )
#             .mappings()
#             .all()
#         )

#     entities = [dict(r) for r in buildings] + [dict(r) for r in projects]
#     log.info(f"Loaded {len(buildings)} buildings and {len(projects)} projects from DB")

#     # --- 2. Fetch all amenity rows first, before touching the DB ---
#     amenity_rows = []
#     transport_rows = []
#     skipped = 0
#     fetched = 0

#     for entity in entities:
#         report, response = le(entity["search_query"])
#         if "error" in report:
#             log.warning(f"Skipping {entity['search_query']}: {report['error']}")
#             skipped += 1
#             continue

#         base = {
#             "entity_id": entity["entity_id"],
#             "entity_type": entity["entity_type"],
#             "area_id": entity["area_id"],
#             "location_name": report.get("loc"),
#             "area": report.get("ar"),
#             "city": report.get("ct"),
#         }

#         # Amenity categories
#         for key, category_name in CATEGORY_MAP.items():
#             for item in report.get(key, []):
#                 amenity_rows.append(
#                     {
#                         **base,
#                         "category": category_name,
#                         "amenity_name": item.get("n", None),
#                         "amenity_type": item.get("t", None),
#                         "distance_km": item.get("d", None),
#                     }
#                 )

#         # Transport — one row per road + metro flag
#         tr = report.get("tr", {})
#         for road in tr.get("rd", []):
#             transport_rows.append(
#                 {
#                     **base,
#                     "road_name": road,
#                     "metro_access": tr.get("mt", False),
#                 }
#             )

#         fetched += 1
#         if fetched % 50 == 0:
#             log.info(f"  Fetched {fetched}/{len(entities)} entities...")

#     log.info(
#         f"Fetch complete — {fetched} entities, {skipped} skipped, "
#         f"{len(amenity_rows)} amenity rows, {len(transport_rows)} transport rows"
#     )

#     if not amenity_rows and not transport_rows:
#         log.warning("No rows to load — aborting truncate to preserve existing data")
#         return

#     # --- 3. Truncate and load atomically ---
#     try:
#         with engine.connect() as conn:
#             conn.execute(text("TRUNCATE TABLE location_amenities"))
#             conn.execute(text("TRUNCATE TABLE location_transport"))
#             log.info("Tables truncated")

#             if amenity_rows:
#                 conn.execute(
#                     text("""
#                         INSERT INTO location_amenities
#                             (entity_id, entity_type, area_id, location_name, area, city,
#                              category, amenity_name, amenity_type, distance_km)
#                         VALUES
#                             (:entity_id, :entity_type, :area_id, :location_name, :area, :city,
#                              :category, :amenity_name, :amenity_type, :distance_km)
#                     """),
#                     amenity_rows,
#                 )

#             if transport_rows:
#                 conn.execute(
#                     text("""
#                         INSERT INTO location_transport
#                             (entity_id, entity_type, area_id, location_name, area, city,
#                              road_name, metro_access)
#                         VALUES
#                             (:entity_id, :entity_type, :area_id, :location_name, :area, :city,
#                              :road_name, :metro_access)
#                     """),
#                     transport_rows,
#                 )

#             conn.commit()

#     except Exception as e:
#         log.error(f"Load failed — rolling back: {e}")
#         raise

#     elapsed = (datetime.now() - start).total_seconds()
#     log.info(
#         f"✓ Location amenities load complete in {elapsed:.1f}s — "
#         f"{len(amenity_rows)} amenity rows + {len(transport_rows)} transport rows "
#         f"across {fetched} entities, {skipped} skipped"
#     )


# # ─────────────────────────────────────────────────────────────
# # PHASE 5 — Run MIQ component functions
# # ─────────────────────────────────────────────────────────────


# def phase_5_miq_components(engine, log):
#     log.info("=" * 60)
#     log.info("PHASE 5 — Running MIQ component functions")
#     log.info("=" * 60)

#     functions = [
#         ("run_miq_cap_app", "Capital Appreciation"),
#         ("run_miq_occupancy", "Occupancy Rate"),
#         ("run_miq_resale", "Resale Index"),
#         ("run_miq_roi", "Rental ROI"),
#         ("run_miq_score", "MIQ Score"),
#         ("run_miq_score_agg", "MIQ Score Agg"),
#     ]

#     miq_summary = {}

#     for fn_name, label in functions:
#         log.info(f"Running {label} ({fn_name})...")
#         start = datetime.now()
#         try:
#             with engine.connect() as conn:
#                 rows = conn.execute(text(f"SELECT * FROM {fn_name}()")).fetchall()
#                 conn.commit()

#             elapsed = (datetime.now() - start).total_seconds()
#             fn_result = {}

#             for row in rows:
#                 fn_result[row[0]] = row[1]
#                 if fn_name == "run_miq_score":
#                     log.info(
#                         f"  {row[0]}: total={row[1]:,} scored={row[2]:,} "
#                         f"avg={row[3]} full={row[4]:,} fallback={row[5]:,} "
#                         f"offplan={row[6]:,} insufficient={row[7]:,}"
#                     )
#                 elif fn_name == "run_miq_score_agg":
#                     log.info(
#                         f"  {row[0]} [{row[1]}]: inserted={row[2]:,} "
#                         f"scored={row[3]:,} avg_score={row[4]}"
#                     )
#                 else:
#                     log.info(f"  {row[0]}: {row[1]:,} rows")

#             log.info(f"  ✓ {label} complete in {elapsed:.1f}s")
#             miq_summary[fn_name] = fn_result

#         except Exception as e:
#             msg = f"{label} ({fn_name}) failed: {e}"
#             log.error(f"  {msg}")
#             slack_phase_failure("MIQ Components", msg, log)
#             raise

#     return miq_summary


# def phase_5b_showcase_categories(engine, log):
#     log.info("=" * 60)
#     log.info("PHASE 5B — Running MIQ Showcase Categories")
#     log.info("=" * 60)

#     start = datetime.now()
#     try:
#         with engine.connect() as conn:
#             rows = conn.execute(
#                 text("SELECT * FROM run_showcase_categories()")
#             ).fetchall()
#             conn.commit()

#         elapsed = (datetime.now() - start).total_seconds()

#         total_rows = 0
#         for row in rows:
#             category = row[0]
#             entity_type = row[1]
#             count = row[2]
#             total_rows += count
#             log.info(f"  {entity_type} / {category}: {count:,} rows")

#         log.info(
#             f"  ✓ Showcase Categories complete — {total_rows:,} total rows in {elapsed:.1f}s"
#         )
#         return {
#             "run_showcase_categories": {f"{row[1]}/{row[0]}": row[2] for row in rows}
#         }

#     except Exception as e:
#         msg = f"Showcase Categories (run_showcase_categories) failed: {e}"
#         log.error(f"  {msg}")
#         slack_phase_failure("Showcase Categories", msg, log)
#         raise


# def phase_5c_entity_lookup_refresh(engine, log):
#     log.info("=" * 60)
#     log.info("PHASE 5C — Refreshing entity lookup table")
#     log.info("=" * 60)

#     start = datetime.now()
#     try:
#         with engine.connect() as conn:
#             rows = conn.execute(
#                 text("SELECT * FROM run_miq_entity_lookup_refresh()")
#             ).fetchall()
#             conn.commit()

#         elapsed = (datetime.now() - start).total_seconds()

#         summary = {row[0]: row[1] for row in rows}

#         # Log each row
#         for row in rows:
#             log.info(f"  {row[0]}: {row[1]:,}")

#         log.info(
#             f"  ✓ Entity lookup refresh complete in {elapsed:.1f}s — "
#             f"{summary.get('entity_lookup_total_inserted', 0):,} inserted, "
#             f"{summary.get('entity_lookup_total_upgraded', 0):,} upgraded"
#         )

#         return {"run_miq_entity_lookup_refresh": summary}

#     except Exception as e:
#         msg = f"Entity lookup refresh (run_miq_entity_lookup_refresh) failed: {e}"
#         log.error(f"  {msg}")
#         slack_phase_failure("Entity Lookup Refresh", msg, log)
#         raise


# ─────────────────────────────────────────────────────────────
# PHASE 4 — Refresh validated tables
# ─────────────────────────────────────────────────────────────


# def phase_4_validated_tables(engine, log):
#     log.info("=" * 60)
#     log.info("PHASE 4 — Refreshing validated tables")
#     log.info("=" * 60)

#     for attempt in range(1, MAX_RETRIES + 1):
#         try:
#             with engine.connect() as conn:
#                 log.info(
#                     f"Calling refresh_validated_tables() "
#                     f"(attempt {attempt}/{MAX_RETRIES})..."
#                 )
#                 start = datetime.now()
#                 conn.execute(text("CALL refresh_validated_tables()"))
#                 conn.commit()
#                 elapsed = (datetime.now() - start).total_seconds()
#                 log.info(f"✓ refresh_validated_tables() completed in {elapsed:.1f}s")
#                 return

#         except Exception as e:
#             if _is_deadlock(e) and attempt < MAX_RETRIES:
#                 log.warning(
#                     f"Phase 4 — deadlock on attempt {attempt}/{MAX_RETRIES}, "
#                     f"retrying in {RETRY_DELAY}s..."
#                 )
#                 time.sleep(RETRY_DELAY)
#             else:
#                 raise


def phase_4_validated_tables(engine, log):
    log.info("=" * 60)
    log.info("PHASE 4 — Refreshing validated tables")
    log.info("=" * 60)

    for attempt in range(1, MAX_RETRIES + 1):
        kill_interval = 0.5
        stop_event = threading.Event()
        killer = threading.Thread(
            target=_query_killer, args=(stop_event, log, kill_interval), daemon=True
        )
        killer.start()
        log.info(
            f"Killer thread started (targeting {BLOCKING_CLIENT_IP}, interval: {kill_interval}s)"
        )

        try:
            with engine.connect() as conn:
                log.info(
                    f"Calling refresh_validated_tables() "
                    f"(attempt {attempt}/{MAX_RETRIES})..."
                )
                start = datetime.now()
                conn.execute(text("CALL refresh_validated_tables()"))
                conn.commit()
                elapsed = (datetime.now() - start).total_seconds()
                log.info(f"✓ refresh_validated_tables() completed in {elapsed:.1f}s")
                return

        except Exception as e:
            if _is_deadlock(e) and attempt < MAX_RETRIES:
                log.warning(
                    f"Phase 4 — deadlock on attempt {attempt}/{MAX_RETRIES}, "
                    f"retrying in {RETRY_DELAY}s..."
                )
                time.sleep(RETRY_DELAY)
            else:
                raise

        finally:
            stop_event.set()
            killer.join(timeout=2)
            log.info("Killer thread stopped")


# ─────────────────────────────────────────────────────────────
# PHASE 4b — Enrich area names
# ─────────────────────────────────────────────────────────────


def phase_4b_enrich_area_names(engine, log):
    log.info("=" * 60)
    log.info("PHASE 4b — Enriching area names")
    log.info("=" * 60)

    start = datetime.now()

    if not os.path.exists(AREA_NICKNAMES_PATH):
        log.warning(
            f"Area nicknames file not found at {AREA_NICKNAMES_PATH} — skipping."
        )
        return

    with open(AREA_NICKNAMES_PATH, "r", encoding="utf-8") as f:
        areas = json.load(f)

    log.info(f"Loaded {len(areas)} area entries from nicknames file.")

    updated_by_id = 0
    updated_by_name = 0
    skipped = 0
    not_found = 0

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with engine.connect() as conn:
                for area in areas:
                    area_id = area.get("AREA_ID")
                    name_en = (area.get("NAME_EN") or "").strip()
                    nickname = (area.get("nicknames") or "").strip()
                    shortname = (area.get("shortnames") or "").strip()

                    if not nickname and not shortname:
                        skipped += 1
                        continue

                    if nickname and shortname:
                        suffix = f"({nickname} - {shortname})"
                    elif nickname:
                        suffix = f"({nickname})"
                    else:
                        suffix = f"({shortname})"

                    try:
                        result = conn.execute(
                            text("""
                                UPDATE areas
                                SET name_en = name_en || ' ' || :suffix
                                WHERE area_id = :area_id
                                AND   name_en NOT LIKE :suffix_check
                            """),
                            {
                                "area_id": str(area_id),
                                "suffix": suffix,
                                "suffix_check": f"%{suffix}",
                            },
                        )
                        conn.commit()

                        if result.rowcount > 0:
                            updated_by_id += 1
                            continue

                        if not name_en:
                            not_found += 1
                            continue

                        result = conn.execute(
                            text("""
                                UPDATE areas
                                SET name_en = name_en || ' ' || :suffix
                                WHERE name_en = :name_en
                                AND   name_en NOT LIKE :suffix_check
                            """),
                            {
                                "name_en": name_en,
                                "suffix": suffix,
                                "suffix_check": f"%{suffix}",
                            },
                        )
                        conn.commit()

                        if result.rowcount > 0:
                            updated_by_name += 1
                        else:
                            not_found += 1

                    except Exception as e:
                        conn.rollback()

            elapsed = (datetime.now() - start).total_seconds()
            log.info(
                f"✓ Area name enrichment complete in {elapsed:.1f}s — "
                f"{updated_by_id} updated by ID, "
                f"{updated_by_name} updated by name, "
                f"{skipped} skipped (no nickname/shortname), "
                f"{not_found} not found in DB."
            )
            return

        except Exception as e:
            if _is_deadlock(e) and attempt < MAX_RETRIES:
                log.warning(
                    f"Phase 4b — deadlock on attempt {attempt}/{MAX_RETRIES}, "
                    f"retrying in {RETRY_DELAY}s..."
                )
                # Reset counters before retry
                updated_by_id = updated_by_name = skipped = not_found = 0
                time.sleep(RETRY_DELAY)
            else:
                raise


# ─────────────────────────────────────────────────────────────
# PHASE 4c — Load location amenities
# ─────────────────────────────────────────────────────────────


def phase_4c_get_location_amenities(engine, log):
    log.info("=" * 60)
    log.info("PHASE 4c — Loading location amenities")
    log.info("=" * 60)
    start = datetime.now()

    CATEGORY_MAP = {
        "lm": "landmarks",
        "sch": "schools",
        "hc": "healthcare",
        "sup": "supermarkets",
        "mal": "malls",
        "res": "restaurants",
        "pk": "parks",
    }

    # --- 1. Pull buildings and projects from DB ---
    with engine.connect() as conn:
        buildings = (
            conn.execute(
                text("""
                    SELECT distinct entity_id, 'building' AS entity_type,
                           b.entity_name || ', ' || a.name_en AS search_query,
                           b.area_id
                    FROM miq_miqs_building b
                    LEFT JOIN areas a ON a.area_id = b.area_id
                    WHERE b.entity_name IS NOT NULL
                """)
            )
            .mappings()
            .all()
        )

        projects = (
            conn.execute(
                text("""
                    SELECT distinct entity_id, 'project' AS entity_type,
                           b.entity_name || ', ' || a.name_en AS search_query,
                           b.area_id
                    FROM miq_miqs_project b
                    LEFT JOIN areas a ON a.area_id = b.area_id
                    WHERE b.entity_name IS NOT NULL
                """)
            )
            .mappings()
            .all()
        )

    entities = [dict(r) for r in buildings] + [dict(r) for r in projects]
    log.info(f"Loaded {len(buildings)} buildings and {len(projects)} projects from DB")

    # --- 2. Fetch amenity rows (external API — no retry on deadlock here) ---
    amenity_rows = []
    transport_rows = []
    skipped = 0
    fetched = 0

    for entity in entities:
        report, response = le(entity["search_query"])
        if "error" in report:
            log.warning(f"Skipping {entity['search_query']}: {report['error']}")
            skipped += 1
            continue

        base = {
            "entity_id": entity["entity_id"],
            "entity_type": entity["entity_type"],
            "area_id": entity["area_id"],
            "location_name": report.get("loc"),
            "area": report.get("ar"),
            "city": report.get("ct"),
        }

        for key, category_name in CATEGORY_MAP.items():
            for item in report.get(key, []):
                amenity_rows.append(
                    {
                        **base,
                        "category": category_name,
                        "amenity_name": item.get("n", None),
                        "amenity_type": item.get("t", None),
                        "distance_km": item.get("d", None),
                    }
                )

        tr = report.get("tr", {})
        for road in tr.get("rd", []):
            transport_rows.append(
                {
                    **base,
                    "road_name": road,
                    "metro_access": tr.get("mt", False),
                }
            )

        fetched += 1
        if fetched % 50 == 0:
            log.info(f"  Fetched {fetched}/{len(entities)} entities...")

    log.info(
        f"Fetch complete — {fetched} entities, {skipped} skipped, "
        f"{len(amenity_rows)} amenity rows, {len(transport_rows)} transport rows"
    )

    if not amenity_rows and not transport_rows:
        log.warning("No rows to load — aborting truncate to preserve existing data")
        return

    # --- 3. Truncate and load atomically (with deadlock retry) ---
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("TRUNCATE TABLE location_amenities"))
                conn.execute(text("TRUNCATE TABLE location_transport"))
                log.info("Tables truncated")

                if amenity_rows:
                    conn.execute(
                        text("""
                            INSERT INTO location_amenities
                                (entity_id, entity_type, area_id, location_name, area, city,
                                 category, amenity_name, amenity_type, distance_km)
                            VALUES
                                (:entity_id, :entity_type, :area_id, :location_name, :area, :city,
                                 :category, :amenity_name, :amenity_type, :distance_km)
                        """),
                        amenity_rows,
                    )

                if transport_rows:
                    conn.execute(
                        text("""
                            INSERT INTO location_transport
                                (entity_id, entity_type, area_id, location_name, area, city,
                                 road_name, metro_access)
                            VALUES
                                (:entity_id, :entity_type, :area_id, :location_name, :area, :city,
                                 :road_name, :metro_access)
                        """),
                        transport_rows,
                    )

                conn.commit()

            elapsed = (datetime.now() - start).total_seconds()
            log.info(
                f"✓ Location amenities load complete in {elapsed:.1f}s — "
                f"{len(amenity_rows)} amenity rows + {len(transport_rows)} transport rows "
                f"across {fetched} entities, {skipped} skipped"
            )
            return

        except Exception as e:
            if _is_deadlock(e) and attempt < MAX_RETRIES:
                log.warning(
                    f"Phase 4c — deadlock on attempt {attempt}/{MAX_RETRIES}, "
                    f"retrying in {RETRY_DELAY}s..."
                )
                time.sleep(RETRY_DELAY)
            else:
                log.error(f"Load failed — rolling back: {e}")
                raise


# ─────────────────────────────────────────────────────────────
# PHASE 5 — Run MIQ component functions
# ─────────────────────────────────────────────────────────────


def phase_5_miq_components(engine, log):
    log.info("=" * 60)
    log.info("PHASE 5 — Running MIQ component functions")
    log.info("=" * 60)

    functions = [
        ("run_miq_cap_app", "Capital Appreciation"),
        ("run_miq_occupancy", "Occupancy Rate"),
        ("run_miq_resale", "Resale Index"),
        ("run_miq_roi", "Rental ROI"),
        ("run_miq_score", "MIQ Score"),
        ("run_miq_score_agg", "MIQ Score Agg"),
    ]

    miq_summary = {}

    for fn_name, label in functions:
        log.info(f"Running {label} ({fn_name})...")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                start = datetime.now()
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
                    elif fn_name == "run_miq_score_agg":
                        log.info(
                            f"  {row[0]} [{row[1]}]: inserted={row[2]:,} "
                            f"scored={row[3]:,} avg_score={row[4]}"
                        )
                    else:
                        log.info(f"  {row[0]}: {row[1]:,} rows")

                log.info(f"  ✓ {label} complete in {elapsed:.1f}s")
                miq_summary[fn_name] = fn_result
                break  # success — move to next function

            except Exception as e:
                if _is_deadlock(e) and attempt < MAX_RETRIES:
                    log.warning(
                        f"Phase 5 ({fn_name}) — deadlock on attempt "
                        f"{attempt}/{MAX_RETRIES}, retrying in {RETRY_DELAY}s..."
                    )
                    time.sleep(RETRY_DELAY)
                else:
                    msg = f"{label} ({fn_name}) failed: {e}"
                    log.error(f"  {msg}")
                    slack_phase_failure("MIQ Components", msg, log)
                    raise

    return miq_summary


# ─────────────────────────────────────────────────────────────
# PHASE 5b — Showcase categories
# ─────────────────────────────────────────────────────────────


def phase_5b_showcase_categories(engine, log):
    log.info("=" * 60)
    log.info("PHASE 5B — Running MIQ Showcase Categories")
    log.info("=" * 60)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            start = datetime.now()
            with engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT * FROM run_showcase_categories()")
                ).fetchall()
                conn.commit()

            elapsed = (datetime.now() - start).total_seconds()
            total_rows = 0
            for row in rows:
                category = row[0]
                entity_type = row[1]
                count = row[2]
                total_rows += count
                log.info(f"  {entity_type} / {category}: {count:,} rows")

            log.info(
                f"  ✓ Showcase Categories complete — "
                f"{total_rows:,} total rows in {elapsed:.1f}s"
            )
            return {
                "run_showcase_categories": {
                    f"{row[1]}/{row[0]}": row[2] for row in rows
                }
            }

        except Exception as e:
            if _is_deadlock(e) and attempt < MAX_RETRIES:
                log.warning(
                    f"Phase 5b — deadlock on attempt {attempt}/{MAX_RETRIES}, "
                    f"retrying in {RETRY_DELAY}s..."
                )
                time.sleep(RETRY_DELAY)
            else:
                msg = f"Showcase Categories (run_showcase_categories) failed: {e}"
                log.error(f"  {msg}")
                slack_phase_failure("Showcase Categories", msg, log)
                raise


# ─────────────────────────────────────────────────────────────
# PHASE 5c — Entity lookup refresh
# ─────────────────────────────────────────────────────────────


def phase_5c_entity_lookup_refresh(engine, log):
    log.info("=" * 60)
    log.info("PHASE 5C — Refreshing entity lookup table")
    log.info("=" * 60)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            start = datetime.now()
            with engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT * FROM run_miq_entity_lookup_refresh()")
                ).fetchall()
                conn.commit()

            elapsed = (datetime.now() - start).total_seconds()
            summary = {row[0]: row[1] for row in rows}

            for row in rows:
                log.info(f"  {row[0]}: {row[1]:,}")

            log.info(
                f"  ✓ Entity lookup refresh complete in {elapsed:.1f}s — "
                f"{summary.get('entity_lookup_total_inserted', 0):,} inserted, "
                f"{summary.get('entity_lookup_total_upgraded', 0):,} upgraded"
            )
            return {"run_miq_entity_lookup_refresh": summary}

        except Exception as e:
            if _is_deadlock(e) and attempt < MAX_RETRIES:
                log.warning(
                    f"Phase 5c — deadlock on attempt {attempt}/{MAX_RETRIES}, "
                    f"retrying in {RETRY_DELAY}s..."
                )
                time.sleep(RETRY_DELAY)
            else:
                msg = (
                    f"Entity lookup refresh (run_miq_entity_lookup_refresh) failed: {e}"
                )
                log.error(f"  {msg}")
                return {"run_miq_entity_lookup_refresh": f"failed: {e}"}


# ─────────────────────────────────────────────────────────────
# PHASE 6 — Monthly snapshot (non-fatal)
# ─────────────────────────────────────────────────────────────


def phase_6_snapshot(engine, log):
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
        slack_snapshot_failure(note, log)
        return "failed", note


# ─────────────────────────────────────────────────────────────
# PHASE 7 — Monthly snapshot (non-fatal)
# ─────────────────────────────────────────────────────────────


def phase_7_build_transactions_cache(engine, log):
    log.info("=" * 60)
    log.info("PHASE 7 — Refreshing Transactions Cache")
    log.info("=" * 60)

    start = datetime.now()
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT * FROM run_miq_transactions_cache_refresh()")
            ).fetchone()
            conn.commit()

        elapsed = (datetime.now() - start).total_seconds()
        log.info(
            f"  ✓ Transactions Cache refreshed — {rows[0]:,} rows in {elapsed:.1f}s"
        )
        return {"run_miq_transactions_cache_refresh": {"rows_written": rows[0]}}

    except Exception as e:
        elapsed = (datetime.now() - start).total_seconds()
        msg = f"Transactions Cache refresh failed after {elapsed:.1f}s: {e}"
        log.warning(f"  ⚠ {msg}")
        # non-destructive — log only, do not raise, do not fail the ETL
        return {"run_miq_transactions_cache_refresh": {"error": str(e)}}
