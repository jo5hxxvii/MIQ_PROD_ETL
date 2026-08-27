import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from functions.config import _config
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

PG_URL = _config.PG_URL
engine = create_engine(PG_URL)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Paths to the amenity JSON files — update as needed
AMENITY_FOLDER = "miq_amenities"
AMENITY_FILES = sorted(Path(AMENITY_FOLDER).glob("*.json"))

# ---------------------------------------------------------------------------
# Scoring model config
# ---------------------------------------------------------------------------

CATEGORIES = {
    "sup": {
        "name": "Supermarket",
        "weight": 18,
        "mode": "walk",
        "radius": 1.0,
        "density": True,
    },
    "hc": {
        "name": "Healthcare",
        "weight": 18,
        "mode": "drive",
        "radius": 5.0,
        "density": True,
    },
    "sch": {
        "name": "School",
        "weight": 14,
        "mode": "drive",
        "radius": 5.0,
        "density": True,
    },
    "res": {
        "name": "Restaurant",
        "weight": 14,
        "mode": "walk",
        "radius": 1.0,
        "density": True,
    },
    "pk": {
        "name": "Park",
        "weight": 10,
        "mode": "walk",
        "radius": 1.0,
        "density": True,
    },
    "mal": {
        "name": "Mall",
        "weight": 8,
        "mode": "drive",
        "radius": 5.0,
        "density": True,
    },
    "lm": {
        "name": "Landmark",
        "weight": 8,
        "mode": "neutral",
        "radius": 2.0,
        "density": True,
    },
    "metro": {
        "name": "Metro",
        "weight": 10,
        "mode": "metro",
        "radius": None,
        "density": False,
    },
}
# Weights sum to 100. Metro has no density component and uses its own proximity bands.

DENSITY_CAP = 5  # count beyond this adds no more density score
PROX_BLEND = 0.70  # proximity share within each category blend
DENS_BLEND = 0.30  # density share within each category blend

CATEGORY_LABELS = {
    "lm": "landmark",
    "sch": "school",
    "hc": "healthcare",
    "sup": "supermarket",
    "mal": "mall",
    "res": "restaurant",
    "pk": "park",
}

# ---------------------------------------------------------------------------
# Proximity functions
# ---------------------------------------------------------------------------


def walk_prox(d):
    if d <= 0.5:
        return 100
    if d <= 1.0:
        return 80
    if d <= 2.0:
        return 50
    if d <= 3.0:
        return 20
    return 5


def drive_prox(d):
    if d <= 2.0:
        return 100
    if d <= 5.0:
        return 80
    if d <= 10.0:
        return 50
    if d <= 15.0:
        return 20
    return 5


def neutral_prox(d):
    if d <= 1.0:
        return 100
    if d <= 2.0:
        return 80
    if d <= 5.0:
        return 50
    if d <= 10.0:
        return 20
    return 5


def metro_prox(d):
    if d <= 0.5:
        return 100
    if d <= 1.0:
        return 80
    if d <= 2.0:
        return 55
    if d <= 5.0:
        return 30
    if d <= 10.0:
        return 10
    return 0


PROX_FN = {
    "walk": walk_prox,
    "drive": drive_prox,
    "neutral": neutral_prox,
    "metro": metro_prox,
}

# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------


def _score_category(items, mode, radius):
    dists = [x["d"] for x in items if x.get("d") is not None]
    if not dists:
        return {
            "nearest_km": None,
            "count_in_radius": 0,
            "proximity_score": 0,
            "density_score": 0,
            "blended_score": 0,
        }
    nearest = min(dists)
    prox_s = PROX_FN[mode](nearest)
    cnt = sum(1 for d in dists if d <= radius)
    dens_s = min(cnt / DENSITY_CAP, 1.0) * 100
    blend = round(PROX_BLEND * prox_s + DENS_BLEND * dens_s, 2)
    return {
        "nearest_km": round(nearest, 2),
        "count_in_radius": cnt,
        "proximity_score": round(prox_s, 2),
        "density_score": round(dens_s, 2),
        "blended_score": blend,
    }


def _score_building(gid, record):
    cat_scores = {}
    composite = 0.0

    for cat_key, meta in CATEGORIES.items():
        if cat_key == "metro":
            metro_dist = record["tr"].get("mt")
            prox_s = metro_prox(metro_dist) if metro_dist is not None else 0
            result = {
                "nearest_km": round(metro_dist, 2) if metro_dist is not None else None,
                "count_in_radius": None,
                "proximity_score": round(prox_s, 2),
                "density_score": None,
                "blended_score": round(prox_s, 2),
            }
        else:
            result = _score_category(
                record.get(cat_key, []), meta["mode"], meta["radius"]
            )

        cat_scores[cat_key] = result
        composite += result["blended_score"] / 100.0 * meta["weight"]

    return {
        "gid": gid,
        "location_name": record["loc"],
        "area": record["ar"],
        "city": record.get("ct", "Dubai"),
        "latitude": record.get("lt"),
        "longitude": record.get("lg"),
        "amenity_score": round(composite, 2),
        "sup_nearest_km": cat_scores["sup"]["nearest_km"],
        "sup_count_in_radius": cat_scores["sup"]["count_in_radius"],
        "sup_proximity_score": cat_scores["sup"]["proximity_score"],
        "sup_density_score": cat_scores["sup"]["density_score"],
        "sup_blended_score": cat_scores["sup"]["blended_score"],
        "hc_nearest_km": cat_scores["hc"]["nearest_km"],
        "hc_count_in_radius": cat_scores["hc"]["count_in_radius"],
        "hc_proximity_score": cat_scores["hc"]["proximity_score"],
        "hc_density_score": cat_scores["hc"]["density_score"],
        "hc_blended_score": cat_scores["hc"]["blended_score"],
        "sch_nearest_km": cat_scores["sch"]["nearest_km"],
        "sch_count_in_radius": cat_scores["sch"]["count_in_radius"],
        "sch_proximity_score": cat_scores["sch"]["proximity_score"],
        "sch_density_score": cat_scores["sch"]["density_score"],
        "sch_blended_score": cat_scores["sch"]["blended_score"],
        "res_nearest_km": cat_scores["res"]["nearest_km"],
        "res_count_in_radius": cat_scores["res"]["count_in_radius"],
        "res_proximity_score": cat_scores["res"]["proximity_score"],
        "res_density_score": cat_scores["res"]["density_score"],
        "res_blended_score": cat_scores["res"]["blended_score"],
        "pk_nearest_km": cat_scores["pk"]["nearest_km"],
        "pk_count_in_radius": cat_scores["pk"]["count_in_radius"],
        "pk_proximity_score": cat_scores["pk"]["proximity_score"],
        "pk_density_score": cat_scores["pk"]["density_score"],
        "pk_blended_score": cat_scores["pk"]["blended_score"],
        "mal_nearest_km": cat_scores["mal"]["nearest_km"],
        "mal_count_in_radius": cat_scores["mal"]["count_in_radius"],
        "mal_proximity_score": cat_scores["mal"]["proximity_score"],
        "mal_density_score": cat_scores["mal"]["density_score"],
        "mal_blended_score": cat_scores["mal"]["blended_score"],
        "lm_nearest_km": cat_scores["lm"]["nearest_km"],
        "lm_count_in_radius": cat_scores["lm"]["count_in_radius"],
        "lm_proximity_score": cat_scores["lm"]["proximity_score"],
        "lm_density_score": cat_scores["lm"]["density_score"],
        "lm_blended_score": cat_scores["lm"]["blended_score"],
        "metro_dist_km": cat_scores["metro"]["nearest_km"],
        "metro_proximity_score": cat_scores["metro"]["proximity_score"],
        "metro_pts": round(
            cat_scores["metro"]["blended_score"]
            / 100.0
            * CATEGORIES["metro"]["weight"],
            2,
        ),
        "major_roads": record["tr"].get("rd", []),
        "scored_at": datetime.now(timezone.utc),
    }


def _detail_rows(gid, record):
    rows = []
    for cat_key, category_label in CATEGORY_LABELS.items():
        for rank, item in enumerate(record.get(cat_key, []), start=1):
            rows.append(
                {
                    "gid": gid,
                    "category": category_label,
                    "name": item.get("n"),
                    "facility_type": item.get("t"),
                    "distance_km": item.get("d"),
                    "rank": rank,
                }
            )
    return rows


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_SCORES = """
CREATE TABLE IF NOT EXISTS miq_amenity_scores (
    gid                    TEXT    PRIMARY KEY,
    location_name          TEXT,
    area                   TEXT,
    city                   TEXT,
    latitude               NUMERIC,
    longitude              NUMERIC,
    amenity_score          NUMERIC,
    sup_nearest_km         NUMERIC,
    sup_count_in_radius    INTEGER,
    sup_proximity_score    NUMERIC,
    sup_density_score      NUMERIC,
    sup_blended_score      NUMERIC,
    hc_nearest_km          NUMERIC,
    hc_count_in_radius     INTEGER,
    hc_proximity_score     NUMERIC,
    hc_density_score       NUMERIC,
    hc_blended_score       NUMERIC,
    sch_nearest_km         NUMERIC,
    sch_count_in_radius    INTEGER,
    sch_proximity_score    NUMERIC,
    sch_density_score      NUMERIC,
    sch_blended_score      NUMERIC,
    res_nearest_km         NUMERIC,
    res_count_in_radius    INTEGER,
    res_proximity_score    NUMERIC,
    res_density_score      NUMERIC,
    res_blended_score      NUMERIC,
    pk_nearest_km          NUMERIC,
    pk_count_in_radius     INTEGER,
    pk_proximity_score     NUMERIC,
    pk_density_score       NUMERIC,
    pk_blended_score       NUMERIC,
    mal_nearest_km         NUMERIC,
    mal_count_in_radius    INTEGER,
    mal_proximity_score    NUMERIC,
    mal_density_score      NUMERIC,
    mal_blended_score      NUMERIC,
    lm_nearest_km          NUMERIC,
    lm_count_in_radius     INTEGER,
    lm_proximity_score     NUMERIC,
    lm_density_score       NUMERIC,
    lm_blended_score       NUMERIC,
    metro_dist_km          NUMERIC,
    metro_proximity_score  NUMERIC,
    metro_pts              NUMERIC,
    major_roads            TEXT[],
    scored_at              TIMESTAMPTZ DEFAULT NOW()
)
"""

DDL_DETAILS = """
CREATE TABLE IF NOT EXISTS miq_amenity_details (
    id             BIGSERIAL PRIMARY KEY,
    gid            TEXT      NOT NULL,
    category       TEXT      NOT NULL,
    name           TEXT,
    facility_type  TEXT,
    distance_km    NUMERIC,
    rank           INTEGER,
    CONSTRAINT fk_amenity_score
        FOREIGN KEY (gid) REFERENCES miq_amenity_scores(gid) ON DELETE CASCADE
)
"""

DDL_DETAILS_IDX_GID = "CREATE INDEX IF NOT EXISTS idx_amenity_details_gid      ON miq_amenity_details(gid)"
DDL_DETAILS_IDX_CAT = "CREATE INDEX IF NOT EXISTS idx_amenity_details_category ON miq_amenity_details(category)"

INSERT_SCORE = """
INSERT INTO miq_amenity_scores (
    gid, location_name, area, city, latitude, longitude, amenity_score,
    sup_nearest_km, sup_count_in_radius, sup_proximity_score, sup_density_score, sup_blended_score,
    hc_nearest_km,  hc_count_in_radius,  hc_proximity_score,  hc_density_score,  hc_blended_score,
    sch_nearest_km, sch_count_in_radius, sch_proximity_score, sch_density_score, sch_blended_score,
    res_nearest_km, res_count_in_radius, res_proximity_score, res_density_score, res_blended_score,
    pk_nearest_km,  pk_count_in_radius,  pk_proximity_score,  pk_density_score,  pk_blended_score,
    mal_nearest_km, mal_count_in_radius, mal_proximity_score, mal_density_score, mal_blended_score,
    lm_nearest_km,  lm_count_in_radius,  lm_proximity_score,  lm_density_score,  lm_blended_score,
    metro_dist_km, metro_proximity_score, metro_pts,
    major_roads, scored_at
) VALUES (
    :gid, :location_name, :area, :city, :latitude, :longitude, :amenity_score,
    :sup_nearest_km, :sup_count_in_radius, :sup_proximity_score, :sup_density_score, :sup_blended_score,
    :hc_nearest_km,  :hc_count_in_radius,  :hc_proximity_score,  :hc_density_score,  :hc_blended_score,
    :sch_nearest_km, :sch_count_in_radius, :sch_proximity_score, :sch_density_score, :sch_blended_score,
    :res_nearest_km, :res_count_in_radius, :res_proximity_score, :res_density_score, :res_blended_score,
    :pk_nearest_km,  :pk_count_in_radius,  :pk_proximity_score,  :pk_density_score,  :pk_blended_score,
    :mal_nearest_km, :mal_count_in_radius, :mal_proximity_score, :mal_density_score, :mal_blended_score,
    :lm_nearest_km,  :lm_count_in_radius,  :lm_proximity_score,  :lm_density_score,  :lm_blended_score,
    :metro_dist_km, :metro_proximity_score, :metro_pts,
    :major_roads, :scored_at
)
"""

INSERT_DETAIL = """
INSERT INTO miq_amenity_details (gid, category, name, facility_type, distance_km, rank)
VALUES (:gid, :category, :name, :facility_type, :distance_km, :rank)
"""

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def run_amenity_pipeline(file_paths: list, engine, log, batch_size: int = 500):
    log.info("=" * 60)
    log.info("PHASE — MIQ Amenity Scoring Pipeline")
    log.info("=" * 60)
    start = datetime.now()

    # -- Load files ----------------------------------------------------------
    log.info(f"Loading {len(file_paths)} file(s)...")
    data = {}
    for fp in file_paths:
        with open(fp, encoding="utf-8") as f:
            part = json.load(f)
        data.update(part)
        log.info(f"  {fp} → {len(part)} records")
    log.info(f"Total records loaded: {len(data)}")

    # -- Score ---------------------------------------------------------------
    log.info("Scoring buildings...")
    score_rows = []
    detail_rows = []
    errors = []

    for gid, record in data.items():
        try:
            score_rows.append(_score_building(gid, record))
            detail_rows.extend(_detail_rows(gid, record))
        except Exception as e:
            errors.append((gid, str(e)))

    if errors:
        log.warning(f"Scoring errors on {len(errors)} records (first 5): {errors[:5]}")

    scores = [r["amenity_score"] for r in score_rows]
    log.info(
        f"Scored {len(score_rows)} buildings — "
        f"min={min(scores):.1f}, max={max(scores):.1f}, avg={sum(scores) / len(scores):.1f}"
    )
    log.info(f"Detail rows prepared: {len(detail_rows)}")

    # -- Push to DB ----------------------------------------------------------
    inserted_scores = 0
    inserted_details = 0

    with engine.connect() as conn:
        # Create tables and indexes if they don't exist
        try:
            conn.execute(text(DDL_SCORES))
            conn.execute(text(DDL_DETAILS))
            conn.execute(text(DDL_DETAILS_IDX_GID))
            conn.execute(text(DDL_DETAILS_IDX_CAT))
            conn.commit()
            log.info("Tables and indexes verified.")
        except Exception as e:
            conn.rollback()
            log.error(f"DDL failed: {e}")
            raise

        # Truncate both tables — details first to respect the FK constraint
        try:
            conn.execute(
                text("TRUNCATE TABLE miq_amenity_details RESTART IDENTITY CASCADE")
            )
            conn.execute(
                text("TRUNCATE TABLE miq_amenity_scores  RESTART IDENTITY CASCADE")
            )
            conn.commit()
            log.info("Tables truncated.")
        except Exception as e:
            conn.rollback()
            log.error(f"Truncate failed: {e}")
            raise

        # Load scores
        log.info(f"Inserting {len(score_rows)} score rows (batch={batch_size})...")
        for i in range(0, len(score_rows), batch_size):
            batch = score_rows[i : i + batch_size]
            try:
                conn.execute(text(INSERT_SCORE), batch)
                conn.commit()
                inserted_scores += len(batch)
                log.info(f"  Scores: {inserted_scores}/{len(score_rows)}")
            except Exception as e:
                conn.rollback()
                log.error(f"Score insert failed at batch {i}: {e}")
                raise

        # Load details
        log.info(f"Inserting {len(detail_rows)} detail rows (batch={batch_size})...")
        for i in range(0, len(detail_rows), batch_size):
            batch = detail_rows[i : i + batch_size]
            try:
                conn.execute(text(INSERT_DETAIL), batch)
                conn.commit()
                inserted_details += len(batch)
                log.info(f"  Details: {inserted_details}/{len(detail_rows)}")
            except Exception as e:
                conn.rollback()
                log.error(f"Detail insert failed at batch {i}: {e}")
                raise

    elapsed = (datetime.now() - start).total_seconds()
    log.info(
        f"✓ Amenity pipeline complete in {elapsed:.1f}s — "
        f"{inserted_scores} scores inserted, "
        f"{inserted_details} detail rows inserted, "
        f"{len(errors)} scoring errors."
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_amenity_pipeline(
        file_paths=AMENITY_FILES,
        engine=engine,
        log=log,
    )
