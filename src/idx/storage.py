"""ClickHouse storage layer for STOXX index data."""

from __future__ import annotations

import hashlib
import logging
import os
import struct
from datetime import date

import polars as pl
import psycopg
from dotenv import load_dotenv
from prefect import task
from prefect.cache_policies import NO_CACHE
from psycopg import sql

logger = logging.getLogger(__name__)


def get_connection() -> psycopg.Connection:
    """Create a ClickHouse connection via Postgres wire protocol."""
    load_dotenv()
    return psycopg.connect(
        host=os.environ["PGHOST"],
        port=int(os.environ["PGPORT"]),
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        dbname=os.environ["PGDATABASE"],
        sslmode="require",
    )


def _hash_to_bigint(*parts: str) -> int:
    """Deterministic hash of natural keys to a signed BIGINT."""
    digest = hashlib.sha256("|".join(parts).encode()).digest()
    return struct.unpack(">q", digest[:8])[0]


def _chunked_executemany(cur: psycopg.Cursor, sql: str, params: list[tuple], chunk_size: int = 10_000) -> None:
    """Execute INSERT in chunks to avoid oversized packets."""
    for i in range(0, len(params), chunk_size):
        cur.executemany(sql, params[i : i + chunk_size])


@task(cache_policy=NO_CACHE)
def write_assets(enriched_assets: pl.DataFrame) -> None:
    """Write enriched assets to the assets table."""
    rows = enriched_assets.to_dicts()
    if not rows:
        logger.warning("No assets to write")
        return

    params: list[tuple] = []
    ids: list[int] = []
    for row in rows:
        asset_id = _hash_to_bigint(row["internal_key"])
        ids.append(asset_id)
        params.append(
            (
                asset_id,
                row["internal_key"],
                row.get("ric"),
                row.get("name"),
                row.get("country"),
                row.get("currency"),
                row.get("isin"),
                row.get("sedol"),
                row.get("yukka_id"),
            )
        )

    try:
        conn = get_connection()
        with conn:
            cur = conn.cursor()
            cur.execute(
                sql.SQL("DELETE FROM assets WHERE id IN ({})").format(
                    sql.SQL(",").join(sql.Placeholder() for _ in ids)
                ),
                ids,
            )
            _chunked_executemany(
                cur,
                "INSERT INTO assets (id, internal_key, ric, name, country, currency, isin, sedol, yukka_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                params,
            )
        conn.close()
        logger.info("Wrote %d rows to assets", len(params))
    except Exception:
        logger.error("Failed to write assets", exc_info=True)
        raise


@task(cache_policy=NO_CACHE)
def write_ranks(ranking_df: pl.DataFrame, enriched_assets: pl.DataFrame) -> None:
    """Unpivot ranking table and write to the ranks table."""
    ric_cols = [c for c in ranking_df.columns if c != "date"]
    if not ric_cols or ranking_df.is_empty():
        logger.warning("No ranking data to write")
        return

    # Build RIC -> asset_id mapping
    ric_to_asset_id: dict[str, int] = {}
    for row in enriched_assets.select("internal_key", "ric").iter_rows():
        ric_to_asset_id[row[1]] = _hash_to_bigint(row[0])

    # Unpivot from wide to long
    long_df = ranking_df.unpivot(on=ric_cols, index="date", variable_name="ric", value_name="rank").filter(
        pl.col("rank").is_not_null()
    )

    unmapped_rics: set[str] = set()
    params: list[tuple] = []
    for row in long_df.iter_rows(named=True):
        ric = row["ric"]
        asset_id = ric_to_asset_id.get(ric)
        if asset_id is None:
            unmapped_rics.add(ric)
            continue
        row_date = row["date"].isoformat() if isinstance(row["date"], date) else str(row["date"])
        rank_id = _hash_to_bigint(str(asset_id), row_date)
        params.append((rank_id, asset_id, row_date, int(row["rank"])))

    if unmapped_rics:
        logger.warning("Skipped %d unmapped RICs in ranks: %s", len(unmapped_rics), sorted(unmapped_rics)[:10])

    if not params:
        logger.warning("No rank rows to write after filtering")
        return

    # Determine date range for DELETE
    dates = long_df["date"].unique().sort()
    min_date = dates[0]
    max_date = dates[-1]

    try:
        conn = get_connection()
        with conn:
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM ranks WHERE date >= %s AND date <= %s",
                (
                    min_date.isoformat() if isinstance(min_date, date) else str(min_date),
                    max_date.isoformat() if isinstance(max_date, date) else str(max_date),
                ),
            )
            _chunked_executemany(
                cur,
                "INSERT INTO ranks (id, asset_id, date, rank) VALUES (%s, %s, %s, %s)",
                params,
            )
        conn.close()
        logger.info("Wrote %d rows to ranks", len(params))
    except Exception:
        logger.error("Failed to write ranks", exc_info=True)
        raise


@task(cache_policy=NO_CACHE)
def write_review_details(entries_dfs: list[pl.DataFrame], membership_dfs: list[pl.DataFrame]) -> None:
    """Join entries with membership and write to review_details table."""
    if not entries_dfs:
        logger.warning("No review details to write")
        return

    all_review_dates: set[str] = set()
    params: list[tuple] = []

    for entries_df, membership_df in zip(entries_dfs, membership_dfs, strict=True):
        # Left-join entries with membership (is_member=True only) to get entry_reason
        members = membership_df.filter(pl.col("is_member")).select("internal_key", "entry_reason")
        joined = entries_df.join(members, on="internal_key", how="left")

        for row in joined.iter_rows(named=True):
            review_date = row["review_date"]
            review_date_str = review_date.isoformat() if isinstance(review_date, date) else str(review_date)
            all_review_dates.add(review_date_str)

            asset_id = _hash_to_bigint(row["internal_key"])
            row_id = _hash_to_bigint(str(asset_id), review_date_str)

            entry_reason = row.get("entry_reason")

            params.append(
                (
                    row_id,
                    asset_id,
                    review_date_str,
                    row.get("ff_mcap"),
                    row.get("comment"),
                    entry_reason,
                )
            )

    if not params:
        logger.warning("No review detail rows to write")
        return

    try:
        conn = get_connection()
        with conn:
            cur = conn.cursor()
            sorted_dates = sorted(all_review_dates)
            cur.execute(
                sql.SQL("DELETE FROM review_details WHERE review_date IN ({})").format(
                    sql.SQL(",").join(sql.Placeholder() for _ in sorted_dates)
                ),
                sorted_dates,
            )
            _chunked_executemany(
                cur,
                "INSERT INTO review_details (id, asset_id, review_date, ff_mcap, comment, entry_reason) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                params,
            )
        conn.close()
        logger.info("Wrote %d rows to review_details", len(params))
    except Exception:
        logger.error("Failed to write review_details", exc_info=True)
        raise
