"""Build and validate long-format daily membership tables from in-memory DataFrames."""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
from prefect import task
from prefect.cache_policies import NO_CACHE

from idx import get_logger

_ASSET_COLS = ("ric", "name", "country", "currency", "isin", "sedol")


def _build_review_members(
    assets_df: pl.DataFrame,
    entries_df: pl.DataFrame,
    membership_df: pl.DataFrame,
) -> pl.DataFrame:
    """Build member rows for one review date, re-ranked 1..N.

    Joins entries with asset identifiers so that every member row carries the
    point-in-time values for ric, name, country, currency, isin, and sedol.

    Args:
        assets_df: Assets DataFrame for this review date.
        entries_df: Entries DataFrame for this review date.
        membership_df: Membership DataFrame for this review date.

    Returns:
        DataFrame with columns [internal_key, ric, name, country, currency,
        isin, sedol, rank] for members only.
    """
    member_keys = set(membership_df.filter(pl.col("is_member"))["internal_key"].to_list())

    members_df = (
        entries_df.filter(pl.col("rank").is_not_null() & pl.col("internal_key").is_in(list(member_keys)))
        .sort("rank", "internal_key")
        .unique(subset=["internal_key"], keep="first")
        .sort("rank", "internal_key")
    )

    # Join asset identifiers onto member entries
    available_cols = [c for c in _ASSET_COLS if c in assets_df.columns]
    if available_cols:
        asset_cols_df = assets_df.select(["internal_key", *available_cols]).unique(subset=["internal_key"], keep="last")
        members_df = members_df.join(asset_cols_df, on="internal_key", how="left", suffix="_asset")
        # Prefer asset value; fall back to entry value if present
        for col in available_cols:
            asset_col = f"{col}_asset"
            if asset_col in members_df.columns:
                members_df = members_df.with_columns(pl.coalesce(pl.col(asset_col), pl.col(col)).alias(col)).drop(
                    asset_col
                )

    # Select output columns, adding any missing ones as null
    out_cols = ["internal_key"]
    for col in _ASSET_COLS:
        if col not in members_df.columns:
            members_df = members_df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
        out_cols.append(col)

    members_df = members_df.select(out_cols).with_row_index("rank", offset=1).cast({"rank": pl.Int64})

    return members_df


_OUTPUT_SCHEMA = {
    "date": pl.Date,
    "internal_key": pl.Utf8,
    "ric": pl.Utf8,
    "name": pl.Utf8,
    "country": pl.Utf8,
    "currency": pl.Utf8,
    "isin": pl.Utf8,
    "sedol": pl.Utf8,
    "rank": pl.Int64,
    "yukka_id": pl.Utf8,
}


@task(cache_policy=NO_CACHE)
def build_membership_table(
    assets_dfs: list[pl.DataFrame],
    entries_dfs: list[pl.DataFrame],
    membership_dfs: list[pl.DataFrame],
    review_dates: list[date],
) -> pl.DataFrame:
    """Build a long-format daily membership table with point-in-time identifiers.

    For each review date, members get their re-ranked position (1..N) and all
    identifiers from that period's selection list.  Between review dates the
    values are forward-filled daily.  When a field is null or empty, the last
    known value for that internal_key is carried forward.

    Args:
        assets_dfs: One assets DataFrame per review date (aligned with review_dates).
        entries_dfs: One entries DataFrame per review date (aligned with review_dates).
        membership_dfs: One membership DataFrame per review date (aligned with review_dates).
        review_dates: Sorted list of review dates.

    Returns:
        DataFrame with columns [date, internal_key, ric, name, country,
        currency, isin, sedol, rank] — one row per member per calendar day.
    """
    if not review_dates:
        return pl.DataFrame(schema=_OUTPUT_SCHEMA)

    logger = get_logger()

    # 1. Build per-review-date member snapshots
    snapshots: list[pl.DataFrame] = []
    for rd, assets_df, entries_df, membership_df in zip(
        review_dates, assets_dfs, entries_dfs, membership_dfs, strict=True
    ):
        members = _build_review_members(assets_df, entries_df, membership_df)
        if not members.is_empty():
            snapshots.append(members.with_columns(pl.lit(rd).cast(pl.Date).alias("review_date")))

    if not snapshots:
        return pl.DataFrame(schema=_OUTPUT_SCHEMA)

    # 2. Forward-fill null/empty string columns across review dates per internal_key
    all_snapshots = pl.concat(snapshots).sort("review_date")
    str_cols = [c for c in _ASSET_COLS if c in all_snapshots.columns]
    all_snapshots = all_snapshots.with_columns(
        pl.when(pl.col(c).is_not_null() & (pl.col(c) != "")).then(pl.col(c)).otherwise(None).alias(c) for c in str_cols
    ).with_columns(pl.col(c).forward_fill().over("internal_key") for c in str_cols)

    # 3. Expand to daily rows
    max_date = date.today()
    daily_parts: list[pl.DataFrame] = []
    for i, rd in enumerate(review_dates):
        end = review_dates[i + 1] if i + 1 < len(review_dates) else max_date + timedelta(days=1)
        snapshot = all_snapshots.filter(pl.col("review_date") == rd).drop("review_date")
        if snapshot.is_empty():
            continue
        dates = pl.date_range(rd, end - timedelta(days=1), eager=True)
        date_df = pl.DataFrame({"date": dates})
        daily_parts.append(date_df.join(snapshot, how="cross"))

    result = pl.concat(daily_parts).sort("date", "rank")
    logger.info("Built membership table: %d rows, %d unique members", len(result), result["internal_key"].n_unique())
    return result


_ASSETS_ID_COLS = ("ric", "name", "country", "currency", "isin", "sedol", "yukka_id")


@task(cache_policy=NO_CACHE)
def build_assets_from_membership(membership_df: pl.DataFrame) -> pl.DataFrame:
    """Derive the assets table from an enriched membership table.

    Groups by internal_key, taking the most recent non-null value for each
    identifier column and computing first/last included dates.

    Args:
        membership_df: Enriched membership DataFrame with yukka_id column.

    Returns:
        DataFrame with columns [internal_key, ric, name, country, currency,
        isin, sedol, yukka_id, first_included, last_included].
    """
    if membership_df.is_empty():
        schema = {"internal_key": pl.Utf8}
        for col in _ASSETS_ID_COLS:
            schema[col] = pl.Utf8
        schema["first_included"] = pl.Date
        schema["last_included"] = pl.Date
        return pl.DataFrame(schema=schema)

    available = [c for c in _ASSETS_ID_COLS if c in membership_df.columns]
    agg_exprs = [pl.col(c).drop_nulls().last().alias(c) for c in available]
    agg_exprs.append(pl.col("date").min().alias("first_included"))
    agg_exprs.append(pl.col("date").max().alias("last_included"))

    result = membership_df.group_by("internal_key").agg(agg_exprs)

    # Add missing identifier columns as null
    for col in _ASSETS_ID_COLS:
        if col not in result.columns:
            result = result.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    col_order = ["internal_key", *_ASSETS_ID_COLS, "first_included", "last_included"]
    return result.select(col_order).sort("internal_key")


@task(cache_policy=NO_CACHE)
def validate_membership_table(membership_df: pl.DataFrame, review_dates: list[date]) -> None:
    """Check that each review date has ranks covering 1-600.

    Args:
        membership_df: Long-format membership DataFrame.
        review_dates: Review dates that should be validated.
    """
    logger = get_logger()
    if membership_df.is_empty():
        logger.warning("Membership table is empty, skipping validation")
        return

    for rd in review_dates:
        day_df = membership_df.filter(pl.col("date") == rd)
        if day_df.is_empty():
            logger.warning("Membership validation: no rows for review date %s", rd)
            continue

        ranks = set(day_df["rank"].to_list())
        expected = set(range(1, 601))
        missing = expected - ranks
        if missing:
            logger.warning(
                "Membership validation FAILED for %s: missing %d ranks in 1-600 (e.g. %s). Only %d distinct ranks.",
                rd,
                len(missing),
                sorted(missing)[:10],
                len(ranks),
            )
        else:
            logger.info("Membership validation passed for %s: ranks 1-600 all present (%d total)", rd, len(ranks))
