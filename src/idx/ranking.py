"""Build and validate wide-format ranking tables from in-memory DataFrames."""

from __future__ import annotations

from datetime import date

import polars as pl
from prefect import task
from prefect.cache_policies import NO_CACHE

from idx import get_logger


@task(cache_policy=NO_CACHE)
def build_ranking_table(
    assets_df: pl.DataFrame,
    entries_dfs: list[pl.DataFrame],
    membership_dfs: list[pl.DataFrame],
    review_dates: list[date],
) -> pl.DataFrame:
    """Build a wide-format ranking table with RICs as columns and daily dates as rows.

    Members get their rank; non-members get null. Uses 0 as a sentinel during
    forward-fill to correctly propagate exits.

    Args:
        assets_df: Deduplicated assets DataFrame with 'internal_key' and 'ric' columns.
        entries_dfs: One entries DataFrame per review date (aligned with review_dates).
        membership_dfs: One membership DataFrame per review date (aligned with review_dates).
        review_dates: Sorted list of review dates.

    Returns:
        DataFrame with a ``date`` column and one column per RIC containing forward-filled ranks.
    """
    if not review_dates:
        return pl.DataFrame({"date": []}).cast({"date": pl.Date})

    # Build internal_key -> RIC lookup from assets
    key_to_ric: dict[str, str] = {}
    if "ric" in assets_df.columns and "internal_key" in assets_df.columns:
        for row in assets_df.select(["internal_key", "ric"]).iter_rows():
            key_to_ric[row[0]] = row[1]

    all_known_rics: set[str] = set()
    long_rows: list[dict] = []

    for rd, entries_df, membership_df in zip(review_dates, entries_dfs, membership_dfs, strict=True):
        # Join RIC from assets if entries lack a ric column
        if "ric" not in entries_df.columns:
            if not key_to_ric:
                continue
            ric_series = entries_df["internal_key"].map_elements(lambda x: key_to_ric.get(x), return_dtype=pl.Utf8)
            entries_df = entries_df.with_columns(ric_series.alias("ric"))

        entries_df = entries_df.filter(pl.col("ric").is_not_null())

        # Get member keys
        member_keys = set(membership_df.filter(pl.col("is_member"))["internal_key"].to_list())

        # Build ric->rank for members, deduplicate
        ric_rank: dict[str, int] = {}
        for row in entries_df.iter_rows(named=True):
            ric = row["ric"]
            if ric in ric_rank:
                continue
            all_known_rics.add(ric)
            if row["internal_key"] in member_keys:
                ric_rank[ric] = row["rank"]

        # Re-rank members 1-600 by original rank order
        sorted_rics = sorted(ric_rank.items(), key=lambda x: x[1])
        ric_rank = {ric: i + 1 for i, (ric, _) in enumerate(sorted_rics)}

        # Members get their rank; all other known RICs get 0 (sentinel for exit)
        for ric in all_known_rics:
            rank = ric_rank.get(ric, 0)
            long_rows.append({"date": rd, "ric": ric, "rank": rank})

    if not long_rows:
        return pl.DataFrame({"date": []}).cast({"date": pl.Date})

    long_df = pl.DataFrame(long_rows).with_columns(pl.col("date").cast(pl.Date))

    # Pivot to wide format: rows=date, columns=RIC, values=rank
    wide_df = long_df.pivot(on="ric", index="date", values="rank")

    # Expand to daily date range
    min_date: date = wide_df["date"].min()  # type: ignore[assignment]
    max_date = date.today()
    daily_dates = pl.DataFrame({"date": pl.date_range(min_date, max_date, eager=True)})

    # Join with daily range, sort, forward-fill
    result = daily_dates.join(wide_df, on="date", how="left").sort("date")
    ric_cols = [c for c in result.columns if c != "date"]
    result = result.with_columns(pl.col(c).forward_fill() for c in ric_cols)

    # Replace sentinel 0 with null
    result = result.with_columns(pl.when(pl.col(c) == 0).then(None).otherwise(pl.col(c)).alias(c) for c in ric_cols)

    return result


@task(cache_policy=NO_CACHE)
def validate_ranking_table(ranking_df: pl.DataFrame, review_dates: list[date]) -> None:
    """Check that each review date row in the ranking table has ranks covering 1-600.

    Args:
        ranking_df: Wide-format ranking DataFrame (date column + RIC columns).
        review_dates: Review dates that should be validated.
    """
    logger = get_logger()
    ric_cols = [c for c in ranking_df.columns if c != "date"]

    if not ric_cols or ranking_df.is_empty():
        logger.warning("Ranking table is empty, skipping validation")
        return

    for rd in review_dates:
        row = ranking_df.filter(pl.col("date") == rd)
        if row.is_empty():
            logger.warning("Ranking validation: no row for review date %s", rd)
            continue

        ranks = set()
        for col in ric_cols:
            val = row[col][0]
            if val is not None:
                ranks.add(int(val))

        expected = set(range(1, 601))
        missing = expected - ranks
        if missing:
            logger.warning(
                "Ranking validation FAILED for %s: missing %d ranks in 1-600 (e.g. %s). Only %d distinct ranks found.",
                rd,
                len(missing),
                sorted(missing)[:10],
                len(ranks),
            )
        else:
            logger.info("Ranking validation passed for %s: ranks 1-600 all present (%d total ranks)", rd, len(ranks))
