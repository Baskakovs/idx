"""Download STOXX selection lists and compute index membership."""

import asyncio
from dataclasses import asdict
from datetime import date
from typing import Any

import polars as pl
from prefect import flow
from prefect.blocks.notifications import SlackWebhook

from idx import get_logger
from idx.download import download_selection_lists
from idx.enrichment import report_unresolved_assets, resolve_yukka_ids
from idx.extract import compute_membership, compute_membership_intervals, parse_selection_list
from idx.ranking import build_ranking_table, validate_ranking_table
from idx.storage import write_assets, write_ranks, write_reviews


def _parse_downloaded_files(downloaded: list[Any], logger: Any) -> dict[date, tuple[list[Any], list[Any]]]:
    """Parse downloaded files and group by review date."""
    groups: dict[date, tuple[list[Any], list[Any]]] = {}
    for filepath in downloaded:
        assets, entries = parse_selection_list(filepath)
        if not entries:
            logger.warning("No entries parsed from %s", filepath.name)
            continue
        rd = entries[0].review_date
        logger.info("Parsed %s → review date %s (%d assets, %d entries)", filepath.name, rd, len(assets), len(entries))
        if rd in groups:
            groups[rd][0].extend(assets)
            groups[rd][1].extend(entries)
        else:
            groups[rd] = (assets, entries)
    return groups


def _compute_all_memberships(
    review_date_groups: dict[date, tuple[list[Any], list[Any]]],
    sorted_dates: list[date],
    logger: Any,
) -> tuple[list[pl.DataFrame], list[pl.DataFrame], list[pl.DataFrame]]:
    """Compute membership for each review date, returning assets/entries/membership DataFrames."""
    assets_dfs: list[pl.DataFrame] = []
    entries_dfs: list[pl.DataFrame] = []
    membership_dfs: list[pl.DataFrame] = []
    prior_membership: set[str] | None = None

    for rd in sorted_dates:
        assets, entries = review_date_groups[rd]
        membership = compute_membership(entries, prior_membership)
        assets_dfs.append(pl.DataFrame([asdict(a) for a in assets]))
        entries_dfs.append(pl.DataFrame([asdict(e) for e in entries], infer_schema_length=None))
        membership_dfs.append(pl.DataFrame([asdict(m) for m in membership]))
        prior_membership = {m.internal_key for m in membership if m.is_member}
        logger.info("Processed review date %s", rd)

    return assets_dfs, entries_dfs, membership_dfs


@flow(name="stoxx-600-scraper", log_prints=True)
async def main(
    periods: list[tuple[int, int]] | None = None,
) -> None:
    """Download, parse, and process STOXX selection lists.

    Args:
        periods: Explicit (year, month) tuples to download.
            When None, downloads the full historical range.
    """
    logger = get_logger()
    try:
        result = await download_selection_lists(periods=periods)
        if not result.downloaded:
            logger.warning("No files downloaded — nothing to process")
            return

        review_date_groups = _parse_downloaded_files(result.downloaded, logger)
        sorted_dates = sorted(review_date_groups.keys())
        assets_dfs, entries_dfs, membership_dfs = _compute_all_memberships(review_date_groups, sorted_dates, logger)

        if not assets_dfs:
            logger.warning("No review dates parsed — nothing to process")
            return

        intervals = compute_membership_intervals(membership_dfs, sorted_dates)
        all_assets = (
            pl.concat(assets_dfs)
            .unique(subset=["internal_key"], keep="first")
            .join(intervals, on="internal_key", how="inner")
        )
        logger.info(
            "Built %d asset rows (%d unique ISINs)",
            len(all_assets),
            all_assets["isin"].n_unique() if "isin" in all_assets.columns else 0,
        )

        enriched_assets = resolve_yukka_ids(all_assets)
        report_unresolved_assets(enriched_assets)

        ranking_df = build_ranking_table(enriched_assets, entries_dfs, membership_dfs, sorted_dates)
        validate_ranking_table(ranking_df, sorted_dates)

        write_assets(enriched_assets)
        write_ranks(ranking_df)
        for entries_df, membership_df, rd in zip(entries_dfs, membership_dfs, sorted_dates, strict=True):
            write_reviews(entries_df, membership_df, rd)
    except Exception as e:
        slack = await SlackWebhook.load("yukka-notification")  # ty: ignore[invalid-await]
        await slack.notify(f"STOXX 600 scraper failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
