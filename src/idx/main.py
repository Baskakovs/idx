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
from idx.enrichment import enrich_membership_with_yukka_ids, report_unresolved_assets
from idx.extract import compute_membership, parse_selection_list
from idx.ranking import build_assets_from_membership, build_membership_table, validate_membership_table
from idx.storage import write_assets, write_membership, write_reviews


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

        membership_table = build_membership_table(assets_dfs, entries_dfs, membership_dfs, sorted_dates)
        membership_table = enrich_membership_with_yukka_ids(membership_table)
        validate_membership_table(membership_table, sorted_dates)

        assets_table = build_assets_from_membership(membership_table)
        report_unresolved_assets(assets_table)

        write_assets(assets_table)
        write_membership(membership_table)
        for entries_df, membership_df, rd in zip(entries_dfs, membership_dfs, sorted_dates, strict=True):
            write_reviews(entries_df, membership_df, rd)
    except Exception as e:
        slack = await SlackWebhook.load("yukka-notification")  # ty: ignore[invalid-await]
        await slack.notify(f"STOXX 600 scraper failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
