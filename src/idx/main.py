"""Download STOXX selection lists and compute index membership."""

import asyncio
import logging
from dataclasses import asdict
from datetime import date

import polars as pl
from prefect import flow
from prefect.artifacts import acreate_markdown_artifact

from idx.download import download_selection_lists
from idx.enrichment import report_unresolved_assets, resolve_yukka_ids
from idx.extract import compute_membership, parse_selection_list
from idx.ranking import build_ranking_table, validate_ranking_table
from idx.storage import write_assets, write_ranks, write_reviews

logger = logging.getLogger(__name__)


@flow(name="stoxx-600-scraper", log_prints=True)
async def main(
    periods: list[tuple[int, int]] | None = None,
) -> None:
    """Download, parse, and process STOXX selection lists.

    Args:
        periods: Explicit (year, month) tuples to download.
            When None, downloads the full historical range.
    """
    result = await download_selection_lists(periods=periods)

    # Parse all downloaded files and group by review_date
    review_date_groups: dict[date, tuple[list, list]] = {}
    for filepath in result.downloaded:
        assets, entries = parse_selection_list(filepath)
        if entries:
            rd = entries[0].review_date
            if rd in review_date_groups:
                existing_assets, existing_entries = review_date_groups[rd]
                existing_assets.extend(assets)
                existing_entries.extend(entries)
            else:
                review_date_groups[rd] = (assets, entries)

    sorted_dates = sorted(review_date_groups.keys())

    # Static security identifiers (RIC, ISIN, SEDOL, country, currency)
    assets_dfs: list[pl.DataFrame] = []
    # Per-review snapshot: rank, free-float mcap, and comments for each security
    entries_dfs: list[pl.DataFrame] = []
    # Computed index membership per review: who is in/out and why (top 550, buffer, fill)
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

    if not assets_dfs:
        return

    # Merge, enrich, and report
    all_assets = pl.concat(assets_dfs).unique(subset=["internal_key"])
    unique_isins = all_assets["isin"].n_unique() if "isin" in all_assets.columns else 0
    logger.info("Built %d asset rows (%d unique ISINs)", len(all_assets), unique_isins)
    enriched_assets = resolve_yukka_ids(all_assets)
    report_unresolved_assets(enriched_assets)

    # Build and validate ranking table
    ranking_df = build_ranking_table(enriched_assets, entries_dfs, membership_dfs, sorted_dates)
    validate_ranking_table(ranking_df, sorted_dates)

    # Persist to R2 as Parquet
    write_assets(enriched_assets)
    write_ranks(ranking_df)
    for entries_df, membership_df, rd in zip(entries_dfs, membership_dfs, sorted_dates, strict=True):
        write_reviews(entries_df, membership_df, rd)

    await acreate_markdown_artifact(
        key="pipeline-summary",
        markdown=f"**Reviews processed:** {len(sorted_dates)}\n\n"
        f"**Assets:** {len(enriched_assets)} rows\n\n"
        f"**Rankings:** {len(ranking_df)} daily rows",
        description="Pipeline run summary",
    )


if __name__ == "__main__":
    asyncio.run(main())
