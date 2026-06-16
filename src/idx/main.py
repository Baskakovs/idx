"""Download STOXX selection lists and compute index membership."""

import asyncio
from dataclasses import asdict
from datetime import date

import polars as pl
from prefect import flow
from prefect.artifacts import acreate_markdown_artifact

from idx import get_logger
from idx.download import download_selection_lists
from idx.enrichment import report_unresolved_assets, resolve_yukka_ids
from idx.extract import compute_membership, parse_selection_list
from idx.ranking import build_ranking_table, validate_ranking_table
from idx.storage import write_assets, write_ranks, write_reviews


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
    result = await download_selection_lists(periods=periods)

    if not result.downloaded:
        logger.warning("No files downloaded — nothing to process")
        return

    # Parse all downloaded files and group by review_date
    review_date_groups: dict[date, tuple[list, list]] = {}
    parse_failures: list[str] = []
    for filepath in result.downloaded:
        assets, entries = parse_selection_list(filepath)
        if entries:
            rd = entries[0].review_date
            logger.info(
                "Parsed %s → review date %s (%d assets, %d entries)", filepath.name, rd, len(assets), len(entries)
            )
            if rd in review_date_groups:
                existing_assets, existing_entries = review_date_groups[rd]
                existing_assets.extend(assets)
                existing_entries.extend(entries)
            else:
                review_date_groups[rd] = (assets, entries)
        else:
            logger.warning("No entries parsed from %s", filepath.name)
            parse_failures.append(filepath.name)

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
        logger.warning("No review dates parsed — nothing to process")
        return

    # Filter to assets that were ever index members
    ever_member_keys = set()
    for mdf in membership_dfs:
        ever_member_keys.update(mdf.filter(pl.col("is_member"))["internal_key"].to_list())

    all_assets = (
        pl.concat(assets_dfs)
        .unique(subset=["internal_key"])
        .filter(pl.col("internal_key").is_in(list(ever_member_keys)))
    )
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

    # Build detailed summary
    review_lines = []
    for entries_df, membership_df, rd in zip(entries_dfs, membership_dfs, sorted_dates, strict=True):
        members = membership_df.filter(pl.col("is_member"))
        review_lines.append(f"| {rd} | {len(entries_df)} | {len(members)} |")

    downloaded_lines = [f"| `{p.name}` |" for p in result.downloaded]
    missed_lines = [f"| {y}-{m:02d} |" for y, m in result.missed]

    summary_parts = [
        "## Pipeline Summary\n",
        f"**Downloaded:** {len(result.downloaded)} files, **Failed:** {len(result.missed)} periods\n",
    ]
    if parse_failures:
        summary_parts.append(f"**Parse failures:** {', '.join(parse_failures)}\n")
    summary_parts.extend(
        [
            f"**Total assets:** {len(enriched_assets)} ({unique_isins} unique ISINs)\n",
            f"**Rankings:** {len(ranking_df)} daily rows\n",
            "\n### Downloads\n",
            "| File |",
            "|------|",
            *downloaded_lines,
        ]
    )
    if missed_lines:
        summary_parts.extend(
            [
                "\n### Failed Periods\n",
                "| Period |",
                "|--------|",
                *missed_lines,
            ]
        )
    summary_parts.extend(
        [
            "\n### Reviews\n",
            "| Review Date | Entries | Members |",
            "|-------------|---------|---------|",
            *review_lines,
        ]
    )

    await acreate_markdown_artifact(
        key="pipeline-summary",
        markdown="\n".join(summary_parts),
        description="Pipeline run summary",
    )


if __name__ == "__main__":
    asyncio.run(main())
