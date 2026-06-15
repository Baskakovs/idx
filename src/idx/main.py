"""Download STOXX selection lists and compute index membership."""

import asyncio
from datetime import date

from idx.download import download_selection_lists
from idx.extract import compute_membership, parse_selection_list


async def main() -> None:
    """Download, parse, and process STOXX selection lists."""
    result = await download_selection_lists()

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

    prior_membership: set[str] | None = None
    for rd in sorted_dates:
        assets, entries = review_date_groups[rd]

        membership = compute_membership(entries, prior_membership)

        # TODO: write_parquet_dataset(assets, entries, membership, output_dir)

        prior_membership = {m.internal_key for m in membership if m.is_member}


if __name__ == "__main__":
    asyncio.run(main())
