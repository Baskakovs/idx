"""Tests for idx.main module."""

from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from idx.extract import Asset, SelectionListEntry


@pytest.mark.asyncio
@patch("idx.main.SlackWebhook", new_callable=AsyncMock)
@patch("idx.main.write_reviews")
@patch("idx.main.write_ranks")
@patch("idx.main.write_assets")
@patch("idx.main.validate_ranking_table")
@patch("idx.main.build_ranking_table")
@patch("idx.main.report_unresolved_assets")
@patch("idx.main.resolve_yukka_ids")
@patch("idx.main.compute_membership")
@patch("idx.main.parse_selection_list")
@patch("idx.main.download_selection_lists", new_callable=AsyncMock)
async def test_main_empty_download(
    mock_download,
    mock_parse,
    mock_membership,
    mock_resolve,
    mock_report,
    mock_build,
    mock_validate,
    mock_write_assets,
    mock_write_ranks,
    mock_write_details,
    mock_slack,
):
    """Pipeline handles empty download result without errors."""
    from idx.main import main

    mock_download.return_value = MagicMock(downloaded=[])
    await main.fn()
    mock_download.assert_called_once()
    mock_parse.assert_not_called()


@pytest.mark.asyncio
@patch("idx.main.SlackWebhook", new_callable=AsyncMock)
@patch("idx.main.write_reviews")
@patch("idx.main.write_ranks")
@patch("idx.main.write_assets")
@patch("idx.main.validate_ranking_table")
@patch("idx.main.build_ranking_table")
@patch("idx.main.report_unresolved_assets")
@patch("idx.main.resolve_yukka_ids")
@patch("idx.main.compute_membership_intervals")
@patch("idx.main.compute_membership")
@patch("idx.main.parse_selection_list")
@patch("idx.main.download_selection_lists", new_callable=AsyncMock)
async def test_main_full_pipeline(
    mock_download,
    mock_parse,
    mock_membership,
    mock_intervals,
    mock_resolve,
    mock_report,
    mock_build,
    mock_validate,
    mock_write_assets,
    mock_write_ranks,
    mock_write_details,
    mock_slack,
):
    """Pipeline processes downloaded files through all stages."""
    from idx.main import main

    rd = date(2024, 9, 1)
    assets = [Asset(internal_key="K1", ric="R1", name="N1", country="DE", currency="EUR", isin="IS1")]
    entries = [SelectionListEntry(internal_key="K1", review_date=rd, ff_mcap=100.0, rank=1)]

    mock_download.return_value = MagicMock(downloaded=[Path("test.csv")])
    mock_parse.return_value = (assets, entries)

    from idx.extract import EntryReason, IndexMembership

    mock_membership.return_value = [
        IndexMembership(internal_key="K1", is_member=True, entry_reason=EntryReason.BOOTSTRAP),
    ]
    mock_intervals.return_value = pl.DataFrame(
        {
            "internal_key": ["K1"],
            "first_included": [rd],
            "last_included": [rd],
        },
        schema={"internal_key": pl.Utf8, "first_included": pl.Date, "last_included": pl.Date},
    )

    enriched = pl.DataFrame(
        {
            "internal_key": ["K1"],
            "ric": ["R1"],
            "name": ["N1"],
            "country": ["DE"],
            "currency": ["EUR"],
            "isin": ["IS1"],
            "sedol": [None],
            "yukka_id": ["YK1"],
            "first_included": [rd],
            "last_included": [rd],
        }
    )
    mock_resolve.return_value = enriched
    ranking = pl.DataFrame({"date": [rd], "R1": [1]})
    mock_build.return_value = ranking

    await main.fn()

    mock_parse.assert_called_once()
    mock_membership.assert_called_once()
    mock_resolve.assert_called_once()
    mock_report.assert_called_once()
    mock_build.assert_called_once()
    mock_validate.assert_called_once()
    mock_write_assets.assert_called_once()
    mock_write_ranks.assert_called_once()
    mock_write_details.assert_called_once()
