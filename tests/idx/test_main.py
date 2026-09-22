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


@pytest.mark.asyncio
@patch("idx.main.SlackWebhook", new_callable=AsyncMock)
@patch("idx.main.download_selection_lists", new_callable=AsyncMock)
async def test_main_slack_notification_on_error(mock_download, mock_slack):
    """Slack notification is sent when the pipeline fails."""
    from idx.main import main

    mock_download.side_effect = RuntimeError("download failed")
    mock_webhook = AsyncMock()
    mock_slack.load.return_value = mock_webhook

    with pytest.raises(RuntimeError, match="download failed"):
        await main.fn()

    mock_webhook.notify.assert_called_once()
    assert "download failed" in mock_webhook.notify.call_args[0][0]


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
async def test_main_empty_entries_skipped(
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
    """Files that parse to empty entries are skipped with a warning."""
    from idx.main import main

    mock_download.return_value = MagicMock(downloaded=[Path("empty.csv")])
    mock_parse.return_value = ([], [])

    await main.fn()

    mock_parse.assert_called_once()
    mock_membership.assert_not_called()


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
async def test_main_merges_duplicate_review_dates(
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
    """Multiple files for the same review date are merged."""
    from idx.main import main

    rd = date(2024, 9, 1)
    assets1 = [Asset(internal_key="K1", ric="R1", name="N1", country="DE", currency="EUR")]
    entries1 = [SelectionListEntry(internal_key="K1", review_date=rd, ff_mcap=100.0, rank=1)]
    assets2 = [Asset(internal_key="K2", ric="R2", name="N2", country="FR", currency="EUR")]
    entries2 = [SelectionListEntry(internal_key="K2", review_date=rd, ff_mcap=200.0, rank=2)]

    mock_download.return_value = MagicMock(downloaded=[Path("a.csv"), Path("b.csv")])
    mock_parse.side_effect = [(assets1, entries1), (assets2, entries2)]

    from idx.extract import EntryReason, IndexMembership

    mock_membership.return_value = [
        IndexMembership(internal_key="K1", is_member=True, entry_reason=EntryReason.BOOTSTRAP),
        IndexMembership(internal_key="K2", is_member=True, entry_reason=EntryReason.BOOTSTRAP),
    ]
    mock_intervals.return_value = pl.DataFrame(
        {"internal_key": ["K1", "K2"], "first_included": [rd, rd], "last_included": [rd, rd]},
        schema={"internal_key": pl.Utf8, "first_included": pl.Date, "last_included": pl.Date},
    )
    mock_resolve.return_value = pl.DataFrame(
        {
            "internal_key": ["K1", "K2"],
            "ric": ["R1", "R2"],
            "name": ["N1", "N2"],
            "country": ["DE", "FR"],
            "currency": ["EUR", "EUR"],
            "isin": [None, None],
            "sedol": [None, None],
            "yukka_id": [None, None],
            "first_included": [rd, rd],
            "last_included": [rd, rd],
        }
    )
    mock_build.return_value = pl.DataFrame({"date": [rd], "R1": [1], "R2": [2]})

    await main.fn()

    # Both files parsed, but only one review date processed
    assert mock_parse.call_count == 2
    mock_membership.assert_called_once()
    # The merged entries should contain both K1 and K2
    call_entries = mock_membership.call_args[0][0]
    keys = {e.internal_key for e in call_entries}
    assert keys == {"K1", "K2"}
