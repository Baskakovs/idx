"""Tests for idx.main module."""

from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from idx.extract import Asset, SelectionListEntry


@pytest.mark.asyncio
@patch("idx.main.write_review_details")
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
):
    """Pipeline handles empty download result without errors."""
    from idx.main import main

    mock_download.return_value = MagicMock(downloaded=[])
    await main()
    mock_download.assert_called_once()
    mock_parse.assert_not_called()


@pytest.mark.asyncio
@patch("idx.main.write_review_details")
@patch("idx.main.write_ranks")
@patch("idx.main.write_assets")
@patch("idx.main.validate_ranking_table")
@patch("idx.main.build_ranking_table")
@patch("idx.main.report_unresolved_assets")
@patch("idx.main.resolve_yukka_ids")
@patch("idx.main.download_selection_lists", new_callable=AsyncMock)
async def test_main_full_pipeline(
    mock_download,
    mock_resolve,
    mock_report,
    mock_build,
    mock_validate,
    mock_write_assets,
    mock_write_ranks,
    mock_write_details,
):
    """Pipeline processes downloaded files through all stages."""
    from idx.main import main

    rd = date(2024, 9, 1)
    [Asset(internal_key="K1", ric="R1", name="N1", country="DE", currency="EUR")]
    [SelectionListEntry(internal_key="K1", review_date=rd, ff_mcap=100.0, rank=1)]

    # Create a real CSV file for parse_selection_list to parse
    csv_content = (
        "Internal Key;RIC;Instrument Name;Country;Currency;ISIN;SEDOL;"
        "Rank Final;FF MCap (MEUR);Comment;Creation Date\n"
        "K1;R1;N1;DE;EUR;IS1;;1;100.0;;20240901\n"
    )
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f:
        f.write(csv_content)
        csv_path = Path(f.name)

    mock_download.return_value = MagicMock(downloaded=[csv_path])

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
        }
    )
    mock_resolve.return_value = enriched
    ranking = pl.DataFrame({"date": [rd], "R1": [1]})
    mock_build.return_value = ranking

    await main()

    mock_resolve.assert_called_once()
    mock_report.assert_called_once()
    mock_build.assert_called_once()
    mock_validate.assert_called_once()
    mock_write_assets.assert_called_once()
    mock_write_ranks.assert_called_once()
    mock_write_details.assert_called_once()

    csv_path.unlink(missing_ok=True)
