"""Tests for STOXX selection list download module."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import httpx
import pytest
import respx

from idx.download import (
    build_url,
    download_file,
    download_selection_lists,
    get_periods,
)

# --- Period generation tests ---


class TestGetPeriods:
    """Tests for get_periods."""

    def test_full_range_2015_2024_returns_37_periods(self):
        """Full range from 2015 to end of 2024 yields 37 periods."""
        periods = get_periods(date(2015, 1, 1), date(2024, 12, 31))
        assert len(periods) == 37

    def test_2015_only_oct_nov(self):
        """2015 has only Oct and Nov available."""
        periods = get_periods(date(2015, 1, 1), date(2015, 12, 31))
        assert periods == [(2015, 10), (2015, 11)]

    def test_2018_missing_aug(self):
        """2018 has Feb, May, Nov (no Aug)."""
        periods = get_periods(date(2018, 1, 1), date(2018, 12, 31))
        assert periods == [(2018, 2), (2018, 5), (2018, 11)]

    def test_2021_standard_quarterly(self):
        """2021 uses standard quarterly months."""
        periods = get_periods(date(2021, 1, 1), date(2021, 12, 31))
        assert periods == [(2021, 3), (2021, 6), (2021, 9), (2021, 12)]

    def test_partial_year_filtering(self):
        """Only periods within the date range are included."""
        periods = get_periods(date(2021, 4, 1), date(2021, 10, 31))
        assert periods == [(2021, 6), (2021, 9)]

    def test_empty_range(self):
        """Start after end returns empty list."""
        periods = get_periods(date(2025, 1, 1), date(2024, 1, 1))
        assert periods == []

    def test_4_files_per_year_from_2024(self):
        """Each full year from 2024 onward has exactly 4 periods."""
        for year in [2024, 2025, 2026]:
            periods = get_periods(date(year, 1, 1), date(year, 12, 31))
            assert len(periods) == 4
            assert periods == [(year, 3), (year, 6), (year, 9), (year, 12)]


# --- URL construction tests ---


class TestBuildUrl:
    """Tests for build_url."""

    def test_pdf_url_format(self):
        """PDF URL contains correct month name, symbol, and ym code."""
        url, filename = build_url(2022, 3, "sxxp")
        assert "SelectionList/2022/March" in url
        assert "sl_sxxp_202203.pdf" in url
        assert filename == "sl_sxxp_202203.pdf"

    def test_csv_url_format(self):
        """CSV URL contains correct ymd format."""
        url, filename = build_url(2024, 3, "sxxp", day=5)
        assert "STOXXSelectionList/2024/March" in url
        assert "slpublic_sxxp_20240305.csv" in url
        assert filename == "slpublic_sxxp_20240305.csv"

    def test_csv_cutoff_boundary(self):
        """Sep 2023 produces PDF, Dec 2023 produces CSV."""
        url_sep, _ = build_url(2023, 9, "sxxp")
        url_dec, _ = build_url(2023, 12, "sxxp")
        assert url_sep.endswith(".pdf")
        assert url_dec.endswith(".csv")


# --- Download tests (mocked with respx) ---


class TestDownloadFile:
    """Tests for download_file."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_download_success(self, tmp_path: Path):
        """Successful download writes file and returns True."""
        url = "https://example.com/test.pdf"
        respx.get(url).mock(return_value=httpx.Response(200, content=b"fake pdf"))
        dest = tmp_path / "test.pdf"

        async with httpx.AsyncClient() as client:
            result = await download_file(client, url, dest)

        assert result is True
        assert dest.read_bytes() == b"fake pdf"

    @respx.mock
    @pytest.mark.asyncio
    async def test_download_404(self, tmp_path: Path):
        """404 response returns False without writing file."""
        url = "https://example.com/missing.pdf"
        respx.get(url).mock(return_value=httpx.Response(404))
        dest = tmp_path / "missing.pdf"

        async with httpx.AsyncClient() as client:
            result = await download_file(client, url, dest)

        assert result is False
        assert not dest.exists()


class TestDownloadSelectionLists:
    """Tests for download_selection_lists with CSV and PDF download logic."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_pdf_download_success(self, tmp_path: Path):
        """PDF period downloads a single file without day iteration."""
        url, _ = build_url(2022, 3, "sxxp")
        respx.get(url).mock(return_value=httpx.Response(200, content=b"pdf data"))
        respx.route().mock(return_value=httpx.Response(200, content=b"{}"))

        result = await download_selection_lists(
            start=date(2022, 3, 1),
            end=date(2022, 3, 31),
            output_dir=tmp_path,
            symbol="sxxp",
        )

        assert len(result.downloaded) == 1
        assert result.downloaded[0].name == "sl_sxxp_202203.pdf"
        assert (tmp_path / "sl_sxxp_202203.pdf").read_bytes() == b"pdf data"

    @respx.mock
    @pytest.mark.asyncio
    async def test_default_end_date(self, tmp_path: Path):
        """When end is not provided, it defaults to today."""
        # Use a far-future start so no periods are generated
        respx.route().mock(return_value=httpx.Response(404))
        result = await download_selection_lists(
            start=date(2099, 1, 1),
            output_dir=tmp_path,
            symbol="sxxp",
        )

        assert result.downloaded == []
        assert result.missed == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_csv_tries_multiple_days(self, tmp_path: Path):
        """CSV download tries multiple days until one succeeds."""
        # Set up: Dec 2023, days 1-4 fail, day 5 succeeds
        for day in range(1, 5):
            url, _ = build_url(2023, 12, "sxxp", day=day)
            respx.get(url).mock(return_value=httpx.Response(404))

        url_day5, _ = build_url(2023, 12, "sxxp", day=5)
        respx.get(url_day5).mock(return_value=httpx.Response(200, content=b"csv data"))

        # Mock remaining days to 404 (won't be reached but respx needs them unmatched to be ok)
        respx.route().mock(return_value=httpx.Response(404))

        result = await download_selection_lists(
            start=date(2023, 12, 1),
            end=date(2023, 12, 31),
            output_dir=tmp_path,
            symbol="sxxp",
        )

        assert len(result.downloaded) == 1
        assert result.downloaded[0].name == "slpublic_sxxp_20231205.csv"
        assert len(result.missed) == 0

    @respx.mock
    @pytest.mark.asyncio
    async def test_csv_fallback_to_next_month(self, tmp_path: Path):
        """CSV search falls back to the next month when the expected month has no files."""
        # Mar 2024 - all days 404. Apr 2024 day 2 succeeds.
        # The fallback should search Mar (all 404), then try Apr.
        # Register specific route first so it takes priority over the catch-all.
        url_apr2, _ = build_url(2024, 4, "sxxp", day=2)
        respx.get(url_apr2).mock(return_value=httpx.Response(200, content=b"csv found"))
        respx.route().mock(return_value=httpx.Response(404))

        result = await download_selection_lists(
            start=date(2024, 3, 1),
            end=date(2024, 3, 31),
            output_dir=tmp_path,
            symbol="sxxp",
        )

        assert len(result.downloaded) == 1
        assert "04" in result.downloaded[0].name  # Found in April

    @respx.mock
    @pytest.mark.asyncio
    async def test_csv_fallback_stops_before_next_quarter(self, tmp_path: Path):
        """CSV fallback search does not bleed into the next quarterly period."""
        # Mar 2024 period: should search Mar, Apr, May but NOT Jun (next quarter)
        # All return 404 -> period is missed
        respx.route().mock(return_value=httpx.Response(404))

        result = await download_selection_lists(
            start=date(2024, 3, 1),
            end=date(2024, 3, 31),
            output_dir=tmp_path,
            symbol="sxxp",
        )

        assert len(result.missed) == 1
        assert result.missed[0] == (2024, 3)

        # Verify no June URLs were attempted
        for call in respx.calls:
            assert "June" not in str(call.request.url)
