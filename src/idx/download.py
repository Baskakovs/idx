"""Download STOXX selection list files (PDF and CSV) from stoxx.com."""

from __future__ import annotations

import asyncio
import calendar
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import httpx

PDF_URL_TEMPLATE = "https://www.stoxx.com/document/Reports/SelectionList/{year}/{month_name}/sl_{symbol}_{ym}.pdf"
CSV_URL_TEMPLATE = (
    "https://www.stoxx.com/document/Reports/STOXXSelectionList/{year}/{month_name}/slpublic_{symbol}_{ymd}.csv"
)

CSV_CUTOFF = date(2023, 12, 1)
START_DATE = date(2015, 10, 1)
DEFAULT_SYMBOL = "sxxp"

AVAILABLE_MONTHS: dict[int, list[int]] = {
    2015: [10, 11],
    2016: [2, 5, 8, 11],
    2017: [2, 5, 8, 11],
    2018: [2, 5, 11],
    2019: [2, 5, 8, 11],
    2020: [2, 5, 8, 11],
}

DEFAULT_QUARTERLY_MONTHS = [3, 6, 9, 12]


@dataclass
class DownloadResult:
    """Result of downloading STOXX selection list files."""

    downloaded: list[Path] = field(default_factory=list)
    missed: list[tuple[int, int]] = field(default_factory=list)


def get_periods(start: date, end: date) -> list[tuple[int, int]]:
    """Generate (year, month) tuples for available STOXX selection list periods."""
    periods = []
    for year in range(start.year, end.year + 1):
        months = AVAILABLE_MONTHS.get(year, DEFAULT_QUARTERLY_MONTHS)
        for month in months:
            period = date(year, month, 1)
            if date(start.year, start.month, 1) <= period <= end:
                periods.append((year, month))
    return periods


def build_url(year: int, month: int, symbol: str, day: int = 1) -> tuple[str, str]:
    """Build the download URL and filename for a given period.

    Returns:
        A (url, filename) tuple.
    """
    month_name = calendar.month_name[month]
    period_date = date(year, month, 1)

    if period_date >= CSV_CUTOFF:
        ymd = f"{year}{month:02d}{day:02d}"
        url = CSV_URL_TEMPLATE.format(year=year, month_name=month_name, symbol=symbol.lower(), ymd=ymd)
        filename = f"slpublic_{symbol}_{ymd}.csv"
    else:
        ym = f"{year}{month:02d}"
        url = PDF_URL_TEMPLATE.format(year=year, month_name=month_name, symbol=symbol.lower(), ym=ym)
        filename = f"sl_{symbol}_{ym}.pdf"

    return url, filename


async def download_file(client: httpx.AsyncClient, url: str, dest: Path) -> bool:
    """Download a single file. Returns True on HTTP 200."""
    try:
        response = await client.get(url)
        if response.status_code == 200:
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(response.content)
            return True
    except httpx.HTTPError:
        pass
    return False


def _next_quarterly_month(month: int) -> int:
    """Return the next quarterly month after the given month."""
    for q in DEFAULT_QUARTERLY_MONTHS:
        if q > month:
            return q
    return DEFAULT_QUARTERLY_MONTHS[0]


async def _download_csv_period(
    client: httpx.AsyncClient,
    year: int,
    month: int,
    symbol: str,
    output_dir: Path,
    semaphore: asyncio.Semaphore,
) -> Path | None:
    """Try to download a CSV file, searching across days and subsequent months."""
    next_q = _next_quarterly_month(month)
    search_months = list(range(month, next_q)) if next_q > month else [month]

    for search_month in search_months:
        days_in_month = calendar.monthrange(year, search_month)[1]
        for day in range(1, days_in_month + 1):
            url, filename = build_url(year, search_month, symbol, day)
            dest = output_dir / filename
            async with semaphore:
                if await download_file(client, url, dest):
                    return dest

    return None


async def _download_pdf_period(
    client: httpx.AsyncClient,
    year: int,
    month: int,
    symbol: str,
    output_dir: Path,
    semaphore: asyncio.Semaphore,
) -> Path | None:
    """Download a PDF file for a single period."""
    url, filename = build_url(year, month, symbol)
    dest = output_dir / filename
    async with semaphore:
        if await download_file(client, url, dest):
            return dest
    return None


async def download_selection_lists(
    start: date = START_DATE,
    end: date | None = None,
    output_dir: Path | str = "cache/stoxx",
    symbol: str = DEFAULT_SYMBOL,
    periods: list[tuple[int, int]] | None = None,
) -> DownloadResult:
    """Download STOXX selection list files for the given date range.

    Args:
        start: Start date (inclusive). Defaults to 2015-10-01.
        end: End date (inclusive). Defaults to today.
        output_dir: Directory for downloaded files.
        symbol: Index symbol (default: sxxp for STOXX Europe 600).
        periods: Explicit list of (year, month) tuples to download.
            When provided, start/end are ignored.

    Returns:
        DownloadResult with lists of downloaded file paths and missed periods.
    """
    if end is None:
        end = date.today()
    output_dir = Path(output_dir)
    if periods is None:
        periods = get_periods(start, end)
    result = DownloadResult()
    semaphore = asyncio.Semaphore(10)

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        tasks = []
        for year, month in periods:
            period_date = date(year, month, 1)
            if period_date >= CSV_CUTOFF:
                tasks.append(_download_csv_period(client, year, month, symbol, output_dir, semaphore))
            else:
                tasks.append(_download_pdf_period(client, year, month, symbol, output_dir, semaphore))

        results = await asyncio.gather(*tasks)

    for (year, month), filepath in zip(periods, results, strict=True):
        if filepath:
            result.downloaded.append(filepath)
        else:
            result.missed.append((year, month))

    return result
