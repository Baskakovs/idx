"""Extract STOXX selection list data and compute index membership."""

from __future__ import annotations

import enum
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pdfplumber
import polars as pl
from prefect import task

from idx import get_logger


@dataclass(frozen=True)
class Asset:
    """Time-invariant security identifiers."""

    internal_key: str
    ric: str
    name: str
    country: str
    currency: str
    isin: str | None = None
    sedol: str | None = None


@dataclass(frozen=True)
class SelectionListEntry:
    """One row per asset per review date."""

    internal_key: str
    review_date: date
    ff_mcap: float | None
    rank: int | None
    comment: str | None = None


class EntryReason(enum.Enum):
    """Why a stock was selected for membership."""

    TOP_550 = "top_550"
    BUFFER_RETAINED = "buffer_retained"
    FILL_TO_600 = "fill_to_600"
    BOOTSTRAP = "bootstrap"


@dataclass(frozen=True)
class IndexMembership:
    """Membership result for a single internal_key."""

    internal_key: str
    is_member: bool
    entry_reason: EntryReason


def _normalize_column_name(name: str) -> str:
    """Normalize column name to lowercase with underscores."""
    normalized = name.lower().strip()
    normalized = re.sub(r"[()\[\]]+", "", normalized)
    normalized = re.sub(r"[\s\-\.]+", "_", normalized)
    return re.sub(r"_+", "_", normalized).strip("_")


def parse_selection_list_csv(
    filepath: Path,
) -> tuple[list[Asset], list[SelectionListEntry]]:
    """Parse a STOXX selection list CSV into Assets and SelectionListEntries.

    Args:
        filepath: Path to a semicolon-delimited STOXX selection list CSV.

    Returns:
        A tuple of (assets, entries) where assets has one per unique internal_key.
    """
    df = pl.read_csv(filepath, separator=";", infer_schema_length=10000)
    df = df.rename({col: _normalize_column_name(col) for col in df.columns})

    # Extract review_date from creation_date column (YYYYMMDD -> date)
    creation_date_str = str(df["creation_date"][0])
    review_date = date(int(creation_date_str[:4]), int(creation_date_str[4:6]), int(creation_date_str[6:8]))

    # Build assets: one per unique internal_key
    asset_df = df.unique(subset=["internal_key"], keep="first")
    assets = []
    for row in asset_df.to_dicts():
        sedol_val = row.get("sedol")
        sedol = str(sedol_val).strip() if sedol_val is not None and str(sedol_val).strip() else None
        internal_key = str(row["internal_key"]).strip()
        raw_isin = str(row.get("isin", "")).strip()
        isin = raw_isin if raw_isin and raw_isin.lower() not in ("", "null", "none") else None
        assets.append(
            Asset(
                internal_key=internal_key,
                ric=str(row["ric"]).strip(),
                name=str(row["instrument_name"]).strip(),
                country=str(row["country"]).strip(),
                currency=str(row["currency"]).strip(),
                isin=isin,
                sedol=sedol,
            )
        )

    # Build entries: one per row
    entries = []
    for row in df.to_dicts():
        rank_val = row.get("rank_final")
        rank = int(rank_val) if rank_val is not None and str(rank_val).strip() != "" else None

        comment_val = row.get("comment")
        comment = str(comment_val).strip() if comment_val is not None and str(comment_val).strip() else None

        ff_mcap_val = row.get("ff_mcap_meur")
        ff_mcap = float(ff_mcap_val) if ff_mcap_val is not None and str(ff_mcap_val).strip() != "" else None

        internal_key = str(row["internal_key"]).strip()
        entries.append(
            SelectionListEntry(
                internal_key=internal_key,
                review_date=review_date,
                ff_mcap=ff_mcap,
                rank=rank,
                comment=comment,
            )
        )

    return assets, entries


def _parse_pdf_date(text_lines: list[str]) -> date:
    """Extract the review date from PDF header lines."""
    for line in text_lines:
        if "last updated" not in line.lower():
            continue
        match = re.search(r"(\d{8})", line)
        if match:
            s = match.group(1)
            return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        match = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", line)
        if match:
            return date(int(match.group(3)), int(match.group(2)), int(match.group(1)))
    msg = f"Could not find review date in PDF header: {text_lines[:5]}"
    raise ValueError(msg)


def parse_selection_list_pdf(filepath: Path) -> tuple[list[Asset], list[SelectionListEntry]]:
    """Parse a STOXX selection list PDF into Assets and SelectionListEntries.

    Args:
        filepath: Path to a STOXX selection list PDF.

    Returns:
        A tuple of (assets, entries) where assets has one per unique ISIN.
    """
    pdf = pdfplumber.open(filepath)

    # Extract review date from header text
    header_text = pdf.pages[0].extract_text().split("\n")
    review_date = _parse_pdf_date(header_text)

    # Extract all rows across pages
    all_rows: list[dict[str, str]] = []
    headers: list[str] = []
    for i, page in enumerate(pdf.pages):
        table = page.extract_table()
        if not table:
            continue
        if i == 0:
            headers = [_normalize_column_name(h or "") for h in table[0]]
            data = table[1:]
        else:
            data = table
        for row in data:
            if len(row) == len(headers):
                all_rows.append(dict(zip(headers, row, strict=True)))

    # Build assets: one per unique internal_key
    seen_keys: set[str] = set()
    assets = []
    entries = []
    for row in all_rows:
        internal_key = str(row.get("int_key", "")).strip()
        if not internal_key:
            continue

        isin_val = str(row.get("isin", "")).strip()
        isin = isin_val if isin_val else None

        if internal_key not in seen_keys:
            seen_keys.add(internal_key)
            sedol_val = row.get("sedol")
            sedol = str(sedol_val).strip() if sedol_val and str(sedol_val).strip() else None
            assets.append(
                Asset(
                    internal_key=internal_key,
                    ric=str(row.get("ric", "")).strip(),
                    name=str(row.get("company_name", "")).strip(),
                    country=str(row.get("country", "")).strip(),
                    currency=str(row.get("currency", "")).strip(),
                    isin=isin,
                    sedol=sedol,
                )
            )

        rank_val = row.get("rank_final")
        rank = int(rank_val) if rank_val and str(rank_val).strip() else None

        # PDF uses BEUR, convert to MEUR for consistency
        mcap_val = row.get("ff_mcap_beur")
        ff_mcap = float(mcap_val) * 1000 if mcap_val and str(mcap_val).strip() else None

        entries.append(
            SelectionListEntry(
                internal_key=internal_key,
                review_date=review_date,
                ff_mcap=ff_mcap,
                rank=rank,
            )
        )

    return assets, entries


@task
def parse_selection_list(
    filepath: Path,
) -> tuple[list[Asset], list[SelectionListEntry]]:
    """Parse a STOXX selection list file (CSV or PDF).

    Args:
        filepath: Path to a STOXX selection list file.

    Returns:
        A tuple of (assets, entries) where assets has one per unique internal_key.
    """
    logger = get_logger()
    file_type = filepath.suffix.lower().lstrip(".")
    if file_type == "pdf":
        assets, entries = parse_selection_list_pdf(filepath)
    else:
        assets, entries = parse_selection_list_csv(filepath)
    review_date = entries[0].review_date if entries else "unknown"
    logger.info(
        "Parsed %s file for %s: %d assets, %d entries", file_type.upper(), review_date, len(assets), len(entries)
    )
    return assets, entries


@task
def compute_membership(
    entries: list[SelectionListEntry],
    prior_membership: set[str] | None,
) -> list[IndexMembership]:
    """Compute STOXX Europe 600 index membership using the buffer rule.

    Args:
        entries: Selection list entries (may include unranked entries).
        prior_membership: Set of internal_keys that were members in the prior review,
            or None for bootstrap mode (first review).

    Returns:
        List of exactly 600 IndexMembership results.
    """
    # Filter to ranked entries, sort by rank ASC then internal_key ASC for deterministic tiebreaker
    ranked = [e for e in entries if e.rank is not None]
    ranked.sort(key=lambda e: (e.rank, e.internal_key))

    logger = get_logger()
    if prior_membership is None:
        logger.warning("Bootstrap mode: no prior membership provided, taking top 600 by FF Mcap")
        return [
            IndexMembership(internal_key=e.internal_key, is_member=True, entry_reason=EntryReason.BOOTSTRAP)
            for e in ranked[:600]
        ]

    members: list[IndexMembership] = []

    # Positions 1-550: automatic members
    for entry in ranked[:550]:
        members.append(
            IndexMembership(internal_key=entry.internal_key, is_member=True, entry_reason=EntryReason.TOP_550)
        )

    # Positions 551-750: retain prior members (buffer zone)
    buffer_zone = ranked[550:750]
    for entry in buffer_zone:
        if len(members) >= 600:
            break
        if entry.internal_key in prior_membership:
            members.append(
                IndexMembership(
                    internal_key=entry.internal_key, is_member=True, entry_reason=EntryReason.BUFFER_RETAINED
                )
            )

    # Fill remaining slots to 600 from largest remaining by FF Mcap
    member_keys = {m.internal_key for m in members}
    remaining = [e for e in ranked if e.internal_key not in member_keys]
    for entry in remaining:
        if len(members) >= 600:
            break
        members.append(
            IndexMembership(internal_key=entry.internal_key, is_member=True, entry_reason=EntryReason.FILL_TO_600)
        )

    return members
