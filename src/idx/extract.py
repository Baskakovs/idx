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


def _clean_optional(val: object) -> str | None:
    """Return a stripped string if non-empty, else None."""
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _clean_isin(raw: str) -> str | None:
    """Return a cleaned ISIN or None for null-like values."""
    s = raw.strip()
    return s if s and s.lower() not in ("", "null", "none") else None


def _asset_from_csv_row(row: dict[str, object]) -> Asset:
    """Build an Asset from a normalised CSV row."""
    return Asset(
        internal_key=str(row["internal_key"]).strip(),
        ric=str(row["ric"]).strip(),
        name=str(row["instrument_name"]).strip(),
        country=str(row["country"]).strip(),
        currency=str(row["currency"]).strip(),
        isin=_clean_isin(str(row.get("isin", ""))),
        sedol=_clean_optional(row.get("sedol")),
    )


def _entry_from_csv_row(row: dict[str, object], review_date: date) -> SelectionListEntry:
    """Build a SelectionListEntry from a normalised CSV row."""
    rank_val = row.get("rank_final")
    rank = int(rank_val) if rank_val is not None and str(rank_val).strip() != "" else None
    ff_mcap_val = row.get("ff_mcap_meur")
    ff_mcap = float(ff_mcap_val) if ff_mcap_val is not None and str(ff_mcap_val).strip() != "" else None
    return SelectionListEntry(
        internal_key=str(row["internal_key"]).strip(),
        review_date=review_date,
        ff_mcap=ff_mcap,
        rank=rank,
        comment=_clean_optional(row.get("comment")),
    )


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

    creation_date_str = str(df["creation_date"][0])
    review_date = date(int(creation_date_str[:4]), int(creation_date_str[4:6]), int(creation_date_str[6:8]))

    asset_df = df.unique(subset=["internal_key"], keep="first")
    assets = [_asset_from_csv_row(row) for row in asset_df.to_dicts()]
    entries = [_entry_from_csv_row(row, review_date) for row in df.to_dicts()]
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


def _extract_pdf_rows(pdf: pdfplumber.PDF) -> list[dict[str, str]]:
    """Extract all data rows from a multi-page PDF table."""
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
        for row_cells in data:
            if len(row_cells) == len(headers):
                all_rows.append(dict(zip(headers, row_cells, strict=True)))  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
    return all_rows


def _asset_from_pdf_row(row: dict[str, str]) -> Asset:
    """Build an Asset from a normalised PDF row."""
    return Asset(
        internal_key=str(row.get("int_key", "")).strip(),
        ric=str(row.get("ric", "")).strip(),
        name=str(row.get("company_name", "")).strip(),
        country=str(row.get("country", "")).strip(),
        currency=str(row.get("currency", "")).strip(),
        isin=_clean_optional(row.get("isin")),
        sedol=_clean_optional(row.get("sedol")),
    )


def _entry_from_pdf_row(row: dict[str, str], internal_key: str, review_date: date) -> SelectionListEntry:
    """Build a SelectionListEntry from a normalised PDF row."""
    rank_val = row.get("rank_final")
    rank = int(rank_val) if rank_val and str(rank_val).strip() else None
    mcap_val = row.get("ff_mcap_beur")
    ff_mcap = float(mcap_val) * 1000 if mcap_val and str(mcap_val).strip() else None
    return SelectionListEntry(internal_key=internal_key, review_date=review_date, ff_mcap=ff_mcap, rank=rank)


def parse_selection_list_pdf(filepath: Path) -> tuple[list[Asset], list[SelectionListEntry]]:
    """Parse a STOXX selection list PDF into Assets and SelectionListEntries.

    Args:
        filepath: Path to a STOXX selection list PDF.

    Returns:
        A tuple of (assets, entries) where assets has one per unique ISIN.
    """
    pdf = pdfplumber.open(filepath)
    header_text = pdf.pages[0].extract_text().split("\n")
    review_date = _parse_pdf_date(header_text)
    all_rows = _extract_pdf_rows(pdf)

    seen_keys: set[str] = set()
    assets = []
    entries = []
    for row in all_rows:
        internal_key = str(row.get("int_key", "")).strip()
        if not internal_key:
            continue
        if internal_key not in seen_keys:
            seen_keys.add(internal_key)
            assets.append(_asset_from_pdf_row(row))
        entries.append(_entry_from_pdf_row(row, internal_key, review_date))

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


def compute_membership_intervals(
    membership_dfs: list[pl.DataFrame],
    sorted_dates: list[date],
) -> pl.DataFrame:
    """Compute contiguous membership intervals for each asset.

    For each asset, groups consecutive review dates where is_member is True
    into contiguous spans, returning (first_included, last_included) pairs.

    Args:
        membership_dfs: One DataFrame per review date with columns
            [internal_key, is_member, entry_reason].
        sorted_dates: Review dates in chronological order, aligned with membership_dfs.

    Returns:
        DataFrame with columns [internal_key, first_included, last_included],
        one row per (asset, contiguous membership interval).
    """
    # Build a mapping from each asset to the set of date indices where it was a member
    asset_member_indices: dict[str, list[int]] = {}
    for i, mdf in enumerate(membership_dfs):
        member_keys = mdf.filter(pl.col("is_member"))["internal_key"].to_list()
        for key in member_keys:
            asset_member_indices.setdefault(key, []).append(i)

    rows: list[dict[str, object]] = []
    for key, indices in asset_member_indices.items():
        indices.sort()
        # Group consecutive indices into spans
        span_start = indices[0]
        prev = indices[0]
        for idx in indices[1:]:
            if idx != prev + 1:
                rows.append(
                    {
                        "internal_key": key,
                        "first_included": sorted_dates[span_start],
                        "last_included": sorted_dates[prev],
                    }
                )
                span_start = idx
            prev = idx
        rows.append(
            {
                "internal_key": key,
                "first_included": sorted_dates[span_start],
                "last_included": sorted_dates[prev],
            }
        )

    return pl.DataFrame(
        rows,
        schema={
            "internal_key": pl.Utf8,
            "first_included": pl.Date,
            "last_included": pl.Date,
        },
    )


def _select_buffer_retained(
    ranked: list[SelectionListEntry], prior_membership: set[str], target: int
) -> list[IndexMembership]:
    """Retain prior members from the buffer zone (positions 551-750)."""
    members: list[IndexMembership] = []
    for entry in ranked[550:750]:
        if len(members) >= target:
            break
        if entry.internal_key in prior_membership:
            members.append(
                IndexMembership(
                    internal_key=entry.internal_key, is_member=True, entry_reason=EntryReason.BUFFER_RETAINED
                )
            )
    return members


def _fill_remaining(ranked: list[SelectionListEntry], existing_keys: set[str], target: int) -> list[IndexMembership]:
    """Fill remaining slots to *target* from the highest-ranked candidates not yet selected."""
    members: list[IndexMembership] = []
    for entry in ranked:
        if len(members) >= target:
            break
        if entry.internal_key not in existing_keys:
            members.append(
                IndexMembership(internal_key=entry.internal_key, is_member=True, entry_reason=EntryReason.FILL_TO_600)
            )
    return members


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
    ranked = [e for e in entries if e.rank is not None]
    ranked.sort(key=lambda e: (e.rank, e.internal_key))

    logger = get_logger()
    if prior_membership is None:
        logger.warning("Bootstrap mode: no prior membership provided, taking top 600 by FF Mcap")
        return [
            IndexMembership(internal_key=e.internal_key, is_member=True, entry_reason=EntryReason.BOOTSTRAP)
            for e in ranked[:600]
        ]

    # Phase 1: top 550 are automatic members
    members = [
        IndexMembership(internal_key=e.internal_key, is_member=True, entry_reason=EntryReason.TOP_550)
        for e in ranked[:550]
    ]

    # Phase 2: retain prior members from buffer zone
    slots_needed = 600 - len(members)
    members.extend(_select_buffer_retained(ranked, prior_membership, slots_needed))

    # Phase 3: fill remaining slots
    member_keys = {m.internal_key for m in members}
    slots_needed = 600 - len(members)
    members.extend(_fill_remaining(ranked, member_keys, slots_needed))

    return members
