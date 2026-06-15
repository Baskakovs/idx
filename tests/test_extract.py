"""Tests for idx.extract module."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from idx.extract import (
    EntryReason,
    SelectionListEntry,
    _normalize_column_name,
    _parse_pdf_date,
    compute_membership,
    parse_selection_list,
    parse_selection_list_csv,
)


class TestNormalizeColumnName:
    """Tests for column name normalization."""

    def test_lowercase(self):
        """Converts to lowercase."""
        assert _normalize_column_name("RankFinal") == "rankfinal"

    def test_spaces_to_underscore(self):
        """Replaces spaces with underscores."""
        assert _normalize_column_name("Rank Final") == "rank_final"

    def test_parentheses_removed(self):
        """Strips parentheses."""
        assert _normalize_column_name("FF MCap (MEUR)") == "ff_mcap_meur"

    def test_dots_and_hyphens(self):
        """Replaces dots and hyphens with underscores."""
        assert _normalize_column_name("some-column.name") == "some_column_name"

    def test_multiple_underscores_collapsed(self):
        """Collapses multiple underscores."""
        assert _normalize_column_name("a   b") == "a_b"

    def test_leading_trailing_stripped(self):
        """Strips leading and trailing whitespace."""
        assert _normalize_column_name("  name  ") == "name"


def _make_entries(n: int, review_date: date = date(2024, 9, 1)) -> list[SelectionListEntry]:
    """Create n ranked entries with sequential keys."""
    return [
        SelectionListEntry(
            internal_key=f"K{i:04d}",
            review_date=review_date,
            ff_mcap=float(n - i),
            rank=i + 1,
        )
        for i in range(n)
    ]


class TestComputeMembership:
    """Tests for STOXX 600 membership computation."""

    def test_bootstrap_takes_top_600(self):
        """First review (no prior membership) takes top 600."""
        entries = _make_entries(800)
        result = compute_membership.fn(entries, prior_membership=None)
        assert len(result) == 600
        assert all(m.is_member for m in result)
        assert all(m.entry_reason == EntryReason.BOOTSTRAP for m in result)

    def test_top_550_always_members(self):
        """Ranks 1-550 are always members."""
        entries = _make_entries(800)
        prior = {f"K{i:04d}" for i in range(600)}
        result = compute_membership.fn(entries, prior_membership=prior)
        assert len(result) == 600
        top_550 = [m for m in result if m.entry_reason == EntryReason.TOP_550]
        assert len(top_550) == 550

    def test_buffer_retains_prior_members(self):
        """Prior members in buffer zone (551-750) are retained."""
        entries = _make_entries(800)
        # Make all 800 prior members
        prior = {f"K{i:04d}" for i in range(800)}
        result = compute_membership.fn(entries, prior_membership=prior)
        assert len(result) == 600
        buffer = [m for m in result if m.entry_reason == EntryReason.BUFFER_RETAINED]
        # 50 slots remain after top 550, all buffer entries are prior members
        assert len(buffer) == 50

    def test_fill_when_buffer_insufficient(self):
        """When buffer zone doesn't fill to 600, remaining slots are filled."""
        entries = _make_entries(800)
        # Only top 550 are prior members, none in buffer zone
        prior = {f"K{i:04d}" for i in range(550)}
        result = compute_membership.fn(entries, prior_membership=prior)
        assert len(result) == 600
        fill = [m for m in result if m.entry_reason == EntryReason.FILL_TO_600]
        assert len(fill) == 50

    def test_unranked_entries_ignored(self):
        """Entries with rank=None are excluded from membership."""
        entries = _make_entries(600)
        entries.append(
            SelectionListEntry(internal_key="NORANK", review_date=date(2024, 9, 1), ff_mcap=999.0, rank=None)
        )
        result = compute_membership.fn(entries, prior_membership=None)
        keys = {m.internal_key for m in result}
        assert "NORANK" not in keys

    def test_result_count_with_fewer_than_600_ranked(self):
        """If fewer than 600 ranked entries, all are members."""
        entries = _make_entries(100)
        result = compute_membership.fn(entries, prior_membership=None)
        assert len(result) == 100


class TestParsePdfDate:
    """Tests for PDF date extraction."""

    def test_yyyymmdd_format(self):
        """Extracts date from YYYYMMDD in 'last updated' line."""
        lines = ["Selection List", "Last updated 20240901"]
        assert _parse_pdf_date(lines) == date(2024, 9, 1)

    def test_dd_mm_yyyy_format(self):
        """Extracts date from DD.MM.YYYY in 'last updated' line."""
        lines = ["Last Updated: 01.09.2024"]
        assert _parse_pdf_date(lines) == date(2024, 9, 1)

    def test_missing_date_raises(self):
        """Raises ValueError if no date found in header."""
        with pytest.raises(ValueError, match="Could not find review date"):
            _parse_pdf_date(["No date here", "Still nothing"])


class TestParseSelectionListCsv:
    """Tests for CSV parsing."""

    def test_parses_csv(self, tmp_path: Path):
        """Parses a semicolon-delimited CSV into assets and entries."""
        csv_content = (
            "Internal Key;RIC;Instrument Name;Country;Currency;ISIN;SEDOL;"
            "Rank Final;FF MCap (MEUR);Comment;Creation Date\n"
            "K1;R1;Name1;DE;EUR;IS1;SE1;1;100.5;good;20240901\n"
            "K2;R2;Name2;FR;EUR;;SE2;2;200.0;;20240901\n"
        )
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        assets, entries = parse_selection_list_csv(csv_file)
        assert len(assets) == 2
        assert len(entries) == 2
        assert assets[0].internal_key in ("K1", "K2")
        assert entries[0].review_date == date(2024, 9, 1)
        assert entries[0].rank == 1

    def test_handles_missing_optional_fields(self, tmp_path: Path):
        """Handles missing ISIN, SEDOL, comment, and rank gracefully."""
        csv_content = (
            "Internal Key;RIC;Instrument Name;Country;Currency;ISIN;SEDOL;"
            "Rank Final;FF MCap (MEUR);Comment;Creation Date\n"
            "K1;R1;Name1;DE;EUR;;;;;;20240901\n"
        )
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        assets, entries = parse_selection_list_csv(csv_file)
        assert len(assets) == 1
        assert assets[0].isin is None
        assert assets[0].sedol is None
        assert entries[0].rank is None
        assert entries[0].ff_mcap is None


class TestParseSelectionList:
    """Tests for the dispatch function."""

    def test_csv_dispatch(self, tmp_path: Path):
        """CSV files are dispatched to CSV parser."""
        csv_content = (
            "Internal Key;RIC;Instrument Name;Country;Currency;ISIN;SEDOL;"
            "Rank Final;FF MCap (MEUR);Comment;Creation Date\n"
            "K1;R1;Name1;DE;EUR;IS1;;1;100.0;;20240901\n"
        )
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        assets, entries = parse_selection_list.fn(csv_file)
        assert len(assets) == 1
        assert len(entries) == 1

    @patch("idx.extract.parse_selection_list_pdf")
    def test_pdf_dispatch(self, mock_pdf_parse, tmp_path: Path):
        """PDF files are dispatched to PDF parser."""
        mock_pdf_parse.return_value = (
            [],
            [SelectionListEntry(internal_key="K1", review_date=date(2024, 9, 1), ff_mcap=100.0, rank=1)],
        )
        pdf_file = tmp_path / "test.pdf"
        pdf_file.write_bytes(b"fake")

        _assets, _entries = parse_selection_list.fn(pdf_file)
        mock_pdf_parse.assert_called_once()
