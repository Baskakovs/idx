"""Tests for idx.extract module."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from idx.extract import (
    EntryReason,
    SelectionListEntry,
    _normalize_column_name,
    _parse_pdf_date,
    compute_membership,
    compute_membership_intervals,
    parse_selection_list,
    parse_selection_list_csv,
    parse_selection_list_pdf,
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


class TestComputeMembershipIntervals:
    """Tests for contiguous membership interval computation."""

    def test_single_continuous_span(self):
        """Asset that is a member for all dates gets one interval."""
        dates = [date(2024, 3, 1), date(2024, 6, 1), date(2024, 9, 1)]
        membership_dfs = [
            pl.DataFrame({"internal_key": ["A"], "is_member": [True], "entry_reason": ["top_550"]}),
            pl.DataFrame({"internal_key": ["A"], "is_member": [True], "entry_reason": ["top_550"]}),
            pl.DataFrame({"internal_key": ["A"], "is_member": [True], "entry_reason": ["top_550"]}),
        ]
        result = compute_membership_intervals(membership_dfs, dates)
        assert len(result) == 1
        assert result["first_included"][0] == date(2024, 3, 1)
        assert result["last_included"][0] == date(2024, 9, 1)

    def test_gap_produces_two_intervals(self):
        """Asset that leaves and rejoins produces two intervals."""
        dates = [date(2024, 3, 1), date(2024, 6, 1), date(2024, 9, 1), date(2024, 12, 1)]
        membership_dfs = [
            pl.DataFrame({"internal_key": ["A"], "is_member": [True], "entry_reason": ["top_550"]}),
            pl.DataFrame({"internal_key": ["A"], "is_member": [False], "entry_reason": ["top_550"]}),
            pl.DataFrame({"internal_key": ["A"], "is_member": [True], "entry_reason": ["top_550"]}),
            pl.DataFrame({"internal_key": ["A"], "is_member": [True], "entry_reason": ["top_550"]}),
        ]
        result = compute_membership_intervals(membership_dfs, dates)
        result = result.sort("first_included")
        assert len(result) == 2
        assert result["first_included"][0] == date(2024, 3, 1)
        assert result["last_included"][0] == date(2024, 3, 1)
        assert result["first_included"][1] == date(2024, 9, 1)
        assert result["last_included"][1] == date(2024, 12, 1)

    def test_non_member_excluded(self):
        """Asset that is never a member produces no rows."""
        dates = [date(2024, 3, 1), date(2024, 6, 1)]
        membership_dfs = [
            pl.DataFrame({"internal_key": ["A"], "is_member": [False], "entry_reason": ["top_550"]}),
            pl.DataFrame({"internal_key": ["A"], "is_member": [False], "entry_reason": ["top_550"]}),
        ]
        result = compute_membership_intervals(membership_dfs, dates)
        assert len(result) == 0

    def test_multiple_assets(self):
        """Multiple assets each get their own intervals."""
        dates = [date(2024, 3, 1), date(2024, 6, 1)]
        membership_dfs = [
            pl.DataFrame(
                {"internal_key": ["A", "B"], "is_member": [True, True], "entry_reason": ["top_550", "top_550"]}
            ),
            pl.DataFrame(
                {"internal_key": ["A", "B"], "is_member": [True, False], "entry_reason": ["top_550", "top_550"]}
            ),
        ]
        result = compute_membership_intervals(membership_dfs, dates)
        assert len(result) == 2
        a_rows = result.filter(pl.col("internal_key") == "A")
        b_rows = result.filter(pl.col("internal_key") == "B")
        assert a_rows["first_included"][0] == date(2024, 3, 1)
        assert a_rows["last_included"][0] == date(2024, 6, 1)
        assert b_rows["first_included"][0] == date(2024, 3, 1)
        assert b_rows["last_included"][0] == date(2024, 3, 1)

    def test_column_types_are_date(self):
        """Output columns first_included and last_included are pl.Date."""
        dates = [date(2024, 3, 1)]
        membership_dfs = [
            pl.DataFrame({"internal_key": ["A"], "is_member": [True], "entry_reason": ["top_550"]}),
        ]
        result = compute_membership_intervals(membership_dfs, dates)
        assert result.schema["first_included"] == pl.Date
        assert result.schema["last_included"] == pl.Date


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


def _mock_pdf(header_text: str, headers: list[str], rows: list[list[str]]) -> MagicMock:
    """Build a mock pdfplumber PDF with one page."""
    page = MagicMock()
    page.extract_text.return_value = header_text
    page.extract_table.return_value = [headers, *rows]
    pdf = MagicMock()
    pdf.pages = [page]
    return pdf


class TestParseSelectionListPdf:
    """Tests for PDF parsing."""

    @patch("idx.extract.pdfplumber.open")
    def test_basic_pdf_parsing(self, mock_open):
        """Parses a single-page PDF into assets and entries."""
        headers = [
            "Int_Key",
            "RIC",
            "Company Name",
            "Country",
            "Currency",
            "ISIN",
            "SEDOL",
            "Rank Final",
            "FF MCap (BEUR)",
        ]
        rows = [
            ["K1", "R1.DE", "Company 1", "DE", "EUR", "DE000A1EWWW0", "B123456", "1", "5.5"],
            ["K2", "R2.FR", "Company 2", "FR", "EUR", "FR0000120271", "", "2", "3.2"],
        ]
        mock_open.return_value = _mock_pdf("Last updated 20240901", headers, rows)

        assets, entries = parse_selection_list_pdf(Path("fake.pdf"))

        assert len(assets) == 2
        assert len(entries) == 2
        assert assets[0].internal_key == "K1"
        assert assets[0].isin == "DE000A1EWWW0"
        assert assets[0].sedol == "B123456"
        assert assets[1].sedol is None
        assert entries[0].review_date == date(2024, 9, 1)
        assert entries[0].rank == 1
        assert entries[0].ff_mcap == 5500.0  # BEUR -> MEUR

    @patch("idx.extract.pdfplumber.open")
    def test_duplicate_internal_key(self, mock_open):
        """Duplicate internal_keys produce one asset but multiple entries."""
        headers = [
            "Int_Key",
            "RIC",
            "Company Name",
            "Country",
            "Currency",
            "ISIN",
            "SEDOL",
            "Rank Final",
            "FF MCap (BEUR)",
        ]
        rows = [
            ["K1", "R1.DE", "Company 1", "DE", "EUR", "DE000A1EWWW0", "", "1", "5.5"],
            ["K1", "R1.DE", "Company 1", "DE", "EUR", "DE000A1EWWW0", "", "1", "5.5"],
        ]
        mock_open.return_value = _mock_pdf("Last updated 20240901", headers, rows)

        assets, entries = parse_selection_list_pdf(Path("fake.pdf"))

        assert len(assets) == 1
        assert len(entries) == 2

    @patch("idx.extract.pdfplumber.open")
    def test_missing_rank_and_mcap(self, mock_open):
        """Empty rank and mcap produce None values."""
        headers = [
            "Int_Key",
            "RIC",
            "Company Name",
            "Country",
            "Currency",
            "ISIN",
            "SEDOL",
            "Rank Final",
            "FF MCap (BEUR)",
        ]
        rows = [
            ["K1", "R1.DE", "Company 1", "DE", "EUR", "", "", "", ""],
        ]
        mock_open.return_value = _mock_pdf("Last updated 20240901", headers, rows)

        assets, entries = parse_selection_list_pdf(Path("fake.pdf"))

        assert entries[0].rank is None
        assert entries[0].ff_mcap is None
        assert assets[0].isin is None
        assert assets[0].sedol is None

    @patch("idx.extract.pdfplumber.open")
    def test_skips_rows_without_internal_key(self, mock_open):
        """Rows with empty internal_key are skipped."""
        headers = [
            "Int_Key",
            "RIC",
            "Company Name",
            "Country",
            "Currency",
            "ISIN",
            "SEDOL",
            "Rank Final",
            "FF MCap (BEUR)",
        ]
        rows = [
            ["", "R1.DE", "Company 1", "DE", "EUR", "", "", "1", "5.5"],
            ["K2", "R2.FR", "Company 2", "FR", "EUR", "", "", "2", "3.2"],
        ]
        mock_open.return_value = _mock_pdf("Last updated 20240901", headers, rows)

        assets, entries = parse_selection_list_pdf(Path("fake.pdf"))

        assert len(assets) == 1
        assert len(entries) == 1
        assert assets[0].internal_key == "K2"

    @patch("idx.extract.pdfplumber.open")
    def test_multi_page_pdf(self, mock_open):
        """Multi-page PDFs: headers from page 0, data rows from all pages."""
        headers = [
            "Int_Key",
            "RIC",
            "Company Name",
            "Country",
            "Currency",
            "ISIN",
            "SEDOL",
            "Rank Final",
            "FF MCap (BEUR)",
        ]

        page0 = MagicMock()
        page0.extract_text.return_value = "Last updated 20240901"
        page0.extract_table.return_value = [
            headers,
            ["K1", "R1.DE", "Company 1", "DE", "EUR", "IS1", "", "1", "5.5"],
        ]

        page1 = MagicMock()
        page1.extract_table.return_value = [
            ["K2", "R2.FR", "Company 2", "FR", "EUR", "IS2", "", "2", "3.2"],
        ]

        pdf = MagicMock()
        pdf.pages = [page0, page1]
        mock_open.return_value = pdf

        assets, entries = parse_selection_list_pdf(Path("fake.pdf"))

        assert len(assets) == 2
        assert len(entries) == 2

    @patch("idx.extract.pdfplumber.open")
    def test_page_with_no_table(self, mock_open):
        """Pages that return no table are skipped."""
        headers = [
            "Int_Key",
            "RIC",
            "Company Name",
            "Country",
            "Currency",
            "ISIN",
            "SEDOL",
            "Rank Final",
            "FF MCap (BEUR)",
        ]

        page0 = MagicMock()
        page0.extract_text.return_value = "Last updated 20240901"
        page0.extract_table.return_value = [
            headers,
            ["K1", "R1.DE", "Company 1", "DE", "EUR", "IS1", "", "1", "5.5"],
        ]

        page1 = MagicMock()
        page1.extract_table.return_value = None

        pdf = MagicMock()
        pdf.pages = [page0, page1]
        mock_open.return_value = pdf

        assets, _entries = parse_selection_list_pdf(Path("fake.pdf"))

        assert len(assets) == 1

    @patch("idx.extract.pdfplumber.open")
    def test_mismatched_row_length_skipped(self, mock_open):
        """Rows with wrong column count are silently skipped."""
        headers = [
            "Int_Key",
            "RIC",
            "Company Name",
            "Country",
            "Currency",
            "ISIN",
            "SEDOL",
            "Rank Final",
            "FF MCap (BEUR)",
        ]
        rows = [
            ["K1", "R1.DE", "Company 1"],  # too few columns
            ["K2", "R2.FR", "Company 2", "FR", "EUR", "IS2", "", "2", "3.2"],
        ]
        mock_open.return_value = _mock_pdf("Last updated 20240901", headers, rows)

        assets, _entries = parse_selection_list_pdf(Path("fake.pdf"))

        assert len(assets) == 1
        assert assets[0].internal_key == "K2"
