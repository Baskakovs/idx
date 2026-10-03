"""Tests for idx.ranking module."""

from __future__ import annotations

from datetime import date

import polars as pl

from idx.ranking import build_assets_from_membership, build_membership_table, validate_membership_table

_EXPECTED_COLS = {"date", "internal_key", "ric", "name", "country", "currency", "isin", "sedol", "rank", "yukka_id"}


def _make_assets(**overrides):
    """Build a single-row assets DataFrame with sensible defaults."""
    defaults = {
        "internal_key": ["K1"],
        "ric": ["R1"],
        "name": ["N1"],
        "country": ["DE"],
        "currency": ["EUR"],
        "isin": ["IS1"],
        "sedol": ["SE1"],
    }
    defaults.update(overrides)
    return pl.DataFrame(defaults)


def _make_entries(rd, **overrides):
    """Build a single-row entries DataFrame with sensible defaults."""
    defaults = {"internal_key": ["K1"], "review_date": [rd], "rank": [1], "ff_mcap": [100.0]}
    defaults.update(overrides)
    return pl.DataFrame(defaults)


def _make_membership(**overrides):
    """Build a single-row membership DataFrame with sensible defaults."""
    defaults = {"internal_key": ["K1"], "is_member": [True], "entry_reason": ["top_550"]}
    defaults.update(overrides)
    return pl.DataFrame(defaults)


class TestBuildMembershipTable:
    """Tests for long-format membership table construction."""

    def test_empty_review_dates(self):
        """Empty review dates produce an empty DataFrame."""
        result = build_membership_table.fn([], [], [], [])
        assert set(result.columns) == _EXPECTED_COLS
        assert len(result) == 0

    def test_single_review_date(self):
        """Single review date produces daily rows from that date to today."""
        rd = date(2024, 12, 1)
        assets = _make_assets(
            internal_key=["K1", "K2"],
            ric=["R1", "R2"],
            name=["N1", "N2"],
            country=["DE", "FR"],
            currency=["EUR", "EUR"],
            isin=["IS1", "IS2"],
            sedol=["SE1", "SE2"],
        )
        entries = _make_entries(rd, internal_key=["K1", "K2"], rank=[1, 2], ff_mcap=[100.0, 50.0], review_date=[rd, rd])
        membership = _make_membership(
            internal_key=["K1", "K2"], is_member=[True, False], entry_reason=["top_550", "top_550"]
        )
        result = build_membership_table.fn([assets], [entries], [membership], [rd])

        # Only K1 is a member
        assert set(result["internal_key"].unique().to_list()) == {"K1"}
        row = result.filter(pl.col("date") == rd)
        assert row["rank"][0] == 1
        assert row["ric"][0] == "R1"
        assert row["name"][0] == "N1"
        assert row["isin"][0] == "IS1"
        expected_days = (date.today() - rd).days + 1
        assert len(result) == expected_days

    def test_forward_fill_between_reviews(self):
        """Ranks, RICs, and identifiers are forward-filled between review dates."""
        rd1 = date(2025, 6, 1)
        rd2 = date(2025, 6, 5)
        assets1 = _make_assets()
        assets2 = _make_assets()
        entries1 = _make_entries(rd1, rank=[5])
        entries2 = _make_entries(rd2, rank=[3])
        membership = _make_membership()

        result = build_membership_table.fn(
            [assets1, assets2], [entries1, entries2], [membership, membership], [rd1, rd2]
        )

        mid = result.filter(pl.col("date") == date(2025, 6, 3))
        assert mid["rank"][0] == 1
        assert mid["ric"][0] == "R1"
        assert mid["name"][0] == "N1"

    def test_point_in_time_ric(self):
        """RIC changes are tracked per review date."""
        rd1 = date(2025, 1, 1)
        rd2 = date(2025, 4, 1)
        assets1 = _make_assets(ric=["NESN.VX"])
        assets2 = _make_assets(ric=["NESN.S"])
        entries1 = _make_entries(rd1)
        entries2 = _make_entries(rd2, rank=[2])
        membership = _make_membership()

        result = build_membership_table.fn(
            [assets1, assets2], [entries1, entries2], [membership, membership], [rd1, rd2]
        )

        before = result.filter(pl.col("date") == date(2025, 3, 31))
        assert before["ric"][0] == "NESN.VX"
        on_rd2 = result.filter(pl.col("date") == rd2)
        assert on_rd2["ric"][0] == "NESN.S"

    def test_null_fields_forward_fill_from_previous(self):
        """Empty identifiers at a review date carry forward from the previous review."""
        rd1 = date(2025, 1, 1)
        rd2 = date(2025, 4, 1)
        assets1 = _make_assets(ric=["NESN.S"], isin=["CH0038863350"], name=["NESTLE"])
        assets2 = _make_assets(ric=[""], isin=[None], name=[""])
        entries1 = _make_entries(rd1)
        entries2 = _make_entries(rd2, rank=[2])
        membership = _make_membership()

        result = build_membership_table.fn(
            [assets1, assets2], [entries1, entries2], [membership, membership], [rd1, rd2]
        )

        on_rd2 = result.filter(pl.col("date") == rd2)
        assert on_rd2["ric"][0] == "NESN.S"
        assert on_rd2["isin"][0] == "CH0038863350"
        assert on_rd2["name"][0] == "NESTLE"

    def test_member_exit(self):
        """A company leaving the index has no rows after the new review date."""
        rd1 = date(2025, 1, 1)
        rd2 = date(2025, 4, 1)
        assets1 = _make_assets()
        assets2 = _make_assets()
        entries1 = _make_entries(rd1)
        entries2 = _make_entries(rd2)
        membership1 = _make_membership(is_member=[True])
        membership2 = _make_membership(is_member=[False])

        result = build_membership_table.fn(
            [assets1, assets2], [entries1, entries2], [membership1, membership2], [rd1, rd2]
        )

        assert result.filter(pl.col("date") == date(2025, 3, 31))["internal_key"][0] == "K1"
        assert result.filter(pl.col("date") == rd2).is_empty()


class TestBuildMembershipTableEdgeCases:
    """Edge cases for membership table construction."""

    def test_entries_without_ric_column(self):
        """Entries missing a 'ric' column get RICs from assets."""
        rd = date(2024, 12, 1)
        assets = _make_assets()
        entries = pl.DataFrame({"internal_key": ["K1"], "review_date": [rd], "rank": [1], "ff_mcap": [100.0]})
        membership = _make_membership()
        result = build_membership_table.fn([assets], [entries], [membership], [rd])
        row = result.filter(pl.col("date") == rd)
        assert row["ric"][0] == "R1"

    def test_no_members_produces_empty(self):
        """No members at any review date produces empty result."""
        rd = date(2024, 12, 1)
        assets = _make_assets()
        entries = _make_entries(rd)
        membership = _make_membership(is_member=[False])
        result = build_membership_table.fn([assets], [entries], [membership], [rd])
        assert len(result) == 0

    def test_two_companies_multiple_reviews(self):
        """Two companies across two review dates with one changing RIC."""
        rd1 = date(2025, 1, 1)
        rd2 = date(2025, 1, 3)
        assets1 = _make_assets(
            internal_key=["K1", "K2"],
            ric=["ING.AS", "ASML.AS"],
            name=["ING", "ASML"],
            country=["NL", "NL"],
            currency=["EUR", "EUR"],
            isin=["NL1", "NL2"],
            sedol=["S1", "S2"],
        )
        assets2 = _make_assets(
            internal_key=["K1", "K2"],
            ric=["INGA.AS", "ASML.AS"],
            name=["ING", "ASML"],
            country=["NL", "NL"],
            currency=["EUR", "EUR"],
            isin=["NL1", "NL2"],
            sedol=["S1", "S2"],
        )
        entries1 = _make_entries(
            rd1, internal_key=["K1", "K2"], rank=[1, 2], ff_mcap=[100.0, 90.0], review_date=[rd1, rd1]
        )
        entries2 = _make_entries(
            rd2, internal_key=["K1", "K2"], rank=[2, 1], ff_mcap=[90.0, 100.0], review_date=[rd2, rd2]
        )
        membership = _make_membership(
            internal_key=["K1", "K2"], is_member=[True, True], entry_reason=["top_550", "top_550"]
        )
        result = build_membership_table.fn(
            [assets1, assets2], [entries1, entries2], [membership, membership], [rd1, rd2]
        )

        day1 = result.filter(pl.col("date") == rd1).sort("rank")
        assert day1["ric"].to_list() == ["ING.AS", "ASML.AS"]

        day3 = result.filter(pl.col("date") == rd2).sort("rank")
        assert day3["ric"].to_list() == ["ASML.AS", "INGA.AS"]


class TestValidateMembershipTable:
    """Tests for membership table validation."""

    def test_empty_table_warns(self):
        """Empty membership table logs a warning and returns."""
        df = pl.DataFrame(schema={"date": pl.Date, "internal_key": pl.Utf8, "ric": pl.Utf8, "rank": pl.Int64})
        validate_membership_table.fn(df, [])

    def test_valid_membership_passes(self):
        """Table with ranks 1-600 passes validation."""
        rd = date(2024, 9, 1)
        df = pl.DataFrame(
            {
                "date": [rd] * 600,
                "internal_key": [f"K{i}" for i in range(1, 601)],
                "ric": [f"R{i}" for i in range(1, 601)],
                "rank": list(range(1, 601)),
            }
        )
        validate_membership_table.fn(df, [rd])

    def test_missing_ranks_warns(self):
        """Table missing some ranks logs a warning."""
        rd = date(2024, 9, 1)
        df = pl.DataFrame(
            {
                "date": [rd] * 50,
                "internal_key": [f"K{i}" for i in range(1, 51)],
                "ric": [f"R{i}" for i in range(1, 51)],
                "rank": list(range(1, 51)),
            }
        )
        validate_membership_table.fn(df, [rd])

    def test_missing_review_date_warns(self):
        """Validation warns when a review date has no rows in the table."""
        rd = date(2024, 9, 1)
        missing_rd = date(2024, 10, 1)
        df = pl.DataFrame({"date": [rd], "internal_key": ["K1"], "ric": ["R1"], "rank": [1]})
        validate_membership_table.fn(df, [rd, missing_rd])


class TestBuildAssetsFromMembership:
    """Tests for deriving assets table from membership."""

    def test_empty_membership(self):
        """Empty membership produces empty assets with correct schema."""
        from idx.ranking import _OUTPUT_SCHEMA

        df = pl.DataFrame(schema=_OUTPUT_SCHEMA)
        result = build_assets_from_membership.fn(df)
        assert "internal_key" in result.columns
        assert "first_included" in result.columns
        assert "last_included" in result.columns
        assert "yukka_id" in result.columns
        assert len(result) == 0

    def test_single_member(self):
        """Single member produces one asset row with correct dates."""
        rd1 = date(2025, 1, 1)
        rd2 = date(2025, 1, 3)
        df = pl.DataFrame(
            {
                "date": [rd1, rd2],
                "internal_key": ["K1", "K1"],
                "ric": ["R1", "R1"],
                "name": ["N1", "N1"],
                "country": ["DE", "DE"],
                "currency": ["EUR", "EUR"],
                "isin": ["IS1", "IS1"],
                "sedol": ["SE1", "SE1"],
                "rank": [1, 1],
                "yukka_id": ["YK1", "YK1"],
            }
        )
        result = build_assets_from_membership.fn(df)
        assert len(result) == 1
        assert result["internal_key"][0] == "K1"
        assert result["first_included"][0] == rd1
        assert result["last_included"][0] == rd2
        assert result["yukka_id"][0] == "YK1"

    def test_takes_last_non_null(self):
        """Assets use the last non-null value for each identifier column."""
        rd1 = date(2025, 1, 1)
        rd2 = date(2025, 1, 2)
        df = pl.DataFrame(
            {
                "date": [rd1, rd2],
                "internal_key": ["K1", "K1"],
                "ric": ["OLD.R", "NEW.R"],
                "name": ["OldName", "NewName"],
                "country": ["DE", "DE"],
                "currency": ["EUR", "EUR"],
                "isin": ["IS1", None],
                "sedol": [None, "SE2"],
                "rank": [1, 1],
                "yukka_id": [None, "YK1"],
            }
        )
        result = build_assets_from_membership.fn(df)
        assert result["ric"][0] == "NEW.R"
        assert result["name"][0] == "NewName"
        assert result["isin"][0] == "IS1"
        assert result["sedol"][0] == "SE2"
        assert result["yukka_id"][0] == "YK1"

    def test_multiple_members(self):
        """Multiple members produce multiple asset rows sorted by internal_key."""
        rd = date(2025, 1, 1)
        df = pl.DataFrame(
            {
                "date": [rd, rd],
                "internal_key": ["K2", "K1"],
                "ric": ["R2", "R1"],
                "name": ["N2", "N1"],
                "country": ["FR", "DE"],
                "currency": ["EUR", "EUR"],
                "isin": ["IS2", "IS1"],
                "sedol": ["SE2", "SE1"],
                "rank": [2, 1],
                "yukka_id": ["YK2", "YK1"],
            }
        )
        result = build_assets_from_membership.fn(df)
        assert len(result) == 2
        assert result["internal_key"].to_list() == ["K1", "K2"]
