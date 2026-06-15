"""Tests for idx.ranking module."""

from __future__ import annotations

from datetime import date

import polars as pl

from idx.ranking import build_ranking_table, validate_ranking_table


class TestBuildRankingTable:
    """Tests for wide-format ranking table construction."""

    def test_empty_review_dates(self):
        """Empty review dates produce an empty DataFrame."""
        assets = pl.DataFrame({"internal_key": [], "ric": []})
        result = build_ranking_table.fn(assets, [], [], [])
        assert "date" in result.columns
        assert len(result) == 0

    def test_single_review_date(self):
        """Single review date produces rows from that date to today."""
        rd = date(2024, 12, 1)
        assets = pl.DataFrame({"internal_key": ["K1", "K2"], "ric": ["R1", "R2"]})
        entries = pl.DataFrame(
            {
                "internal_key": ["K1", "K2"],
                "review_date": [rd, rd],
                "rank": [1, 2],
                "ff_mcap": [100.0, 50.0],
            }
        )
        membership = pl.DataFrame(
            {
                "internal_key": ["K1", "K2"],
                "is_member": [True, False],
                "entry_reason": ["top_550", "top_550"],
            }
        )
        result = build_ranking_table.fn(assets, [entries], [membership], [rd])
        assert "date" in result.columns
        assert "R1" in result.columns
        # R1 is a member -> has rank; R2 is not -> sentinel 0 -> null after cleanup
        row = result.filter(pl.col("date") == rd)
        assert row["R1"][0] == 1
        assert row["R2"][0] is None

    def test_forward_fill(self):
        """Ranks are forward-filled across daily dates."""
        rd = date(2025, 6, 1)
        assets = pl.DataFrame({"internal_key": ["K1"], "ric": ["R1"]})
        entries = pl.DataFrame({"internal_key": ["K1"], "review_date": [rd], "rank": [5], "ff_mcap": [100.0]})
        membership = pl.DataFrame({"internal_key": ["K1"], "is_member": [True], "entry_reason": ["top_550"]})
        result = build_ranking_table.fn(assets, [entries], [membership], [rd])
        # Day after review should also have rank 1 (re-ranked from original 5)
        day_after = result.filter(pl.col("date") == date(2025, 6, 2))
        if not day_after.is_empty():
            assert day_after["R1"][0] == 1


class TestValidateRankingTable:
    """Tests for ranking table validation."""

    def test_empty_table_warns(self):
        """Empty ranking table logs a warning and returns."""
        df = pl.DataFrame({"date": []}).cast({"date": pl.Date})
        validate_ranking_table.fn(df, [])

    def test_valid_ranking_passes(self):
        """Table with ranks 1-100 passes validation."""
        rd = date(2024, 9, 1)
        data = {"date": [rd]}
        for i in range(1, 101):
            data[f"RIC{i}"] = [i]
        # Add some extra columns with None (non-members)
        data["RIC_EXTRA"] = [None]
        df = pl.DataFrame(data)
        # Should not raise
        validate_ranking_table.fn(df, [rd])

    def test_missing_ranks_warns(self):
        """Table missing some ranks 1-100 logs a warning."""
        rd = date(2024, 9, 1)
        data = {"date": [rd]}
        # Only ranks 1-50
        for i in range(1, 51):
            data[f"RIC{i}"] = [i]
        df = pl.DataFrame(data)
        # Should not raise (just warns)
        validate_ranking_table.fn(df, [rd])
