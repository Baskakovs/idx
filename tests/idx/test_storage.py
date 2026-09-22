"""Tests for idx.storage module."""

from __future__ import annotations

import io
from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from idx.storage import write_assets, write_ranks, write_reviews


@pytest.fixture
def mock_s3():
    """Patch _get_s3_client and R2_BUCKET env var, return the mock client."""
    mock_client = MagicMock()
    with patch("idx.storage._get_s3_client", return_value=mock_client):
        yield mock_client


class TestWriteAssets:
    """Tests for write_assets task."""

    def test_uploads_parquet(self, mock_s3):
        """Writes enriched assets as parquet to R2."""
        df = pl.DataFrame(
            {
                "internal_key": ["K1", "K2"],
                "ric": ["R1", "R2"],
                "name": ["N1", "N2"],
                "country": ["DE", "FR"],
                "currency": ["EUR", "EUR"],
                "isin": ["IS1", None],
                "sedol": [None, "SE2"],
                "yukka_id": ["Y1", "Y2"],
            }
        )
        write_assets.fn(df)

        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "idx-extract"
        assert call_kwargs["Key"] == "STOXX600_dev/assets.parquet"

        # Verify the bytes are valid parquet
        result = pl.read_parquet(io.BytesIO(call_kwargs["Body"]))
        assert len(result) == 2
        assert result.columns == df.columns

    def test_empty_df_skips_upload(self, mock_s3):
        """Empty DataFrame skips upload."""
        df = pl.DataFrame(
            {
                "internal_key": [],
                "ric": [],
                "name": [],
                "country": [],
                "currency": [],
                "isin": [],
                "sedol": [],
                "yukka_id": [],
            }
        )
        write_assets.fn(df)
        mock_s3.put_object.assert_not_called()


class TestWriteRanks:
    """Tests for write_ranks task."""

    def test_uploads_parquet(self, mock_s3):
        """Writes ranking table as parquet to R2."""
        ranking_df = pl.DataFrame(
            {
                "date": [date(2024, 9, 1), date(2024, 12, 1)],
                "R1": [1, 2],
                "R2": [3, None],
            }
        )
        write_ranks.fn(ranking_df)

        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Key"] == "STOXX600_dev/ranking.parquet"

        result = pl.read_parquet(io.BytesIO(call_kwargs["Body"]))
        assert len(result) == 2

    def test_empty_df_skips_upload(self, mock_s3):
        """Empty DataFrame skips upload."""
        ranking_df = pl.DataFrame({"date": []}).cast({"date": pl.Date})
        write_ranks.fn(ranking_df)
        mock_s3.put_object.assert_not_called()


class TestWriteReviews:
    """Tests for write_reviews task."""

    def test_joins_and_uploads(self, mock_s3):
        """Joins entries with membership and writes per-review parquet."""
        entries_df = pl.DataFrame(
            {
                "internal_key": ["K1", "K2"],
                "review_date": [date(2024, 9, 15), date(2024, 9, 15)],
                "ff_mcap": [100.0, 200.0],
                "rank": [1, 2],
                "comment": [None, "test"],
            }
        )
        membership_df = pl.DataFrame(
            {
                "internal_key": ["K1", "K2"],
                "is_member": [True, False],
                "entry_reason": ["top_550", "top_550"],
            }
        )

        write_reviews.fn(entries_df, membership_df, date(2024, 9, 15))

        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Key"] == "STOXX600_dev/reviews/2024-09-15.parquet"

        result = pl.read_parquet(io.BytesIO(call_kwargs["Body"]))
        assert len(result) == 2
        # K1 is a member -> entry_reason should be "top_550"
        k1_row = result.filter(pl.col("internal_key") == "K1")
        assert k1_row["entry_reason"][0] == "top_550"
        # K2 is not a member -> entry_reason should be null (left join)
        k2_row = result.filter(pl.col("internal_key") == "K2")
        assert k2_row["entry_reason"][0] is None

    def test_empty_entries_skips_upload(self, mock_s3):
        """Empty entries DataFrame skips upload."""
        entries_df = pl.DataFrame(
            {
                "internal_key": [],
                "review_date": [],
                "ff_mcap": [],
                "rank": [],
                "comment": [],
            },
            schema={
                "internal_key": pl.Utf8,
                "review_date": pl.Date,
                "ff_mcap": pl.Float64,
                "rank": pl.Int64,
                "comment": pl.Utf8,
            },
        )
        membership_df = pl.DataFrame(
            {
                "internal_key": [],
                "is_member": [],
                "entry_reason": [],
            },
            schema={
                "internal_key": pl.Utf8,
                "is_member": pl.Boolean,
                "entry_reason": pl.Utf8,
            },
        )
        write_reviews.fn(entries_df, membership_df, date(2024, 9, 15))
        mock_s3.put_object.assert_not_called()
