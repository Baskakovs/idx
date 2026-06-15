"""Tests for idx.storage module."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from idx.storage import _chunked_executemany, _hash_to_bigint


class TestHashToBigint:
    """Tests for deterministic hash function."""

    def test_deterministic(self):
        """Same inputs always produce the same hash."""
        assert _hash_to_bigint("abc") == _hash_to_bigint("abc")

    def test_different_inputs_differ(self):
        """Different inputs produce different hashes."""
        assert _hash_to_bigint("abc") != _hash_to_bigint("def")

    def test_multiple_parts(self):
        """Multiple parts are joined with pipe separator."""
        h1 = _hash_to_bigint("a", "b")
        h2 = _hash_to_bigint("a|b")
        assert h1 == h2

    def test_returns_int(self):
        """Result is a Python int (signed 64-bit range)."""
        result = _hash_to_bigint("test")
        assert isinstance(result, int)
        assert -(2**63) <= result < 2**63

    def test_order_matters(self):
        """Swapping parts produces a different hash."""
        assert _hash_to_bigint("a", "b") != _hash_to_bigint("b", "a")


class TestChunkedExecutemany:
    """Tests for chunked bulk insert helper."""

    def test_single_chunk(self):
        """All rows fit in one chunk."""
        cur = MagicMock()
        params = [(1,), (2,), (3,)]
        _chunked_executemany(cur, "INSERT ...", params, chunk_size=10)
        cur.executemany.assert_called_once_with("INSERT ...", [(1,), (2,), (3,)])

    def test_multiple_chunks(self):
        """Rows are split into correct chunks."""
        cur = MagicMock()
        params = [(i,) for i in range(5)]
        _chunked_executemany(cur, "INSERT ...", params, chunk_size=2)
        assert cur.executemany.call_count == 3
        cur.executemany.assert_any_call("INSERT ...", [(0,), (1,)])
        cur.executemany.assert_any_call("INSERT ...", [(2,), (3,)])
        cur.executemany.assert_any_call("INSERT ...", [(4,)])

    def test_empty_params(self):
        """No rows means no calls."""
        cur = MagicMock()
        _chunked_executemany(cur, "INSERT ...", [], chunk_size=10)
        cur.executemany.assert_not_called()


class TestWriteAssets:
    """Tests for write_assets task."""

    @patch("idx.storage.get_connection")
    def test_write_assets_inserts_rows(self, mock_get_conn):
        """write_assets DELETEs then INSERTs for each asset."""
        from idx.storage import write_assets

        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_get_conn.return_value = mock_conn

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

        assert mock_cur.execute.call_count == 1  # DELETE
        assert mock_cur.executemany.call_count == 1  # INSERT
        insert_params = mock_cur.executemany.call_args[0][1]
        assert len(insert_params) == 2

    @patch("idx.storage.get_connection")
    def test_write_assets_empty_df(self, mock_get_conn):
        """write_assets does nothing for empty DataFrame."""
        from idx.storage import write_assets

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
        mock_get_conn.assert_not_called()

    @patch("idx.storage.get_connection")
    def test_write_assets_reraises_on_error(self, mock_get_conn):
        """write_assets logs and re-raises DB errors."""
        from idx.storage import write_assets

        mock_get_conn.side_effect = Exception("connection failed")
        df = pl.DataFrame(
            {
                "internal_key": ["K1"],
                "ric": ["R1"],
                "name": ["N1"],
                "country": ["DE"],
                "currency": ["EUR"],
                "isin": [None],
                "sedol": [None],
                "yukka_id": [None],
            }
        )
        with pytest.raises(Exception, match="connection failed"):
            write_assets.fn(df)


class TestWriteRanks:
    """Tests for write_ranks task."""

    @patch("idx.storage.get_connection")
    def test_write_ranks_inserts_unpivoted_rows(self, mock_get_conn):
        """write_ranks unpivots and inserts rank rows."""
        from idx.storage import write_ranks

        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_get_conn.return_value = mock_conn

        ranking_df = pl.DataFrame(
            {
                "date": [date(2024, 9, 1), date(2024, 12, 1)],
                "R1": [1, 2],
                "R2": [3, None],
            }
        )
        assets_df = pl.DataFrame({"internal_key": ["K1", "K2"], "ric": ["R1", "R2"]})

        write_ranks.fn(ranking_df, assets_df)

        assert mock_cur.execute.call_count == 1  # DELETE
        assert mock_cur.executemany.call_count == 1  # INSERT
        insert_params = mock_cur.executemany.call_args[0][1]
        # R1 has 2 dates, R2 has 1 (null filtered out) = 3 rows
        assert len(insert_params) == 3

    @patch("idx.storage.get_connection")
    def test_write_ranks_empty_ranking(self, mock_get_conn):
        """write_ranks does nothing for empty ranking table."""
        from idx.storage import write_ranks

        ranking_df = pl.DataFrame({"date": []}).cast({"date": pl.Date})
        assets_df = pl.DataFrame({"internal_key": [], "ric": []})
        write_ranks.fn(ranking_df, assets_df)
        mock_get_conn.assert_not_called()

    @patch("idx.storage.get_connection")
    def test_write_ranks_warns_unmapped_rics(self, mock_get_conn):
        """write_ranks warns about RICs not in assets."""
        from idx.storage import write_ranks

        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_get_conn.return_value = mock_conn

        ranking_df = pl.DataFrame({"date": [date(2024, 9, 1)], "UNKNOWN_RIC": [5]})
        assets_df = pl.DataFrame({"internal_key": ["K1"], "ric": ["R1"]})

        # All RICs unmapped -> no params -> early return
        write_ranks.fn(ranking_df, assets_df)
        mock_cur.executemany.assert_not_called()

    @patch("idx.storage.get_connection")
    def test_write_ranks_reraises_on_error(self, mock_get_conn):
        """write_ranks logs and re-raises DB errors."""
        from idx.storage import write_ranks

        mock_get_conn.side_effect = Exception("db error")
        ranking_df = pl.DataFrame({"date": [date(2024, 9, 1)], "R1": [1]})
        assets_df = pl.DataFrame({"internal_key": ["K1"], "ric": ["R1"]})
        with pytest.raises(Exception, match="db error"):
            write_ranks.fn(ranking_df, assets_df)


class TestWriteReviewDetails:
    """Tests for write_review_details task."""

    @patch("idx.storage.get_connection")
    def test_write_review_details_inserts_joined_rows(self, mock_get_conn):
        """write_review_details joins entries+membership and inserts."""
        from idx.storage import write_review_details

        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_get_conn.return_value = mock_conn

        entries_df = pl.DataFrame(
            {
                "internal_key": ["K1", "K2"],
                "review_date": [date(2024, 9, 1), date(2024, 9, 1)],
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

        write_review_details.fn([entries_df], [membership_df])

        assert mock_cur.execute.call_count == 1  # DELETE
        assert mock_cur.executemany.call_count == 1  # INSERT
        insert_params = mock_cur.executemany.call_args[0][1]
        assert len(insert_params) == 2

    @patch("idx.storage.get_connection")
    def test_write_review_details_empty(self, mock_get_conn):
        """write_review_details does nothing for empty list."""
        from idx.storage import write_review_details

        write_review_details.fn([], [])
        mock_get_conn.assert_not_called()

    @patch("idx.storage.get_connection")
    def test_write_review_details_reraises_on_error(self, mock_get_conn):
        """write_review_details logs and re-raises DB errors."""
        from idx.storage import write_review_details

        mock_get_conn.side_effect = Exception("db error")
        entries_df = pl.DataFrame(
            {
                "internal_key": ["K1"],
                "review_date": [date(2024, 9, 1)],
                "ff_mcap": [100.0],
                "rank": [1],
                "comment": [None],
            }
        )
        membership_df = pl.DataFrame(
            {
                "internal_key": ["K1"],
                "is_member": [True],
                "entry_reason": ["top_550"],
            }
        )
        with pytest.raises(Exception, match="db error"):
            write_review_details.fn([entries_df], [membership_df])
