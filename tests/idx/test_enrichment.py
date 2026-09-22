"""Tests for idx.enrichment module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import polars as pl

from idx.enrichment import report_unresolved_assets, resolve_yukka_ids


class TestResolveYukkaIds:
    """Tests for Yukka ID resolution."""

    @patch("idx.enrichment._build_client")
    def test_isin_lookup(self, mock_build):
        """Assets with ISINs get yukka_ids via ISIN lookup."""
        mock_client = MagicMock()
        mock_build.return_value = mock_client

        # ISIN lookup returns a mapping
        mock_client.post.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value={"ISIN1": {"alpha_id": "YK1"}}),
            raise_for_status=MagicMock(),
        )

        df = pl.DataFrame(
            {
                "internal_key": ["K1"],
                "ric": ["R1"],
                "isin": ["ISIN1"],
            }
        )

        result = resolve_yukka_ids.fn(df)
        assert "yukka_id" in result.columns
        assert result["yukka_id"][0] == "YK1"

    @patch("idx.enrichment._build_client")
    def test_ric_fallback(self, mock_build):
        """Assets without ISIN fall back to RIC lookup."""
        mock_client = MagicMock()
        mock_build.return_value = mock_client

        # RIC lookup returns mapping (only one call since no ISINs to look up)
        mock_client.post.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value={"R1": {"alpha_id": "YK_RIC"}}),
            raise_for_status=MagicMock(),
        )

        df = pl.DataFrame(
            {
                "internal_key": ["K1"],
                "ric": ["R1"],
            }
        )

        result = resolve_yukka_ids.fn(df)
        assert result["yukka_id"][0] == "YK_RIC"

    @patch("idx.enrichment._build_client")
    def test_no_match_returns_null(self, mock_build):
        """Unresolved assets get null yukka_id."""
        mock_client = MagicMock()
        mock_build.return_value = mock_client
        mock_client.post.return_value = MagicMock(
            status_code=200, json=MagicMock(return_value={}), raise_for_status=MagicMock()
        )

        df = pl.DataFrame(
            {
                "internal_key": ["K1"],
                "ric": ["R1"],
                "isin": [None],
            }
        )

        result = resolve_yukka_ids.fn(df)
        assert result["yukka_id"][0] is None

    @patch("idx.enrichment._build_client")
    def test_existing_yukka_id_column_replaced(self, mock_build):
        """If yukka_id column already exists, it gets replaced."""
        mock_client = MagicMock()
        mock_build.return_value = mock_client
        mock_client.post.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value={"ISIN1": {"alpha_id": "YK_NEW"}}),
            raise_for_status=MagicMock(),
        )

        df = pl.DataFrame(
            {
                "internal_key": ["K1"],
                "ric": ["R1"],
                "isin": ["ISIN1"],
                "yukka_id": ["YK_OLD"],
            }
        )

        result = resolve_yukka_ids.fn(df)
        assert result["yukka_id"][0] == "YK_NEW"


class TestReportUnresolvedAssets:
    """Tests for unresolved assets reporting."""

    @patch("idx.enrichment.create_table_artifact")
    def test_all_resolved(self, mock_artifact):
        """No artifact created when all assets are resolved."""
        df = pl.DataFrame(
            {
                "internal_key": ["K1"],
                "ric": ["R1"],
                "yukka_id": ["YK1"],
            }
        )
        report_unresolved_assets.fn(df)
        mock_artifact.assert_not_called()

    @patch("idx.enrichment.create_table_artifact")
    def test_unresolved_creates_artifact(self, mock_artifact):
        """Artifact created listing unresolved assets."""
        df = pl.DataFrame(
            {
                "internal_key": ["K1", "K2"],
                "ric": ["R1", "R2"],
                "name": ["N1", "N2"],
                "country": ["DE", "FR"],
                "currency": ["EUR", "EUR"],
                "yukka_id": ["YK1", None],
            }
        )
        report_unresolved_assets.fn(df)
        mock_artifact.assert_called_once()
        call_kwargs = mock_artifact.call_args[1]
        assert len(call_kwargs["table"]) == 1

    def test_no_yukka_column_warns(self):
        """Missing yukka_id column logs warning and returns."""
        df = pl.DataFrame({"internal_key": ["K1"], "ric": ["R1"]})
        # Should not raise
        report_unresolved_assets.fn(df)
