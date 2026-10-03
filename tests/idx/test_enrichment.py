"""Tests for idx.enrichment module."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl

from idx.enrichment import enrich_membership_with_yukka_ids, report_unresolved_assets, resolve_yukka_ids


class TestResolveYukkaIds:
    """Tests for Yukka ID resolution."""

    def test_isin_lookup(self, mock_yukka_client):
        """Assets with ISINs get yukka_ids via ISIN lookup."""
        mock_yukka_client.post.return_value = MagicMock(
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

    def test_ric_fallback(self, mock_yukka_client):
        """Assets without ISIN fall back to RIC lookup."""
        mock_yukka_client.post.return_value = MagicMock(
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

    def test_no_match_returns_null(self, mock_yukka_client):
        """Unresolved assets get null yukka_id."""
        mock_yukka_client.post.return_value = MagicMock(
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

    def test_existing_yukka_id_column_replaced(self, mock_yukka_client):
        """If yukka_id column already exists, it gets replaced."""
        mock_yukka_client.post.return_value = MagicMock(
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


class TestEnrichMembershipWithYukkaIds:
    """Tests for membership-level Yukka ID enrichment."""

    def test_isin_lookup(self, mock_yukka_client):
        """Membership rows with ISINs get yukka_ids via ISIN lookup."""
        mock_yukka_client.post.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value={"ISIN1": {"alpha_id": "YK1"}}),
            raise_for_status=MagicMock(),
        )

        df = pl.DataFrame(
            {
                "date": [date(2025, 1, 1)],
                "internal_key": ["K1"],
                "ric": ["R1"],
                "isin": ["ISIN1"],
                "rank": [1],
            }
        )

        result = enrich_membership_with_yukka_ids.fn(df)
        assert "yukka_id" in result.columns
        assert result["yukka_id"][0] == "YK1"

    def test_ric_fallback(self, mock_yukka_client):
        """Membership rows without ISIN match fall back to RIC lookup."""
        # ISIN lookup returns nothing, RIC lookup resolves
        mock_yukka_client.post.side_effect = [
            MagicMock(
                status_code=200,
                json=MagicMock(return_value={}),
                raise_for_status=MagicMock(),
            ),
            MagicMock(
                status_code=200,
                json=MagicMock(return_value={"R1": {"alpha_id": "YK_RIC"}}),
                raise_for_status=MagicMock(),
            ),
        ]

        df = pl.DataFrame(
            {
                "date": [date(2025, 1, 1)],
                "internal_key": ["K1"],
                "ric": ["R1"],
                "isin": ["ISIN1"],
                "rank": [1],
            }
        )

        result = enrich_membership_with_yukka_ids.fn(df)
        assert result["yukka_id"][0] == "YK_RIC"

    def test_no_match_returns_null(self, mock_yukka_client):
        """Unresolved membership rows get null yukka_id."""
        mock_yukka_client.post.return_value = MagicMock(
            status_code=200, json=MagicMock(return_value={}), raise_for_status=MagicMock()
        )

        df = pl.DataFrame(
            {
                "date": [date(2025, 1, 1)],
                "internal_key": ["K1"],
                "ric": ["R1"],
                "isin": [None],
                "rank": [1],
            }
        )

        result = enrich_membership_with_yukka_ids.fn(df)
        assert result["yukka_id"][0] is None

    def test_no_isin_column(self, mock_yukka_client):
        """Membership without isin column falls back to RIC lookup."""
        mock_yukka_client.post.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value={"R1": {"alpha_id": "YK_RIC"}}),
            raise_for_status=MagicMock(),
        )

        df = pl.DataFrame(
            {
                "date": [date(2025, 1, 1)],
                "internal_key": ["K1"],
                "ric": ["R1"],
                "rank": [1],
            }
        )

        result = enrich_membership_with_yukka_ids.fn(df)
        assert result["yukka_id"][0] == "YK_RIC"

    def test_existing_yukka_id_replaced(self, mock_yukka_client):
        """If yukka_id column already exists, it gets replaced."""
        mock_yukka_client.post.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value={"ISIN1": {"alpha_id": "YK_NEW"}}),
            raise_for_status=MagicMock(),
        )

        df = pl.DataFrame(
            {
                "date": [date(2025, 1, 1)],
                "internal_key": ["K1"],
                "ric": ["R1"],
                "isin": ["ISIN1"],
                "rank": [1],
                "yukka_id": ["YK_OLD"],
            }
        )

        result = enrich_membership_with_yukka_ids.fn(df)
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
