"""Shared test configuration and Prefect mock fixtures."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
def mock_yukka_client():
    """Patch _build_client in idx.enrichment and return the mock HTTP client."""
    mock_client = MagicMock()
    with patch("idx.enrichment._build_client", return_value=mock_client):
        yield mock_client


@pytest.fixture
def mock_s3():
    """Patch _get_s3_client in idx.storage and return the mock S3 client."""
    mock_client = MagicMock()
    with patch("idx.storage._get_s3_client", return_value=mock_client):
        yield mock_client


@pytest.fixture
def mock_slack():
    """Patch SlackWebhook in idx.main and return the mock."""
    with patch("idx.main.SlackWebhook", new_callable=AsyncMock) as mock:
        yield mock
