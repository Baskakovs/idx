"""Enrich assets with Yukka entity IDs via ISIN and RIC lookups."""

from __future__ import annotations

import httpx
import polars as pl
from prefect import task
from prefect.artifacts import create_table_artifact
from prefect.blocks.system import Secret
from prefect.cache_policies import NO_CACHE

from idx import get_logger

_BASE_URL = "https://metadata.api.yukkalab.com"
_BATCH_SIZE = 100


def _build_client() -> httpx.Client:
    """Build an authenticated HTTP client for the Yukka metadata API."""
    token = Secret.load("yukka-token").get()  # type: ignore[union-attr]  # ty: ignore[unresolved-attribute]
    return httpx.Client(
        base_url=_BASE_URL,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )


def _batch_lookup(client: httpx.Client, endpoint: str, identifiers: list[str]) -> dict[str, str]:
    """POST identifiers in batches and collect alpha_id mappings.

    Args:
        client: Authenticated HTTP client.
        endpoint: API endpoint path.
        identifiers: List of identifiers (ISINs or RICs) to look up.

    Returns:
        Mapping of identifier to alpha_id for successful lookups.
    """
    result: dict[str, str] = {}
    for i in range(0, len(identifiers), _BATCH_SIZE):
        batch = identifiers[i : i + _BATCH_SIZE]
        resp = client.post(endpoint, json=batch)
        resp.raise_for_status()
        data = resp.json()
        for key, entity in data.items():
            if entity is not None and "alpha_id" in entity:
                result[key] = entity["alpha_id"]
    return result


def _resolve_by_isin(client: httpx.Client, assets_df: pl.DataFrame) -> dict[str, str]:
    """Resolve internal_key → yukka_id via ISIN lookup."""
    logger = get_logger()
    if "isin" not in assets_df.columns:
        return {}
    isins = assets_df.filter(pl.col("isin").is_not_null())["isin"].unique().to_list()
    if not isins:
        return {}
    isin_to_yukka = _batch_lookup(client, "/v2/isin_to_entity", isins)
    logger.info("ISIN lookup resolved %d / %d", len(isin_to_yukka), len(isins))

    key_map: dict[str, str] = {}
    for row in assets_df.filter(pl.col("isin").is_not_null()).select("internal_key", "isin").iter_rows(named=True):
        if row["isin"] in isin_to_yukka and row["internal_key"] not in key_map:
            key_map[row["internal_key"]] = isin_to_yukka[row["isin"]]
    return key_map


def _resolve_by_ric(client: httpx.Client, assets_df: pl.DataFrame, unresolved_keys: set[str]) -> dict[str, str]:
    """Resolve internal_key → yukka_id via RIC fallback for unresolved keys."""
    logger = get_logger()
    if not unresolved_keys:
        return {}
    unresolved_df = assets_df.filter(pl.col("internal_key").is_in(list(unresolved_keys)))
    rics = [r for r in unresolved_df["ric"].unique().drop_nulls().to_list() if r]
    if not rics:
        return {}
    ric_to_yukka = _batch_lookup(client, "/ric_to_entity", rics)
    logger.info("RIC lookup resolved %d / %d", len(ric_to_yukka), len(rics))

    key_map: dict[str, str] = {}
    for row in unresolved_df.select("internal_key", "ric").unique().iter_rows(named=True):
        if row["ric"] in ric_to_yukka and row["internal_key"] not in key_map:
            key_map[row["internal_key"]] = ric_to_yukka[row["ric"]]
    return key_map


@task(cache_policy=NO_CACHE)
def resolve_yukka_ids(assets_df: pl.DataFrame) -> pl.DataFrame:
    """Enrich assets DataFrame with yukka_id column via ISIN and RIC lookups.

    Args:
        assets_df: DataFrame with at least 'internal_key' and 'ric' columns,
            and optionally an 'isin' column.

    Returns:
        DataFrame with an added 'yukka_id' column (nullable string).
    """
    logger = get_logger()
    client = _build_client()
    try:
        key_yukka_map = _resolve_by_isin(client, assets_df)
        all_keys = set(assets_df["internal_key"].unique().to_list())
        unresolved_keys = all_keys - set(key_yukka_map.keys())
        key_yukka_map.update(_resolve_by_ric(client, assets_df, unresolved_keys))
    finally:
        client.close()

    logger.info("Total resolved: %d / %d assets", len(key_yukka_map), len(all_keys))

    if key_yukka_map:
        mapping_df = pl.DataFrame(
            {"internal_key": list(key_yukka_map.keys()), "yukka_id": list(key_yukka_map.values())}
        )
    else:
        mapping_df = pl.DataFrame(
            {"internal_key": pl.Series([], dtype=pl.Utf8), "yukka_id": pl.Series([], dtype=pl.Utf8)}
        )

    if "yukka_id" in assets_df.columns:
        assets_df = assets_df.drop("yukka_id")

    return assets_df.join(mapping_df, on="internal_key", how="left")


@task(cache_policy=NO_CACHE)
def report_unresolved_assets(assets_df: pl.DataFrame) -> None:
    """Create a Prefect artifact reporting all assets without a Yukka ID.

    Args:
        assets_df: Enriched assets DataFrame with a 'yukka_id' column.
    """
    logger = get_logger()
    if "yukka_id" not in assets_df.columns:
        logger.warning("No yukka_id column, skipping artifact")
        return

    unresolved = assets_df.filter(pl.col("yukka_id").is_null())

    if len(unresolved) == 0:
        logger.info("All assets resolved to Yukka IDs")
        return

    logger.warning("%d assets could not be resolved to Yukka IDs", len(unresolved))

    report_cols = [c for c in ("internal_key", "isin", "ric", "name", "country", "currency") if c in unresolved.columns]
    report_df = unresolved.select(report_cols).unique(subset=["internal_key"]).sort("internal_key")
    date_cols = [c for c in report_df.columns if report_df[c].dtype == pl.Date]
    report_df = report_df.with_columns(pl.col(c).cast(pl.Utf8) for c in date_cols)
    table = report_df.to_dicts()

    create_table_artifact(
        key="unresolved-yukka-assets",
        table=table,
        description=f"{len(table)} unique ISINs could not be resolved to Yukka entity IDs",
    )
