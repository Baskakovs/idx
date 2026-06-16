"""R2 Parquet storage layer for STOXX index data."""

from __future__ import annotations

import io
import os
from datetime import date

import boto3
import polars as pl
from prefect import task
from prefect.blocks.system import Secret
from prefect.cache_policies import NO_CACHE

from idx import get_logger


def _get_s3_client() -> boto3.client:
    """Create a boto3 S3 client configured for Cloudflare R2."""
    return boto3.client(
        "s3",
        endpoint_url=Secret.load("r2-endpoint-url").get(),
        aws_access_key_id=Secret.load("r2-access-key-id").get(),
        aws_secret_access_key=Secret.load("r2-secret-access-key").get(),
        region_name="auto",
    )


R2_BUCKET = "idx-extract"
R2_PREFIX = os.environ.get("R2_PREFIX", "STOXX600_dev")


def _upload_parquet(df: pl.DataFrame, key: str) -> None:
    """Write a DataFrame as Parquet and upload to R2."""
    logger = get_logger()
    full_key = f"{R2_PREFIX}/{key}"
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    client = _get_s3_client()
    client.put_object(Bucket=R2_BUCKET, Key=full_key, Body=buf.getvalue())
    logger.info("Uploaded %s (%d rows)", full_key, len(df))


@task(cache_policy=NO_CACHE)
def write_assets(enriched_assets: pl.DataFrame) -> None:
    """Write enriched assets to assets.parquet in R2."""
    logger = get_logger()
    if enriched_assets.is_empty():
        logger.warning("No assets to write")
        return
    _upload_parquet(enriched_assets, "assets.parquet")


@task(cache_policy=NO_CACHE)
def write_ranks(ranking_df: pl.DataFrame) -> None:
    """Write wide-format ranking table to rankings.parquet in R2."""
    logger = get_logger()
    if ranking_df.is_empty():
        logger.warning("No ranking data to write")
        return
    _upload_parquet(ranking_df, "ranking.parquet")


@task(cache_policy=NO_CACHE)
def write_reviews(entries_df: pl.DataFrame, membership_df: pl.DataFrame, review_date: date) -> None:
    """Join entries with membership and write to reviews/{review_date}.parquet in R2."""
    logger = get_logger()
    members = membership_df.filter(pl.col("is_member")).select("internal_key", "entry_reason")
    joined = entries_df.join(members, on="internal_key", how="left")
    if joined.is_empty():
        logger.warning("No review data for %s", review_date)
        return
    # Replace any Object columns with String equivalents so Parquet can serialize them
    for col_name, dtype in zip(joined.columns, joined.dtypes, strict=False):
        if dtype == pl.Object:
            joined = joined.with_columns(
                pl.Series(
                    col_name, [v.value if v is not None else None for v in joined[col_name].to_list()], dtype=pl.Utf8
                )
            )
    _upload_parquet(joined, f"reviews/{review_date}.parquet")
