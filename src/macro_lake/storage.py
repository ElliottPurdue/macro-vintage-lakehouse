"""Clients for the lake: boto3 for object operations, DuckDB for SQL over Parquet."""

from __future__ import annotations

import boto3
import duckdb
from botocore.config import Config

from macro_lake.settings import LakeSettings


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def s3_client(settings: LakeSettings):
    return boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        region_name=settings.region,
        aws_access_key_id=settings.access_key_id,
        aws_secret_access_key=settings.secret_access_key,
        config=Config(
            s3={"addressing_style": "path" if settings.url_style == "path" else "virtual"},
            retries={"mode": "standard", "max_attempts": 5},
            # Only send checksums where S3 requires them. Not every
            # S3-compatible store accepts the newer botocore defaults.
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def duckdb_connect(settings: LakeSettings, database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """DuckDB connection that can read and write s3:// paths in the lake bucket."""
    con = duckdb.connect(database)
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET lake (
            TYPE s3,
            KEY_ID {sql_literal(settings.access_key_id)},
            SECRET {sql_literal(settings.secret_access_key)},
            REGION {sql_literal(settings.region)},
            ENDPOINT {sql_literal(settings.endpoint)},
            URL_STYLE {sql_literal(settings.url_style)},
            USE_SSL {'true' if settings.use_ssl else 'false'},
            SCOPE {sql_literal(f's3://{settings.bucket}')}
        )
        """
    )
    return con
