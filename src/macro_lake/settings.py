"""Settings read from environment variables, or from a .env file in the working directory."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import find_dotenv, load_dotenv

URL_STYLES = ("path", "vhost")


@dataclass(frozen=True)
class LakeSettings:
    endpoint: str  # host:port, no scheme
    bucket: str
    access_key_id: str
    secret_access_key: str = field(repr=False)
    region: str = "us-east-1"
    use_ssl: bool = False
    url_style: str = "path"

    @property
    def endpoint_url(self) -> str:
        return f"{'https' if self.use_ssl else 'http'}://{self.endpoint}"

    def uri(self, path: str) -> str:
        """s3:// URI for a path inside the lake bucket."""
        return f"s3://{self.bucket}/{path.lstrip('/')}"


def _load_env() -> None:
    load_dotenv(find_dotenv(usecwd=True))


def _get(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, "").strip()
    if value:
        return value
    if default is None:
        raise RuntimeError(f"{name} is not set. Copy .env.example to .env and fill it in.")
    return default


def load_lake_settings() -> LakeSettings:
    _load_env()
    url_style = _get("LAKE_S3_URL_STYLE", "path").lower()
    if url_style not in URL_STYLES:
        raise RuntimeError(f"LAKE_S3_URL_STYLE must be one of {URL_STYLES}, not {url_style!r}")
    return LakeSettings(
        endpoint=_get("LAKE_S3_ENDPOINT"),
        bucket=_get("LAKE_S3_BUCKET"),
        access_key_id=_get("LAKE_S3_ACCESS_KEY_ID"),
        secret_access_key=_get("LAKE_S3_SECRET_ACCESS_KEY"),
        region=_get("LAKE_S3_REGION", "us-east-1"),
        use_ssl=_get("LAKE_S3_USE_SSL", "false").lower() in ("1", "true", "yes"),
        url_style=url_style,
    )


def fred_api_key() -> str | None:
    _load_env()
    return os.environ.get("FRED_API_KEY", "").strip() or None
