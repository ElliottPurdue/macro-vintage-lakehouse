"""Client for the FRED and ALFRED web API.

Pages through long responses, retries rate limiting and server errors with
backoff, spaces requests out, and keeps the API key out of error messages and
logs.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

import requests

BASE_URL = "https://api.stlouisfed.org/fred"

# The widest real-time window the API accepts, which returns every vintage.
ALL_VINTAGES = {"realtime_start": "1776-07-04", "realtime_end": "9999-12-31"}

OBSERVATIONS_PAGE_LIMIT = 100_000  # API maximum for series/observations
VINTAGE_DATES_PAGE_LIMIT = 10_000  # API maximum for series/vintagedates

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
MAX_BACKOFF_SECONDS = 60.0

log = logging.getLogger(__name__)


class FredError(RuntimeError):
    """A FRED request failed. The message never contains the API key."""


class FredClient:
    def __init__(
        self,
        api_key: str,
        *,
        session: requests.Session | None = None,
        min_interval: float = 0.5,
        max_attempts: int = 5,
        timeout: float = 60.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not api_key:
            raise ValueError("api_key is empty")
        self._api_key = api_key
        self._session = session if session is not None else requests.Session()
        self._min_interval = min_interval
        self._max_attempts = max_attempts
        self._timeout = timeout
        self._sleep = sleep
        self._clock = clock
        self._next_request_at = float("-inf")
        self.requests_made = 0

    def series(self, series_id: str) -> dict[str, Any]:
        """The series' current metadata: title, units, frequency, notes and so on."""
        seriess = self._get("series", series_id=series_id)["seriess"]
        if len(seriess) != 1:
            raise FredError(f"series ({series_id}): expected one series, got {len(seriess)}")
        return seriess[0]

    def latest_vintage_date(self, series_id: str) -> str:
        body = self._get("series/vintagedates", series_id=series_id, sort_order="desc", limit=1, **ALL_VINTAGES)
        if not body["vintage_dates"]:
            raise FredError(f"series/vintagedates ({series_id}): no vintage dates")
        return body["vintage_dates"][0]

    def vintage_dates(self, series_id: str) -> list[str]:
        """Every date on which the series' values were revised or new values released."""
        return self._paged(
            "series/vintagedates", "vintage_dates", VINTAGE_DATES_PAGE_LIMIT, series_id=series_id, **ALL_VINTAGES
        )

    def observations_all_vintages(self, series_id: str) -> list[dict[str, str]]:
        """Every value the series has published, each with the real-time period it was current."""
        return self._paged(
            "series/observations", "observations", OBSERVATIONS_PAGE_LIMIT, series_id=series_id, **ALL_VINTAGES
        )

    def _paged(self, endpoint: str, key: str, page_limit: int, **params: Any) -> list[Any]:
        where = f"{endpoint} ({params.get('series_id')})"
        items: list[Any] = []
        expected: int | None = None
        while expected is None or len(items) < expected:
            body = self._get(endpoint, limit=page_limit, offset=len(items), **params)
            count = int(body["count"])
            if expected is not None and count != expected:
                # The data changed between pages, so the offsets no longer line up.
                raise FredError(f"{where}: count changed from {expected} to {count} while paging")
            expected = count
            page = body[key]
            if not page and len(items) < expected:
                raise FredError(f"{where}: empty page at offset {len(items)} of {expected}")
            items.extend(page)
        if len(items) != expected:
            raise FredError(f"{where}: received {len(items)} items, expected {expected}")
        return items

    def _get(self, endpoint: str, **params: Any) -> dict[str, Any]:
        url = f"{BASE_URL}/{endpoint}"
        query = {**params, "api_key": self._api_key, "file_type": "json"}
        shown = ", ".join(f"{name}={value}" for name, value in params.items() if name in ("series_id", "offset"))
        where = f"{endpoint} ({shown})"
        for attempt in range(1, self._max_attempts + 1):
            self._throttle()
            self.requests_made += 1
            retry_after = None
            try:
                response = self._session.get(url, params=query, timeout=self._timeout)
            except requests.RequestException as exc:
                problem, retryable = f"{type(exc).__name__}: {exc}", True
            else:
                if response.status_code == 200:
                    try:
                        return response.json()
                    except ValueError:
                        problem, retryable = "response body was not JSON", True
                else:
                    problem = f"HTTP {response.status_code}: {_error_message(response)}"
                    retryable = response.status_code in RETRYABLE_STATUS
                    retry_after = _retry_after(response)
            # Request exceptions quote the full URL, key included.
            problem = problem.replace(self._api_key, "<FRED_API_KEY>")
            if not retryable or attempt == self._max_attempts:
                raise FredError(f"{where}: {problem}") from None
            delay = retry_after if retry_after is not None else min(2.0**attempt, MAX_BACKOFF_SECONDS)
            log.warning("%s: %s; retrying in %.1fs (attempt %d of %d)", where, problem, delay, attempt + 1, self._max_attempts)
            self._sleep(delay)
        raise AssertionError("unreachable")

    def _throttle(self) -> None:
        wait = self._next_request_at - self._clock()
        if wait > 0:
            self._sleep(wait)
        self._next_request_at = self._clock() + self._min_interval


def _error_message(response: requests.Response) -> str:
    try:
        message = response.json().get("error_message")
    except (ValueError, AttributeError):
        message = None
    return message or response.reason or "no error message"


def _retry_after(response: requests.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return min(max(float(value), 0.0), MAX_BACKOFF_SECONDS)
    except ValueError:
        return None
