"""Synchronous client for the NASA FIRMS Area API."""

from __future__ import annotations

import email.utils
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import quote, urlsplit, urlunsplit

import requests


class FirmsClientError(RuntimeError):
    """Report a sanitized acquisition failure without exposing authentication material."""


@dataclass(frozen=True)
class FirmsAreaResponse:
    """Immutable HTTP response metadata and CSV payload returned by an Area API request."""

    content: bytes
    http_status: int
    content_type: str
    requested_at: str
    received_at: str
    sanitized_endpoint: str


class FirmsAreaClient:
    """Acquire bounded FIRMS CSV windows with retry and credential-safe diagnostics."""

    RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

    def __init__(
        self,
        *,
        map_key: str,
        base_url: str,
        connect_timeout_seconds: float = 10.0,
        read_timeout_seconds: float = 60.0,
        max_attempts: int = 4,
        session: requests.Session | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if not map_key.strip():
            raise FirmsClientError("FIRMS_MAP_KEY is required for Area API acquisition.")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one.")
        self._map_key = map_key.strip()
        self._base_url = base_url.rstrip("/")
        self._timeout = (connect_timeout_seconds, read_timeout_seconds)
        self._max_attempts = max_attempts
        self._session = session or requests.Session()
        self._sleeper = sleeper

    def fetch_csv(
        self,
        *,
        source_product: str,
        bbox: str,
        start_date: str,
        day_range: int,
    ) -> FirmsAreaResponse:
        """Fetch one product/window and return a validated CSV response.

        Retry covers transport failures, confirmed quota exhaustion and transient errors.
        Authentication and contract errors fail immediately. Error messages and returned
        metadata never contain the MAP_KEY.
        """
        if not 1 <= day_range <= 5:
            raise ValueError("FIRMS Area API day_range must be between one and five.")
        endpoint = self._endpoint(source_product, bbox, day_range, start_date, include_key=True)
        sanitized = self._endpoint(source_product, bbox, day_range, start_date, include_key=False)
        for attempt in range(1, self._max_attempts + 1):
            requested_at = datetime.now(timezone.utc).isoformat()
            try:
                response = self._session.get(endpoint, timeout=self._timeout)
            except requests.RequestException:
                if attempt == self._max_attempts:
                    break
                self._sleeper(min(2 ** (attempt - 1), 30))
                continue
            received_at = datetime.now(timezone.utc).isoformat()
            if (
                response.status_code in {400, 429}
                and not response.headers.get("Retry-After")
                and attempt < self._max_attempts
            ):
                # FIRMS may return HTTP 400 when the shared MAP_KEY quota is exhausted.
                # Confirm quota exhaustion; ordinary contract errors still fail immediately.
                if self._quota_exhausted():
                    logging.getLogger(__name__).warning(
                        "FIRMS shared quota exhausted; waiting 610 seconds before retry %s/%s.",
                        attempt + 1, self._max_attempts,
                    )
                    self._sleeper(610.0)
                    continue
            if response.status_code in self.RETRYABLE_STATUS:
                if attempt == self._max_attempts:
                    raise FirmsClientError(
                        f"FIRMS Area API remained unavailable with HTTP {response.status_code}."
                    )
                self._sleeper(self._retry_delay(response.headers.get("Retry-After"), attempt))
                continue
            if response.status_code >= 400:
                raise FirmsClientError(
                    f"FIRMS Area API rejected the request with HTTP {response.status_code}."
                )
            content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
            content = bytes(response.content)
            self._validate_csv(content, content_type)
            return FirmsAreaResponse(
                content=content,
                http_status=response.status_code,
                content_type=content_type,
                requested_at=requested_at,
                received_at=received_at,
                sanitized_endpoint=sanitized,
            )
        raise FirmsClientError("FIRMS Area API transport failed after bounded retries.") from None

    def _quota_exhausted(self) -> bool:
        """Check the official quota endpoint without exposing its authenticated URL."""
        base = urlsplit(self._base_url)
        endpoint = urlunsplit((
            base.scheme, base.netloc, "/mapserver/mapkey_status/",
            "MAP_KEY=" + quote(self._map_key, safe=""), "",
        ))
        try:
            response = self._session.get(endpoint, timeout=self._timeout)
            if response.status_code != 200:
                return False
            quota = json.loads(response.content)
            limit = int(quota["transaction_limit"])
            current = int(quota["current_transactions"])
            return limit > 0 and current >= limit
        except (requests.RequestException, ValueError, TypeError, KeyError):
            return False

    def _endpoint(
        self,
        source_product: str,
        bbox: str,
        day_range: int,
        start_date: str,
        *,
        include_key: bool,
    ) -> str:
        key_segment = quote(self._map_key, safe="") if include_key else "{MAP_KEY}"
        segments = (
            key_segment,
            quote(source_product, safe="_"),
            quote(bbox, safe=",.-"),
            str(day_range),
            quote(start_date, safe="-"),
        )
        return f"{self._base_url}/{'/'.join(segments)}"

    @staticmethod
    def _validate_csv(content: bytes, content_type: str) -> None:
        if content_type and not (
            content_type.startswith("text/")
            or content_type in {"application/csv", "application/octet-stream"}
        ):
            raise FirmsClientError(f"FIRMS returned unsupported content type {content_type!r}.")
        try:
            header = content.decode("utf-8-sig").splitlines()[0].casefold()
        except (UnicodeDecodeError, IndexError) as exc:
            raise FirmsClientError("FIRMS returned an empty or non-UTF-8 payload.") from exc
        required = {"latitude", "longitude", "acq_date", "acq_time", "confidence"}
        columns = {column.strip() for column in header.split(",")}
        missing = sorted(required - columns)
        if missing:
            raise FirmsClientError(
                "FIRMS CSV header is missing required columns: " + ", ".join(missing)
            )

    @staticmethod
    def _retry_delay(retry_after: str | None, attempt: int) -> float:
        if retry_after:
            try:
                return max(0.0, min(float(retry_after), 60.0))
            except ValueError:
                try:
                    retry_at = email.utils.parsedate_to_datetime(retry_after)
                    return max(
                        0.0,
                        min((retry_at - datetime.now(timezone.utc)).total_seconds(), 60.0),
                    )
                except (TypeError, ValueError):
                    pass
        return float(min(2 ** (attempt - 1), 30))
