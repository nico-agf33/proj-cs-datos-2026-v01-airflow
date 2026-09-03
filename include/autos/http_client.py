from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Callable

import requests

logger = logging.getLogger(__name__)


class RateLimitError(requests.HTTPError):
    """La fuente mantuvo el rate limit luego de los reintentos locales."""


def _retry_after_seconds(value: str | None, now: datetime | None = None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            current = now or datetime.now(timezone.utc)
            return max(0.0, (retry_at - current).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


class PoliteHttpClient:
    """Cliente HTTP con rate limit local y backoff acotado."""

    def __init__(
        self,
        *,
        headers: dict[str, str],
        min_interval: float = 0.0,
        max_attempts: int = 3,
        base_backoff: float = 2.0,
        max_local_wait: float = 300.0,
        timeout: float = 20.0,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        jitter: Callable[[float, float], float] = random.uniform,
    ):
        self.session = session or requests.Session()
        self.session.headers.update(headers)
        self.min_interval = max(0.0, min_interval)
        self.max_attempts = max(1, max_attempts)
        self.base_backoff = max(0.0, base_backoff)
        self.max_local_wait = max(0.0, max_local_wait)
        self.timeout = timeout
        self._sleep = sleep
        self._monotonic = monotonic
        self._jitter = jitter
        self._last_request_at: float | None = None

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        timeout = kwargs.pop("timeout", self.timeout)

        for attempt in range(1, self.max_attempts + 1):
            self._throttle()
            response = self.session.request(method, url, timeout=timeout, **kwargs)
            self._last_request_at = self._monotonic()

            if response.status_code == 429:
                retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
                if attempt == self.max_attempts:
                    raise RateLimitError(
                        f"HTTP 429 persistente en {url}; Airflow debe reintentar más tarde",
                        response=response,
                    )
                wait = retry_after
                if wait is None:
                    wait = self.base_backoff * (2 ** (attempt - 1))
                    wait += self._jitter(0.0, max(0.25, wait * 0.25))
                if wait > self.max_local_wait:
                    raise RateLimitError(
                        f"HTTP 429 en {url}; Retry-After={wait:.0f}s excede la espera local",
                        response=response,
                    )
                logger.warning(
                    "HTTP 429 en %s; reintento local %d/%d en %.1fs",
                    url,
                    attempt + 1,
                    self.max_attempts,
                    wait,
                )
                self._sleep(wait)
                continue

            if response.status_code >= 500 and attempt < self.max_attempts:
                wait = min(
                    self.max_local_wait,
                    self.base_backoff * (2 ** (attempt - 1))
                    + self._jitter(0.0, self.base_backoff),
                )
                logger.warning(
                    "HTTP %d en %s; reintento local %d/%d en %.1fs",
                    response.status_code,
                    url,
                    attempt + 1,
                    self.max_attempts,
                    wait,
                )
                self._sleep(wait)
                continue

            response.raise_for_status()
            return response

        raise RuntimeError("bucle de reintentos HTTP agotado")

    def get(self, url: str, **kwargs) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> requests.Response:
        return self.request("POST", url, **kwargs)

    def _throttle(self) -> None:
        if self._last_request_at is None or self.min_interval <= 0:
            return
        elapsed = self._monotonic() - self._last_request_at
        remaining = self.min_interval - elapsed
        if remaining > 0:
            self._sleep(remaining)
