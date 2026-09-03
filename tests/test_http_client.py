from __future__ import annotations

from unittest.mock import Mock

import pytest
import requests

from autos.http_client import PoliteHttpClient, RateLimitError


def _response(status: int, *, retry_after: str | None = None) -> Mock:
    response = Mock(spec=requests.Response)
    response.status_code = status
    response.headers = {"Retry-After": retry_after} if retry_after else {}
    response.raise_for_status.side_effect = (
        requests.HTTPError(response=response) if status >= 400 else None
    )
    return response


def test_429_honors_retry_after_then_succeeds():
    session = Mock(spec=requests.Session)
    session.headers = {}
    session.request.side_effect = [
        _response(429, retry_after="7"),
        _response(200),
    ]
    sleeps: list[float] = []
    client = PoliteHttpClient(
        headers={},
        session=session,
        max_attempts=2,
        sleep=sleeps.append,
        monotonic=Mock(side_effect=[0.0, 7.0, 7.0]),
    )

    response = client.get("https://example.test")

    assert response.status_code == 200
    assert sleeps == [7.0]


def test_persistent_429_is_exposed_to_airflow():
    session = Mock(spec=requests.Session)
    session.headers = {}
    session.request.side_effect = [_response(429), _response(429)]
    client = PoliteHttpClient(
        headers={},
        session=session,
        max_attempts=2,
        base_backoff=0,
        sleep=Mock(),
        jitter=Mock(return_value=0),
    )

    with pytest.raises(RateLimitError):
        client.get("https://example.test")


def test_non_retryable_error_is_not_parsed():
    session = Mock(spec=requests.Session)
    session.headers = {}
    response = _response(404)
    session.request.return_value = response
    client = PoliteHttpClient(headers={}, session=session)

    with pytest.raises(requests.HTTPError):
        client.get("https://example.test/missing")
