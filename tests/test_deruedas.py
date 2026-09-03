from __future__ import annotations

from unittest.mock import Mock

from autos.collectors import deruedas


def _response(html: str) -> Mock:
    response = Mock()
    response.text = html
    response.encoding = None
    return response


def test_search_limit_stops_before_requesting_more_details(tmp_path):
    listing = """
    <a href="/vendo/auto.asp?cod=1">uno</a>
    <a href="/vendo/auto.asp?cod=2">dos</a>
    """
    detail = """
    <script>const auto = {modelo: 'Uno', marca: 'Fiat'};</script>
    <meta itemprop="brand" content="Fiat">
    <meta itemprop="model" content="Uno">
    """
    client = Mock()
    client.get.side_effect = [_response(listing), _response(detail)]

    records = deruedas.search(
        marca="Fiat",
        client=client,
        limit=1,
        max_pages=200,
        raw_dir=tmp_path,
    )

    assert len(records) == 1
    assert records[0]["id_publicacion"] == "1"
    assert client.get.call_count == 2
