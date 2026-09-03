from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup

from ..http_client import PoliteHttpClient
from ..normalize import (
    as_number,
    clean_price_and_currency,
    parse_consumo,
    parse_motor,
    remove_accents,
)

logger = logging.getLogger(__name__)

_BASE = "https://www.deruedas.com.ar"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-AR,es;q=0.9",
}


def build_client(delay: float = 2.0) -> PoliteHttpClient:
    return PoliteHttpClient(
        headers=_HEADERS,
        min_interval=delay,
        max_attempts=3,
        base_backoff=5.0,
        max_local_wait=300.0,
    )


def get_available_brands(
    *,
    client: PoliteHttpClient | None = None,
    raw_dir: str | Path | None = None,
) -> list[str]:
    """Descubrir marcas una sola vez; los errores HTTP se propagan a Airflow."""
    http = client or build_client()
    response = http.get(f"{_BASE}/bus.asp", params={"segmento": 0})
    _write_raw(raw_dir, "marcas.html", response.text)

    soup = BeautifulSoup(response.text, "html.parser")
    marcas = [
        item.get("value")
        for item in soup.select("#divModelosFancy input.fancyCheck")
        if item.get("value")
    ]
    if not marcas:
        marcas = [
            link.get("marcaVal")
            for link in soup.find_all("a", {"marcaVal": True})
            if link.get("marcaVal")
        ]
    return list(dict.fromkeys(marca.strip() for marca in marcas if marca))


def search(
    marca: str | None = None,
    modelo: str | None = None,
    *,
    delay: float = 2.0,
    limit: int | None = None,
    max_pages: int = 200,
    client: PoliteHttpClient | None = None,
    raw_dir: str | Path | None = None,
) -> list[dict]:
    """Recorrer deRuedas de forma secuencial, acotada y observable."""
    http = client or build_client(delay)
    results: list[dict] = []
    seen_links: set[str] = set()
    previous_page_links: tuple[str, ...] | None = None
    params = {"segmento": 0, "weNeed": "divBusqueda"}
    if marca:
        params["marca"] = marca
    if modelo:
        params["modelo"] = f"{marca}:{modelo}"

    logger.info("[deruedas] Iniciando cosecha para %s", marca or "GLOBAL")

    for page in range(1, max_pages + 1):
        response = http.get(f"{_BASE}/busCraw.asp", params={**params, "pag": page})
        _write_raw(raw_dir, f"listado_{page:04d}.html", response.text)
        soup = BeautifulSoup(response.text, "html.parser")

        links = tuple(
            dict.fromkeys(
                urljoin(_BASE, anchor["href"])
                for anchor in soup.find_all("a", href=True)
                if "vendo/" in anchor["href"]
            )
        )
        if not links:
            logger.info("[deruedas] Fin de catálogo en página %d", page)
            break
        if links == previous_page_links:
            logger.warning("[deruedas] Página %d repetida; se corta el crawl", page)
            break
        previous_page_links = links

        new_links = [link for link in links if link not in seen_links]
        if not new_links:
            logger.info("[deruedas] Página %d sin avisos nuevos", page)
            break

        logger.info("[deruedas] Página %d: %d avisos nuevos", page, len(new_links))
        for detail_url in new_links:
            seen_links.add(detail_url)
            item = _scrape_detail(
                detail_url,
                client=http,
                raw_dir=raw_dir,
            )
            if item:
                results.append(item)
            if limit is not None and len(results) >= limit:
                return results[:limit]
    else:
        logger.warning("[deruedas] Se alcanzó max_pages=%d", max_pages)

    return results


def _scrape_detail(
    url: str,
    *,
    client: PoliteHttpClient,
    raw_dir: str | Path | None = None,
) -> dict | None:
    response = client.get(url)
    response.encoding = "utf-8"
    listing_id = _listing_id(url)
    _write_raw(raw_dir, f"detalle_{_safe_name(listing_id)}.html", response.text)
    soup = BeautifulSoup(response.text, "html.parser")

    model_exact, make_exact = None, None
    for script in soup.find_all("script"):
        if script.string and "modelo:" in script.string:
            model_match = re.search(r"modelo:\s*'([^']+)'", script.string)
            make_match = re.search(r"marca:\s*'([^']+)'", script.string)
            if model_match:
                model_exact = model_match.group(1)
            if make_match:
                make_exact = make_match.group(1)
            break

    price_val, price_curr = None, None
    for cell in soup.find_all("td"):
        if "Precio:" in cell.get_text():
            price = cell.find("b")
            if price:
                price_val, price_curr = clean_price_and_currency(
                    price.get_text(strip=True)
                )
            break

    mapping = {
        "motor": "motor_lt",
        "potencia": "potencia_hp",
        "transmision": "transmision",
        "traccion": "traccion",
        "combustible": "combustible",
        "consumo prom.": "consumo_lt_100km",
    }
    specs: dict[str, str] = {}
    for box in soup.select(".box-destacado"):
        content = box.get_text(separator="|", strip=True).split("|")
        if len(content) < 2:
            continue
        label = remove_accents(content[0])
        value = box.find("b").get_text(strip=True) if box.find("b") else content[-1]
        if label in mapping:
            specs[mapping[label]] = value

    def get_meta(prop: str) -> str | None:
        tag = soup.find("meta", itemprop=prop)
        return tag.get("content") if tag else None

    marca = make_exact or get_meta("brand")
    modelo = model_exact or get_meta("model")
    if not listing_id or not marca or not modelo:
        logger.warning("[deruedas] Ficha incompleta omitida: %s", url)
        return None

    title = soup.select_one(".titulo.resaltar span")
    return {
        "fuente": "deruedas",
        "id_publicacion": listing_id,
        "marca": marca,
        "modelo": modelo,
        "version": title.get_text(strip=True) if title else None,
        "fabricado_en": _as_int(get_meta("modelDate")),
        "kilometraje": _as_int(get_meta("mileageFromOdometer")),
        "precio": price_val,
        "moneda": price_curr,
        "motor_lt": parse_motor(specs.get("motor_lt")),
        "potencia_hp": as_number(specs.get("potencia_hp")),
        "transmision": specs.get("transmision"),
        "traccion": specs.get("traccion"),
        "combustible": specs.get("combustible"),
        "consumo_lt_100km": parse_consumo(specs.get("consumo_lt_100km")),
        "ubicacion": get_meta("address"),
        "url": url,
        "fecha_ingesta": datetime.now(timezone.utc).isoformat(),
    }


def _listing_id(url: str) -> str:
    query = parse_qs(urlparse(url).query)
    if query.get("cod"):
        return query["cod"][0]
    return url.rstrip("/").split("/")[-1]


def _as_int(value) -> int | None:
    number = as_number(value)
    return int(number) if number is not None else None


def _safe_name(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", value)[:100] or "sin_id"


def _write_raw(raw_dir: str | Path | None, name: str, content: str) -> None:
    if raw_dir is None:
        return
    directory = Path(raw_dir)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text(content, encoding="utf-8")
