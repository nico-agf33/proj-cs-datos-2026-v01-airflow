from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from ..http_client import PoliteHttpClient
from ..normalize import as_number, parse_consumo, parse_motor

logger = logging.getLogger(__name__)

_GRAPHQL_URL = "https://carone.com.ar/api/graphql"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) "
        "Gecko/20100101 Firefox/140.0"
    ),
    "Content-Type": "application/json",
    "x-v6-country": "ar",
    "Origin": "https://carone.com.ar",
    "Referer": "https://carone.com.ar/comprar?carOptions=usados",
}

_BRANDS_QUERY = """
query CatalogFilters($filters: CatalogFiltersInput) {
  catalogFilters(filters: $filters) {
    brands { default { label } others { label } }
  }
}
"""

_PRODUCTS_QUERY = """
query GetProductsCard(
  $q: String!,
  $pageSize: Int!,
  $currentPage: Int!,
  $filter: ProductAttributeFilterInput
) {
  products(
    search: $q,
    pageSize: $pageSize,
    currentPage: $currentPage,
    filter: $filter
  ) {
    total_count
    items {
      sku name url_key carone_year carone_mileage carone_potency
      carone_cylinder_capacity carone_consumption
      carone_marca_data { label }
      carone_modelo_data { label }
      carone_transmission_data { label }
      carone_traction_data { label }
      carone_fuel_data { label }
      carone_dealer_id
      price_range {
        maximum_price { final_price { currency value } }
      }
    }
  }
}
"""


def build_client() -> PoliteHttpClient:
    return PoliteHttpClient(
        headers=_HEADERS,
        min_interval=0.5,
        max_attempts=3,
        base_backoff=2.0,
    )


def get_available_brands(
    *,
    client: PoliteHttpClient | None = None,
    raw_dir: str | Path | None = None,
) -> list[str]:
    http = client or build_client()
    payload = {
        "operationName": "CatalogFilters",
        "variables": {"filters": {}},
        "query": _BRANDS_QUERY,
    }
    response = http.post(_GRAPHQL_URL, json=payload)
    body = _graphql_data(response)
    _write_raw_json(raw_dir, "marcas.json", body)
    brands = body.get("data", {}).get("catalogFilters", {}).get("brands", {})
    all_brands = brands.get("default", []) + brands.get("others", [])
    return [brand["label"] for brand in all_brands if brand.get("label")]


def search(
    marca: str | None = None,
    modelo: str | None = None,
    *,
    max_pages: int = 100,
    client: PoliteHttpClient | None = None,
    raw_dir: str | Path | None = None,
) -> list[dict]:
    del modelo  # reservado para mantener el contrato común de collectors
    http = client or build_client()
    results: list[dict] = []
    page_size = 20
    filters = {
        "stock_status": {"eq": "IN_STOCK"},
        "carone_tags_arg": {"in": [2]},
    }
    if marca:
        filters["carone_marca_label"] = {"eq": marca}

    logger.info("[carone] Iniciando ingesta para %s", marca or "GLOBAL")

    for current_page in range(1, max_pages + 1):
        payload = {
            "operationName": "GetProductsCard",
            "variables": {
                "q": "",
                "pageSize": page_size,
                "currentPage": current_page,
                "sort": {"created_at": "DESC"},
                "filter": filters,
            },
            "query": _PRODUCTS_QUERY,
        }
        response = http.post(_GRAPHQL_URL, json=payload)
        body = _graphql_data(response)
        _write_raw_json(raw_dir, f"catalogo_{current_page:04d}.json", body)
        data = body.get("data", {}).get("products", {})
        items = data.get("items") or []
        total = int(data.get("total_count") or 0)
        if not items:
            break

        results.extend(_normalize_item(item) for item in items)
        logger.info(
            "[carone] Página %d procesada. Registros: %d/%d",
            current_page,
            len(results),
            total,
        )
        if len(results) >= total:
            break
    else:
        logger.warning("[carone] Se alcanzó max_pages=%d", max_pages)

    return results


def _graphql_data(response) -> dict:
    body = response.json()
    if body.get("errors"):
        raise ValueError(f"CarOne GraphQL devolvió errores: {body['errors']}")
    return body


def _normalize_item(item: dict) -> dict:
    price = (
        item.get("price_range", {})
        .get("maximum_price", {})
        .get("final_price", {})
    )
    return {
        "fuente": "carone",
        "id_publicacion": item.get("sku"),
        "marca": (item.get("carone_marca_data") or {}).get("label"),
        "modelo": (item.get("carone_modelo_data") or {}).get("label"),
        "version": item.get("name"),
        "fabricado_en": _as_int(item.get("carone_year")),
        "kilometraje": _as_int(item.get("carone_mileage")),
        "precio": as_number(price.get("value")),
        "moneda": price.get("currency", "ARS"),
        "motor_lt": parse_motor(item.get("carone_cylinder_capacity")),
        "potencia_hp": as_number(item.get("carone_potency")),
        "transmision": (item.get("carone_transmission_data") or {}).get("label"),
        "traccion": (item.get("carone_traction_data") or {}).get("label"),
        "combustible": (item.get("carone_fuel_data") or {}).get("label"),
        "consumo_lt_100km": parse_consumo(item.get("carone_consumption")),
        "ubicacion": item.get("carone_dealer_id"),
        "url": f"https://carone.com.ar/comprar/usados/{item.get('url_key')}",
        "fecha_ingesta": datetime.now(timezone.utc).isoformat(),
    }


def _as_int(value) -> int | None:
    number = as_number(value)
    return int(number) if number is not None else None


def _write_raw_json(
    raw_dir: str | Path | None,
    name: str,
    body: dict,
) -> None:
    if raw_dir is None:
        return
    directory = Path(raw_dir)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text(
        json.dumps(body, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
