import requests
import json
import logging
from datetime import datetime
from ..normalize import as_number, parse_motor, parse_consumo

logger = logging.getLogger(__name__)

_GRAPHQL_URL = "https://carone.com.ar/api/graphql"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
    "Content-Type": "application/json",
    "x-v6-country": "ar",
    "Origin": "https://carone.com.ar",
    "Referer": "https://carone.com.ar/comprar?carOptions=usados"
}

def get_available_brands() -> list[str]:
### extraer marcas desde catalogFilters via API GraphQL
    payload = {
        "operationName": "CatalogFilters",
        "variables": {"filters": {}},
        "query": """
            query CatalogFilters($filters: CatalogFiltersInput) {
              catalogFilters(filters: $filters) {
                brands {
                  default { label }
                  others { label }
                }
              }
            }
        """
    }
    try:
        resp = requests.post(_GRAPHQL_URL, json=payload, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
        data = resp.json().get("data", {}).get("catalogFilters", {}).get("brands", {})
        all_brands = data.get("default", []) + data.get("others", [])
        return [b["label"] for b in all_brands if b.get("label")]
    except Exception as e:
        logger.error(f"[carone] Error descubriendo marcas: {e}")
        return []

def search(marca: str = None, modelo: str = None) -> list[dict]:

    results = []
    current_page = 1
    page_size = 20 
    
    ### filtro base -> solo autos en stock y de categoría usados (ID 2)
    filters = {
        "stock_status": {"eq": "IN_STOCK"},
        "carone_tags_arg": {"in": [2]}
    }
    if marca:
        filters["carone_marca_label"] = {"eq": marca}

    logger.info(f"[carone] Iniciando ingesta para marca {marca or 'GLOBAL'}...")

    while True:
        payload = {
            "operationName": "GetProductsCard",
            "variables": {
                "q": "",
                "pageSize": page_size,
                "currentPage": current_page,
                "sort": {"created_at": "DESC"},
                "filter": filters
            },
            "query": """
            query GetProductsCard($q: String!, $pageSize: Int!, $currentPage: Int!, $filter: ProductAttributeFilterInput) {
              products(search: $q, pageSize: $pageSize, currentPage: $currentPage, filter: $filter) {
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
                    maximum_price {
                      final_price { currency value }
                    }
                  }
                }
              }
            }
            """
        }

        try:
            resp = requests.post(_GRAPHQL_URL, json=payload, headers=_HEADERS, timeout=20)
            resp.raise_for_status()
            data = resp.json().get("data", {}).get("products", {})
            
            items = data.get("items", [])
            total_disponible = data.get("total_count", 0)

            if not items:
                break

            for item in items:
                price_info = item.get("price_range", {}).get("maximum_price", {}).get("final_price", {})
                
                results.append({
                    "fuente": "carone",
                    "id_publicacion": item.get("sku"),
                    "marca": (item.get("carone_marca_data") or {}).get("label"),
                    "modelo": (item.get("carone_modelo_data") or {}).get("label"),
                    "version": item.get("name"),
                    "fabricado_en": int(as_number(item.get("carone_year"))),
                    "kilometraje": int(as_number(item.get("carone_mileage"))),
                    "precio": as_number(price_info.get("value")),
                    "moneda": price_info.get("currency", "ARS"),
                    "motor_lt": parse_motor(item.get("carone_cylinder_capacity")),
                    "potencia_hp": as_number(item.get("carone_potency")),
                    "transmision": (item.get("carone_transmission_data") or {}).get("label"),
                    "traccion": (item.get("carone_traction_data") or {}).get("label"),
                    "combustible": (item.get("carone_fuel_data") or {}).get("label"),
                    "consumo_lt_100km": parse_consumo(item.get("carone_consumption")),
                    "ubicacion": item.get("carone_dealer_id"),
                    "url": f"https://carone.com.ar/comprar/usados/{item.get('url_key')}",
                    "fecha_ingesta": datetime.now().isoformat()
                })

            logger.info(f"[carone] Página {current_page} procesada. Registros: {len(results)}/{total_disponible}")

            ### condicion de break
            if len(results) >= total_disponible:
                break

            current_page += 1
            
        except Exception as e:
            logger.error(f"[carone] Error en página {current_page}: {e}")
            break
            
    return results