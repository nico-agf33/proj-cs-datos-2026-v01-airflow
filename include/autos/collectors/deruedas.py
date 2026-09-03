import requests
from bs4 import BeautifulSoup
import re
import time
import logging
from datetime import datetime
from ..normalize import as_number, remove_accents, clean_price_and_currency, parse_motor, parse_consumo

logger = logging.getLogger(__name__)

_BASE = "https://www.deruedas.com.ar"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def get_available_brands() -> list[str]:
    ## extraer todas las marcas disponibles desde el panel lateral del sitio
    url = f"{_BASE}/bus.asp?segmento=0"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20)
        soup = BeautifulSoup(resp.text, 'html.parser')
        ### buscar en el div de filtros dinamicos analizado anteriormente
        marcas = [i.get("value") for i in soup.select("#divModelosFancy input.fancyCheck") if i.get("value")]
        if not marcas:
            ### fallback a los enlaces de texto si el div no esta presente
            enlaces = soup.find_all("a", {"marcaVal": True})
            marcas = [a.get("marcaVal") for a in enlaces if a.get("marcaVal")]
        
        return list(dict.fromkeys([m.strip() for m in marcas if m]))
    except Exception as e:
        logger.error(f"[deruedas] Error descubriendo marcas: {e}")
        return []

def search(marca: str = None, modelo: str = None,delay: float = 1.0) -> list[dict]:

    ### buscar vehiculos y recorrer todas las paginas disponibles,
    ### detener automaticamente cuando no encuentra mas avisos

    results = []
    page = 1
    params = "segmento=0"
    if marca: 
        params += f"&marca={marca.replace(' ', '%20')}"
    if modelo: 
        ### si se especifica modelo, deRuedas usa el formato marca:modelo
        params += f"&modelo={marca}:{modelo}".replace(" ", "%20")

    logger.info(f"[deruedas] Iniciando cosecha para {marca or 'GLOBAL'}...")

    while True:
        ### usar busCraw.asp (mas liviano para el scraping masivo)
        url = f"{_BASE}/busCraw.asp?{params}&weNeed=divBusqueda&pag={page}"
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            ### extraer links de los avisos en la pagina actual
            links = []
            for a in soup.find_all('a', href=True):
                if 'vendo/' in a['href']:
                    full_url = a['href'] if a['href'].startswith("http") else _BASE + a['href']
                    links.append(full_url)
            
            unique_links = list(dict.fromkeys(links))
            
            ### condicion de finalizacion: si la pagina no tiene links, se llega al final del catalogo
            if not unique_links:
                logger.info(f"[deruedas] No se encontraron más avisos en la página {page}. Fin de la fuente.")
                break

            logger.info(f"[deruedas] Procesando página {page} ({len(unique_links)} avisos)...")

            for url_ficha in unique_links:
                time.sleep(delay) ### rate limit
                item = _scrape_detail(url_ficha)
                if item:
                    results.append(item)
            
            page += 1
            
        except Exception as e:
            logger.error(f"[deruedas] Error en página {page}: {e}")
            break ### si hay un error de red persistente, devolver lo que se haya obtenido
            
    return results

def _scrape_detail(url: str) -> dict | None:
    ### extraer la ficha tecnica completa y normalizar a nombres en español
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=15)
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, 'html.parser')

        ### marca y modelo exactos desde el bloque JS
        model_exact, make_exact = None, None
        scripts = soup.find_all("script")
        for script in scripts:
            if script.string and "modelo:" in script.string:
                model_match = re.search(r"modelo:\s*'([^']+)'", script.string)
                make_match = re.search(r"marca:\s*'([^']+)'", script.string)
                if model_match: model_exact = model_match.group(1)
                if make_match: make_exact = make_match.group(1)
                break 

        ### precio y moneda
        price_val, price_curr = 0.0, "ARS"
        for td in soup.find_all("td"):
            if "Precio:" in td.get_text():
                b_tag = td.find("b")
                if b_tag:
                    price_val, price_curr = clean_price_and_currency(b_tag.get_text(strip=True))
                break

        ### mapeo de atributos tecnicos (box-destacado)
        mapping = {
            "motor": "motor_lt", 
            "potencia": "potencia_hp", 
            "transmision": "transmision", 
            "traccion": "traccion", 
            "combustible": "combustible", 
            "consumo prom.": "consumo_lt_100km"
        }
        specs = {}
        for box in soup.select(".box-destacado"):
            content = box.get_text(separator="|", strip=True).split("|")
            if len(content) >= 2:
                label = remove_accents(content[0])
                ### el valor se encuentra en el tag <b> o es el ultimo elemento de la lista
                val = box.find("b").get_text(strip=True) if box.find("b") else content[-1]
                if label in mapping:
                    specs[mapping[label]] = val

        ### datos basicos desde meta tags (Schema.org)
        def get_meta(prop):
            tag = soup.find("meta", itemprop=prop)
            return tag["content"] if tag else None

        return {
            "fuente": "deruedas",
            "id_publicacion": url.split("cod=")[-1],
            "marca": make_exact or get_meta("brand"),
            "modelo": model_exact or get_meta("model"),
            "version": soup.select_one(".titulo.resaltar span").get_text(strip=True) if soup.select_one(".titulo.resaltar span") else "",
            "fabricado_en": int(as_number(get_meta("modelDate") or 0)),
            "kilometraje": int(as_number(get_meta("mileageFromOdometer") or 0)),
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
            "fecha_ingesta": datetime.now().isoformat()
        }
    except Exception:
        return None