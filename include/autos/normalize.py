import re
import unicodedata

def remove_accents(text: str) -> str:

    	### eliminar tildes y normalizar a minusculas 
    	### ej: ´´Tracción´´ -> ´´traccion´´

    if not text:
        return ""
    text = str(text).lower().strip()
    return "".join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )

def as_number(val) -> float:

    	### extraer el 1er nro de un string y devolverlo como float
    	### manejando formatos locales (punto para unidades de mil, coma para decimales)
    
    	### ej:
    	### '1.6 lts' -> 1.6
    	### '157.000 km' -> 157000.0
    	### '9,3 lts / 100km' -> 9.3
    	### '$ 31.500.000' -> 31500000.0

    if val is None or val == "":
        return None
    if isinstance(val, (int, float)):
        return float(val)
    
    	### eliminar puntos de unidad de mil 
    s = str(val).replace('.', '').strip()
    	### cambiar coma decimal por punto
    s = s.replace(',', '.')
    
    	### buscar el 1er grupo numerico (entero o con decimales)
    match = re.search(r'(\d+(\.\d+)?)', s)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None

def clean_price_and_currency(text: str) -> tuple[float, str]:

    	### detectar u$d o ar$ y devolver la dupla (valor_float , moneda_str)

    if not text:
        return 0.0, "ARS"
    
    t = text.upper()
    currency = "USD" if ("U$" in t or "USD" in t) else "ARS"
    return as_number(t), currency

def _slug(text: str) -> str:

    	### generar nombres de archivo limpios
    	### ej: ´´Peugeot 208´´ -> ´´peugeot-208´´

    if not text:
        return "desconocido"
    return remove_accents(text).replace(" ", "-")

	### ---> funciones especificas para ´´capa plata´´ (floats tecnicos) 

def parse_motor(val) -> float:

    	### conversion ( '1600' o '1.6 lts' a float 1.6 )

    num = as_number(val)
    if num is None: return None
    	### si el valor es > 100, se asume que son cc y se convierte a lt
    if num > 100:
        return round(num / 1000, 1)
    return num

def parse_consumo(val) -> float:

    	### extraer solo el valor numerico del consumo
    	### ej: '12 lts / 100km' -> 12.0

    	### usar as_number para devolver el float
    return as_number(val)