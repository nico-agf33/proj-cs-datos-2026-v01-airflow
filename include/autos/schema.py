CLAVE = "clave_publicacion"
OBJETIVO = "precio"

COLUMNAS = [
    CLAVE,
    "fuente",
    "id_publicacion",
    "marca",
    "modelo",
    "version",
    "fabricado_en",
    "kilometraje",
    OBJETIVO,
    "moneda",
    "motor_lt",
    "potencia_hp",
    "transmision",
    "traccion",
    "combustible",
    "consumo_lt_100km",
    "ubicacion",
    "url",
    "fecha_ingesta",
    "grupo",
]

NUMERICAS = [
    "fabricado_en",
    "kilometraje",
    OBJETIVO,
    "motor_lt",
    "potencia_hp",
    "consumo_lt_100km",
]

REQUERIDAS = [
    CLAVE,
    "fuente",
    "id_publicacion",
    "marca",
    "modelo",
    OBJETIVO,
    "url",
    "fecha_ingesta",
]