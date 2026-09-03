# Pipeline de autos usados — Grupo 5K09-03

Pipeline Astro/Airflow para producir un dataset de publicaciones de autos usados
desde dos fuentes públicas:

- **deRuedas:** scraping HTML.
- **CarOne:** API GraphQL.

Una fila representa una publicación. La clave única es
`clave_publicacion = fuente:id_publicacion` y la variable objetivo es `precio`.

## Flujo del DAG

`tp1_5K09_03_autos_mensual` se evalúa diariamente, pero consulta las fuentes
solo si no existe una cosecha previa, pasaron 30 días o se activa el parámetro
`forzar_descarga`.

1. Crea los directorios de trabajo.
2. Decide entre cosecha nueva y respaldo frozen **antes** de consultar portales.
3. Descubre las marcas de deRuedas una sola vez.
4. Cosecha CarOne y procesa las marcas de deRuedas secuencialmente.
5. Consolida y deduplica la capa plata.
6. Verifica los siete controles de calidad.
7. Genera `include/output/tp1_5K09_03.zip`.

## Protección contra HTTP 429

- deRuedas ejecuta una sola tarea de cosecha por DAG.
- Todas sus solicitudes están separadas por `deruedas_delay_seconds`.
- Cada crawl tiene un máximo configurable de páginas.
- Un `429` respeta `Retry-After`; si la espera es excesiva o el límite persiste,
  la tarea falla de forma explícita y Airflow la reintenta 15 minutos después.
- Nunca se interpreta el HTML de una respuesta `429` como si fuera una ficha.
- No hay sensor que consulte el portal cada minuto.

La pausa inicial es de 2 segundos. Debe ajustarse con evidencia de los headers y
logs del sitio; no se recomienda reducirla para acelerar una presentación.

## Capas y salidas

- `include/output/bronze/carone/raw/`: respuestas GraphQL originales.
- `include/output/bronze/deruedas/raw/`: HTML original de listados y fichas.
- `include/output/bronze/*/normalized`: registros normalizados por fuente.
- `include/output/silver/final_YYYY-MM-DD.csv`: dataset tidy.
- `include/output/reporte_calidad.json`: métricas de la corrida.
- `include/frozen/autos_semilla.csv`: copia automática de la última cosecha validada.

Las salidas generadas están ignoradas por Git.

## Ejecución

Requisitos:

- Docker Desktop.
- Astro CLI.

```bash
astro dev start
```

La interfaz queda disponible en <http://localhost:8080/>.

Para una cosecha nueva, ejecutar el DAG con `forzar_descarga=true`. La corrida
completa hace miles de solicitudes y debe realizarse una sola vez, con tiempo,
antes de la exposición. Para detener el entorno:

```bash
astro dev stop
```

## Variables Airflow

| Variable | Default | Uso |
|---|---:|---|
| `grupo` | `5K09-03` | Identificación del equipo |
| `meta_volumen` | `1001` | Mínimo de filas exigido |
| `deruedas_delay_seconds` | `2.0` | Intervalo entre requests de deRuedas |
| `deruedas_max_pages` | `200` | Tope de páginas por marca |
| `carone_max_pages` | `100` | Tope de páginas GraphQL |

## Controles de calidad

La corrida falla si no cumple:

1. `clave_publicacion` única.
2. Al menos `meta_volumen` filas (por defecto, más de 1.000).
3. Cinco o más columnas útiles.
4. Columnas numéricas y categóricas.
5. Tasas de nulos calculadas.
6. Ninguna columna completamente vacía.
7. Al menos un valor disponible en la variable objetivo `precio`.

## Checklist para la entrega

- Levantar Airflow y abrir la UI antes de ingresar.
- Ejecutar previamente una corrida completa y conservarla en verde.
- Mostrar el grafo, el CSV y `reporte_calidad.json`.
- Explicar la clave, los nulos, el objetivo `precio` y las capas bronze/silver.
- No ejecutar la cosecha completa durante los 20 minutos de exposición.
- Todo integrante debe poder explicar cualquier tarea del DAG.

## Pruebas sin tráfico externo

```bash
python -m pytest
```

Las pruebas usan respuestas simuladas; no consultan deRuedas ni CarOne.
