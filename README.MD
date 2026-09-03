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
| `deruedas_target_records` | `1001` | Tope global de registros deRuedas por cosecha |
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

## Evidencia de entrega — 2026-09-03

Corrida completa exitosa:

- DAG run: `manual__2026-09-03T16:04:10.521198+00:00`.
- Configuración: `{"forzar_descarga": true}`.
- Duración: 2.401,14 segundos (40 min 01,14 s).
- Dataset: 1.192 filas únicas y 20 columnas.
- Fuentes: 1.001 filas de deRuedas y 191 de CarOne.
- Calidad: los siete controles pasaron; hay columnas numéricas y de texto,
  ninguna columna está completamente vacía y `precio` está disponible en
  1.189 filas.
- Nulos medidos: `consumo_lt_100km` 4,03 %, `motor_lt` 2,68 %,
  `potencia_hp`, `combustible`, `traccion` y `transmision` 1,43 %,
  `version` 0,42 % y `precio` 0,25 %.
- HTTP 429: no se observó ninguno en esta corrida. Se mantuvo el intervalo de
  2 segundos y la lógica de `Retry-After`.

Artefactos observados:

- Bronze: `include/output/bronze/` (129 MiB).
- Silver: `include/output/silver/final_2026-09-03.csv` (316 KiB).
- Calidad: `include/output/reporte_calidad.json`.
- Frozen: `include/frozen/autos_semilla.csv` (316 KiB).
- Entregable: `include/output/tp1_5K09_03.zip` (68 KiB).

La corrida no forzada `manual__2026-09-03T16:57:12.136079+00:00` terminó
correctamente en 8,29 segundos: seleccionó el frozen y dejó salteadas las tres
tareas que consultan CarOne/deRuedas.

### Pasos exactos para la presentación

1. Ejecutar `astro dev start` y abrir la URL de Airflow que imprime la CLI.
2. Abrir `tp1_5K09_03_autos_mensual` y mostrar en Runs la corrida completa
   verde indicada arriba.
3. Mostrar el grafo y explicar la bifurcación entre cosecha y frozen.
4. Abrir `include/output/silver/final_2026-09-03.csv` y señalar
   `clave_publicacion`, `fuente`, los tipos mixtos y el objetivo `precio`.
5. Abrir `include/output/reporte_calidad.json` y mostrar filas, nulos y los
   siete checks en `true`.
6. Mostrar las carpetas bronze, silver, frozen y el ZIP.
7. No lanzar una cosecha forzada durante la exposición; si se demuestra la
   operación, usar una corrida normal para seleccionar el frozen.
