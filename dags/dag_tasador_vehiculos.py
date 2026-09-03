"""Pipeline de Entrega 1 para avisos de autos usados.

Grupo: 5K09-03
Una fila representa una publicación. La variable objetivo es ``precio``.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import zipfile
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Param, Variable, dag, task
from airflow.task.trigger_rule import TriggerRule

from autos.collectors import carone, deruedas
from autos.normalize import _slug
from autos.quality import consolidate_json_files, validate_dataset

log = logging.getLogger(__name__)

DIR_SALIDA = Path("/usr/local/airflow/include/output")
DIR_BRONCE = DIR_SALIDA / "bronze"
DIR_PLATA = DIR_SALIDA / "silver"
DIR_FROZEN = Path("/usr/local/airflow/include/frozen")
VAR_ULTIMA_COSECHA = "autos_fecha_ultima_ingesta"


@dag(
    dag_id="tp1_5K09_03_autos_mensual",
    schedule="@daily",
    start_date=pendulum.datetime(
        2026,
        8,
        1,
        tz="America/Argentina/Buenos_Aires",
    ),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["proyecto-integrador", "vehiculos"],
    params={
        "forzar_descarga": Param(
            False,
            type="boolean",
            title="Forzar recolección ahora",
        ),
    },
)
def pipeline_vehiculos():
    @task
    def crear_carpetas_trabajo() -> bool:
        for directory in [
            DIR_SALIDA,
            DIR_BRONCE / "carone" / "raw",
            DIR_BRONCE / "deruedas" / "raw",
            DIR_BRONCE / "deruedas" / "normalized",
            DIR_PLATA,
            DIR_FROZEN,
        ]:
            directory.mkdir(parents=True, exist_ok=True)
        return True

    @task.branch
    def elegir_ruta_datos(**context) -> str | list[str]:
        if context["params"]["forzar_descarga"]:
            return ["descubrir_marcas_deruedas", "cosecha_bronce_carone"]

        last_harvest = Variable.get(VAR_ULTIMA_COSECHA, default=None)
        if not last_harvest:
            return ["descubrir_marcas_deruedas", "cosecha_bronce_carone"]

        elapsed_days = (pendulum.now("UTC") - pendulum.parse(last_harvest)).days
        if elapsed_days >= 30:
            return ["descubrir_marcas_deruedas", "cosecha_bronce_carone"]
        return "usar_respaldo_congelado"

    @task(retries=2, retry_delay=timedelta(minutes=15))
    def descubrir_marcas_deruedas() -> list[str]:
        client = deruedas.build_client(
            float(Variable.get("deruedas_delay_seconds", default=2.0))
        )
        brands = deruedas.get_available_brands(
            client=client,
            raw_dir=DIR_BRONCE / "deruedas" / "raw",
        )
        if not brands:
            raise ValueError("deRuedas no publicó marcas para procesar")
        log.info("deRuedas: %d marcas descubiertas una sola vez", len(brands))
        return brands

    @task(retries=2, retry_delay=timedelta(minutes=10))
    def cosecha_bronce_carone(**context) -> str:
        raw_dir = DIR_BRONCE / "carone" / "raw"
        if context["ti"].try_number == 1:
            shutil.rmtree(raw_dir, ignore_errors=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
        records = carone.search(
            raw_dir=raw_dir,
            max_pages=int(Variable.get("carone_max_pages", default=100)),
        )
        if not records:
            raise ValueError("CarOne no devolvió publicaciones")
        output = DIR_BRONCE / "carone" / "normalized.json"
        output.write_text(
            json.dumps(records, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return str(output)

    @task(
        retries=2,
        retry_delay=timedelta(minutes=15),
        max_active_tis_per_dag=1,
    )
    def cosecha_bronce_deruedas(
        brands: list[str],
        **context,
    ) -> list[str]:
        """Procesar marcas en serie, con límite global y checkpoints."""
        delay = float(Variable.get("deruedas_delay_seconds", default=2.0))
        max_pages = int(Variable.get("deruedas_max_pages", default=200))
        target_records = int(
            Variable.get("deruedas_target_records", default=1001)
        )
        client = deruedas.build_client(delay)
        raw_root = DIR_BRONCE / "deruedas" / "raw"
        normalized_dir = DIR_BRONCE / "deruedas" / "normalized"
        outputs: list[str] = []
        collected = 0

        if context["ti"].try_number == 1:
            for child in raw_root.iterdir():
                if child.is_dir():
                    shutil.rmtree(child)
            for checkpoint in normalized_dir.glob("*.json*"):
                checkpoint.unlink()
        else:
            for checkpoint in sorted(normalized_dir.glob("*.json")):
                records = json.loads(checkpoint.read_text(encoding="utf-8"))
                if isinstance(records, list) and records:
                    outputs.append(str(checkpoint))
                    collected += len(records)
            log.info(
                "deRuedas: reanudando %d checkpoints con %d registros",
                len(outputs),
                collected,
            )

        for brand in brands:
            slug = _slug(brand)
            output = normalized_dir / f"{slug}.json"
            if str(output) in outputs:
                continue
            remaining = target_records - collected
            if remaining <= 0:
                break
            records = deruedas.search(
                marca=brand,
                client=client,
                limit=remaining,
                max_pages=max_pages,
                raw_dir=raw_root / slug,
            )
            if not records:
                log.warning("deRuedas no devolvió publicaciones para %s", brand)
                continue
            temporary = output.with_suffix(".json.tmp")
            temporary.write_text(
                json.dumps(records, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            temporary.replace(output)
            outputs.append(str(output))
            collected += len(records)
            log.info(
                "deRuedas: checkpoint %s guardado (%d/%d registros)",
                brand,
                collected,
                target_records,
            )

        if not outputs:
            raise ValueError("deRuedas no produjo archivos normalizados")
        return outputs

    @task
    def consolidar_capa_plata(
        carone_path: str,
        deruedas_paths: list[str],
        **context,
    ) -> dict:
        output = DIR_PLATA / f"final_{context['ds']}.csv"
        path = consolidate_json_files(
            [carone_path, *deruedas_paths],
            output,
            group=Variable.get("grupo", default="5K09-03"),
        )
        return {"path": path, "fresh": True}

    @task
    def usar_respaldo_congelado() -> dict:
        seed = DIR_FROZEN / "autos_semilla.csv"
        if not seed.exists():
            raise FileNotFoundError(
                "sin respaldo en include/frozen/autos_semilla.csv"
            )
        return {"path": str(seed), "fresh": False}

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def resolver_dataset(
        fresh_dataset: dict | None,
        frozen_dataset: dict | None,
    ) -> dict:
        selected = fresh_dataset or frozen_dataset
        if not selected:
            raise ValueError("ninguna ruta produjo un dataset")
        return selected

    @task
    def validar_calidad_dataset(dataset: dict, **context) -> dict:
        minimum_rows = int(Variable.get("meta_volumen", default=1001))
        report = validate_dataset(
            dataset["path"],
            minimum_rows=minimum_rows,
        )
        report_path = DIR_SALIDA / "reporte_calidad.json"
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        if dataset["fresh"]:
            shutil.copy2(dataset["path"], DIR_FROZEN / "autos_semilla.csv")
            Variable.set(VAR_ULTIMA_COSECHA, context["ts"])
        return {
            **dataset,
            "quality_report": str(report_path),
        }

    @task
    def generar_entregable_zip(dataset: dict, **context) -> str:
        dag_run = context["dag_run"]
        zip_path = DIR_SALIDA / "tp1_5K09_03.zip"

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.write(dataset["path"], arcname="dataset.csv")
            archive.write(
                dataset["quality_report"],
                arcname="reporte_calidad.json",
            )
            archive.write(__file__, arcname="codigo_dag.py")
            log_dir = (
                f"/usr/local/airflow/logs/dag_id={dag_run.dag_id}"
                f"/run_id={dag_run.run_id}"
            )
            if os.path.exists(log_dir):
                for root, _, files in os.walk(log_dir):
                    for file_name in files:
                        absolute = os.path.join(root, file_name)
                        relative = os.path.relpath(absolute, log_dir)
                        archive.write(
                            absolute,
                            arcname=os.path.join("logs", relative),
                        )
        return str(zip_path)

    setup = crear_carpetas_trabajo()
    route = elegir_ruta_datos()
    brands = descubrir_marcas_deruedas()
    carone_raw = cosecha_bronce_carone()
    deruedas_raw = cosecha_bronce_deruedas(brands)
    fresh = consolidar_capa_plata(carone_raw, deruedas_raw)
    frozen = usar_respaldo_congelado()
    selected = resolver_dataset(fresh, frozen)
    validated = validar_calidad_dataset(selected)
    generar_entregable_zip(validated)

    setup >> route
    route >> [brands, carone_raw, frozen]


pipeline_vehiculos()
