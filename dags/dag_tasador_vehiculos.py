"""
Grupo: 5K09-03
"""
from __future__ import annotations
import json
import zipfile
import os
import logging
from pathlib import Path
import pendulum
import pandas as pd

from airflow.sdk import Param, Variable, dag, task, PokeReturnValue as ResultadoValidacion
from airflow.task.trigger_rule import TriggerRule

### logica de negocio
from autos import schema
from autos.collectors import carone, deruedas
from autos.normalize import _slug

log = logging.getLogger(__name__)

### config de directorios
DIR_SALIDA = Path("/usr/local/airflow/include/output")
DIR_BRONCE = DIR_SALIDA / "bronze"
DIR_PLATA = DIR_SALIDA / "silver"
DIR_FROZEN = Path("/usr/local/airflow/include/frozen")

VAR_ULTIMA_COSECHA = "autos_fecha_ultima_ingesta"

@dag(
    dag_id="tp1_5K09_03_autos_mensual",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 8, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    max_active_tasks=1, 
    tags=["proyecto-integrador", "vehiculos"],
    params={
        "forzar_descarga": Param(False, type="boolean", title="Forzar recolección ahora"),
    },
)
def pipeline_vehiculos():

    #### sensor -> verificar que los portales respondan
    @task.sensor(poke_interval=60, timeout=1800, mode="reschedule", soft_fail=True)
    def validar_disponibilidad_fuentes():
        try:
            ### si se obtienen marcas, el sitio esta online
            m_carone = carone.get_available_brands()
            m_deruedas = deruedas.get_available_brands()
            if m_carone and m_deruedas:
                log.info("Fuentes en linea.")
                return ResultadoValidacion(is_done=True)
            return ResultadoValidacion(is_done=False)
        except Exception as e:
            log.warning("Fallo de conectividad: %s", e)
            return ResultadoValidacion(is_done=False)

    ### listado de marcas
    @task
    def obtener_marcas_actuales():
        marcas = deruedas.get_available_brands()
        if not marcas:
            raise ValueError("no se detectaron marcas para procesar.")
        return marcas

    ### branching -> ¿descarga nueva o capa frozen?
    @task.branch(trigger_rule=TriggerRule.ALL_DONE)
    def elegir_ruta_datos(marcas_ok) -> str:
        if marcas_ok:
            return "chequear_refresh_mensual"
        return "usar_respaldo_congelado"

    ### short circuit -> ciclo de 30 dias
    @task.short_circuit
    def chequear_refresh_mensual(**context) -> bool:
        if context["params"]["forzar_descarga"]:
            return True
        ultima_fecha = Variable.get(VAR_ULTIMA_COSECHA, default_var=None)
        if not ultima_fecha:
            return True
        dias_transcurridos = (pendulum.now() - pendulum.parse(ultima_fecha)).days
        return dias_transcurridos >= 30

    ### capa bronce -> ingesta
    @task
    def cosecha_bronce_carone():
        DIR_BRONCE.mkdir(parents=True, exist_ok=True)
        ### el collector search() se detiene cuando la API no devuelve mas nada
        datos_api = carone.search() 
        ruta_archivo = DIR_BRONCE / "carone_raw.json"
        with open(ruta_archivo, "w", encoding="utf-8") as f:
            json.dump(datos_api, f, ensure_ascii=False, indent=4)
        return str(ruta_archivo)

    @task(map_index_template="{{ task.op_kwargs['marca'] }}")
    def cosecha_bronce_deruedas(marca):
        subcarpeta = DIR_BRONCE / "deruedas"
        subcarpeta.mkdir(parents=True, exist_ok=True)
        ### el collector search() se detiene cuando no hay mas paginas
        datos_scraping = deruedas.search(marca=marca)
        if not datos_scraping: return None
        
        ruta_archivo = subcarpeta / f"crudo_{_slug(marca)}.json"
        with open(ruta_archivo, "w", encoding="utf-8") as f:
            json.dump(datos_scraping, f, ensure_ascii=False, indent=4)
        return str(ruta_archivo)

    ### capa plata -> transformacion y dataset tidy
    @task
    def consolidar_capa_plata(json_carone, lista_jsons_dr, **context):
        DIR_PLATA.mkdir(parents=True, exist_ok=True)
        registros_unificados = []
        
        with open(json_carone, 'r') as f:
            registros_unificados.extend(json.load(f))
            
        for path in lista_jsons_dr:
            if path and os.path.exists(path):
                with open(path, 'r') as f:
                    registros_unificados.extend(json.load(f))
        
        df = pd.DataFrame(registros_unificados)
        
        for col in schema.NUMERICAS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df['grupo'] = Variable.get("grupo", default_var="5K09-03")
        df = df.drop_duplicates(subset=["id_publicacion"]).reset_index(drop=True)
        
        ds = context["ds"]
        ruta_final = DIR_PLATA / f"final_{ds}.csv"
        df.to_csv(ruta_final, index=False, encoding="utf-8")
        
        Variable.set(VAR_ULTIMA_COSECHA, ds)
        return str(ruta_final)

    ### capa frozen
    @task
    def usar_respaldo_congelado():
        ruta_semilla = DIR_FROZEN / "autos_semilla.csv"
        if ruta_semilla.exists():
            return str(ruta_semilla)
        raise FileNotFoundError("sin respaldo disponible en include/frozen/")

    ### validacion y entrega
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def validar_calidad_dataset(ruta_csv):
        df = pd.read_csv(ruta_csv)
        meta_v = int(Variable.get("meta_volumen", default_var=9000))
        if len(df) < meta_v:
            raise ValueError(f"volumen insuficiente: {len(df)}/{meta_v}")
        return ruta_csv

    @task
    def generar_entregable_zip(csv_path: str, **context):
        dag_run = context["dag_run"]
        nombre_zip = f"tp1_5K09_03.zip"
        path_zip = DIR_SALIDA / nombre_zip
        
        with zipfile.ZipFile(path_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(csv_path, arcname="dataset.csv")
            zipf.write(__file__, arcname="codigo_dag.py")
            ### logs
            log_dir = f"/usr/local/airflow/logs/dag_id={dag_run.dag_id}/run_id={dag_run.run_id}"
            if os.path.exists(log_dir):
                for root, _, archivos in os.walk(log_dir):
                    for a in archivos:
                        ruta_abs = os.path.join(root, a)
                        zipf.write(ruta_abs, arcname=os.path.join("logs", os.path.relpath(ruta_abs, log_dir)))
        return str(path_zip)

    ### flujo del grafo
    fuentes_vivas = validar_disponibilidad_fuentes()
    lista_marcas = obtener_marcas_actuales()
    camino = elegir_ruta_datos(lista_marcas)
    refresh = chequear_refresh_mensual()
    
    ### dependencias de ingesta
    c_raw = cosecha_bronce_carone()
    d_raw = cosecha_bronce_deruedas.expand(marca=lista_marcas)
    
    plata = consolidar_capa_plata(c_raw, d_raw)
    congelado = usar_respaldo_congelado()
    
    ### flujo de cierre
    validado = validar_calidad_dataset(plata)
    generar_entregable_zip(validado)

    ### orquestacion de dependencias
    fuentes_vivas >> lista_marcas >> camino
    camino >> [refresh, congelado]
    refresh >> [c_raw, d_raw]

pipeline_vehiculos()