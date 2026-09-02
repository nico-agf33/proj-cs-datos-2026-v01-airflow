"""
### Tasación de Vehículos (ingesta mensual)
Grupo: 5K09-03 (Garcia, Via, Ramos, Martin, Velasco)
"""

from __future__ import annotations
import json
import zipfile
import os
import logging
from pathlib import Path
import pendulum
import pandas as pd

from airflow.sdk import Param, Variable, dag, task
from airflow.task.trigger_rule import TriggerRule

### logica de negocio en include/autos
from autos import schema
from autos.collectors import carone, deruedas
from autos.normalize import _slug

log = logging.getLogger(__name__)

### config de directorios
OUTPUT_DIR = Path("/usr/local/airflow/include/output")
DIR_BRONCE = OUTPUT_DIR / "bronze"
DIR_PLATA = OUTPUT_DIR / "silver"
DIR_FROZEN = Path("/usr/local/airflow/include/frozen")

### var de control de ciclo
VAR_ULTIMA_COSECHA = "autos_fecha_ultima_ingesta"

@dag(
    dag_id="tp1_5K09_03_autos_mensual",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 8, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    max_active_tasks=5, 
    tags=["proyecto-integrador", "vehiculos", "entrega-1"],
    params={
        "forzar_descarga": Param(False, type="boolean", title="Forzar recolección inmediata"),
    },
)
def pipeline_vehiculos():

    ### sensor -> validar disponibilidad y obtener catalogo de marcas
    @task.sensor(poke_interval=300, timeout=1800, mode="reschedule", soft_fail=True)
    def monitorear_fuentes(**context):
        """Verifica conectividad y extrae marcas para el paralelismo dinámico."""
        try:
            marcas = deruedas.get_available_brands()
            ### llamada de verificacion para CarOne
            carone_ok = carone.get_available_brands()
            
            if marcas and carone_ok:
                log.info("Portales activos. Se detectaron %s marcas.", len(marcas))
                return {"is_done": True, "xcom_value": marcas}
            return {"is_done": False}
        except Exception as e:
            log.warning("Fuentes no disponibles: %s", e)
            return {"is_done": False}

    ### branching -> ¿descarga o respaldo?
    @task.branch(trigger_rule=TriggerRule.ALL_DONE)
    def decidir_camino_datos(**context) -> str:
        marcas = context["ti"].xcom_pull(task_ids="monitorear_fuentes")
        if marcas:
            return "verificar_ventana_30_dias"
        return "cargar_capa_frozen"

    ### short circuit -> control mensual
    @task.short_circuit
    def verificar_ventana_30_dias(**context) -> bool:
        if context["params"]["forzar_descarga"]:
            return True
            
        ultima = Variable.get(VAR_ULTIMA_COSECHA, default_var=None)
        if not ultima:
            return True
            
        dias = (pendulum.now() - pendulum.parse(ultima)).days
        if dias < 30:
            log.info("Datos frescos (hace %s días). Saltando cosecha.", dias)
            return False
        return True

    ### capa bronce -> ingesta
    @task
    def ingesta_bronce_carone():
        DIR_BRONCE.mkdir(parents=True, exist_ok=True)
        ### `limit` interno del collector 
        datos = carone.search() 
        ruta = DIR_BRONCE / "carone_raw.json"
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump(datos, f, ensure_ascii=False, indent=4)
        return str(ruta)

    @task(map_index_template="{{ task.op_kwargs['marca'] }}")
    def ingesta_bronce_deruedas(marca):
        ### baja todo lo disponible de una marca en DeRuedas
        subfolder = DIR_BRONCE / "deruedas"
        subfolder.mkdir(parents=True, exist_ok=True)
        datos = deruedas.search(marca=marca)
        if not datos: return None
        
        ruta = subfolder / f"raw_{_slug(marca)}.json"
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump(datos, f, ensure_ascii=False, indent=4)
        return str(ruta)

    ### capa plata -> transformacion y dataset tidy
    @task
    def consolidar_capa_plata(path_carone, paths_dr, **context):
        DIR_PLATA.mkdir(parents=True, exist_ok=True)
        unificado = []
        
        ### consolidar archivos JSON
        with open(path_carone, 'r') as f: unificado.extend(json.load(f))
        for p in paths_dr:
            if p and os.path.exists(p):
                with open(p, 'r') as f: unificado.extend(json.load(f))
        
        df = pd.DataFrame(unificado)
        
        ### motor_lt y consumo_lt_100km como FLOATS
        for col in schema.NUMERICAS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        ### columna `grupo`
        df['grupo'] = Variable.get("grupo", default_var="5K09-03")
        
        ### limpiar duplicados
        df = df.drop_duplicates(subset=["id_publicacion"]).reset_index(drop=True)
        
        ### guardar con nombre dinamico {{ ds }}
        ds = context["ds"]
        final_path = DIR_PLATA / f"final_{ds}.csv"
        df.to_csv(final_path, index=False, encoding="utf-8")
        
        ### registrar finalizacion 
        Variable.set(VAR_ULTIMA_COSECHA, ds)
        return str(final_path)

    ### capa frozen
    @task
    def cargar_capa_frozen():
        ruta = DIR_FROZEN / "autos_semilla.csv"
        if ruta.exists(): return str(ruta)
        raise FileNotFoundError("Sin respaldo disponible en include/frozen/")

    ### validar
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def validar_calidad_dataset(csv_path):
        df = pd.read_csv(csv_path)
        meta = int(Variable.get("meta_volumen", default_var=9000))
        
        problemas = []
        if not df["id_publicacion"].is_unique: problemas.append("IDs duplicados")
        if len(df) < meta: problemas.append(f"Volumen insuficiente: {len(df)}")
        if not pd.api.types.is_float_dtype(df["motor_lt"]): problemas.append("motor_lt no es float")
        
        if problemas:
            raise ValueError(f"Calidad insuficiente: {', '.join(problemas)}")
        
        log.info("Dataset validado")
        return csv_path

    ### archivo .zip
    @task
    def armar_zip_entrega(csv_path: str, **context):
        dag_run = context["dag_run"]
        grupo_slug = _slug(Variable.get("grupo", default_var="5K09-03"))
        nombre_zip = f"tp1_{grupo_slug}.zip"
        path_zip = DIR_SALIDA / nombre_zip
        
        ### generar manifiesto.json 
        df = pd.read_csv(csv_path)
        manifiesto = {
            "grupo": Variable.get("grupo", default_var="5K09-03"),
            "integrantes": ["Garcia, Nicolas", "Via, Tomas", "Ramos, Ignacio", "Martin, Sergio", "Velasco, Victoria"],
            "filas": int(df.shape[0]),
            "columnas": int(df.shape[1]),
            "run_id": dag_run.run_id,
            "generado_en": pendulum.now().to_iso8601_string()
        }
        with open(DIR_SALIDA / "manifiesto.json", "w") as f:
            json.dump(manifiesto, f, indent=4)

        ### generar bronce.txt
        with open(DIR_SALIDA / "bronce.txt", "w") as f:
            for file in DIR_BRONCE.rglob("*.json"):
                f.write(f"{file}\n")

        with zipfile.ZipFile(path_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(csv_path, arcname="dataset.csv")
            zipf.write(DIR_SALIDA / "manifiesto.json", arcname="manifiesto.json")
            zipf.write(DIR_SALIDA / "bronce.txt", arcname="bronce.txt")
            zipf.write(__file__, arcname="codigo_dag.py")
            
            ### logs
            base_logs = f"/usr/local/airflow/logs/dag_id={dag_run.dag_id}/run_id={dag_run.run_id}"
            if os.path.exists(base_logs):
                for root, _, archivos in os.walk(base_logs):
                    for a in archivos:
                        fp = os.path.join(root, a)
                        zipf.write(fp, arcname=os.path.join("logs", os.path.relpath(fp, base_logs)))
        
        return str(path_zip)

    ### flujo
    marcas_cat = monitorear_fuentes()
    camino = decidir_camino_datos()
    frescura = verificar_ventana_30_dias()
    
    c_raw = ingesta_bronce_carone()
    d_raw = ingesta_bronce_deruedas.expand(marca=marcas_cat)
    
    plata = consolidar_capa_plata(c_raw, d_raw)
    congelado = cargar_capa_frozen()
    
    validado = validar_calidad_dataset(plata)
    armar_zip_entrega(validado)

    ### orquestar dependencias
    marcas_cat >> camino
    camino >> [frescura, congelado]
    frescura >> [c_raw, d_raw]

pipeline_vehiculos()