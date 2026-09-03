from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pandas as pd

from . import schema


def consolidate_json_files(
    json_paths: Iterable[str | Path],
    output_path: str | Path,
    *,
    group: str,
) -> str:
    records: list[dict] = []
    for path_value in json_paths:
        if not path_value:
            continue
        path = Path(path_value)
        if not path.exists():
            continue
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, list):
            raise ValueError(f"{path} no contiene una lista de registros")
        records.extend(payload)

    if not records:
        raise ValueError("no hay registros para consolidar")

    frame = pd.DataFrame(records)
    _require_columns(frame, ["fuente", "id_publicacion"])
    invalid_id = (
        frame["fuente"].isna()
        | frame["id_publicacion"].isna()
        | frame["fuente"].astype(str).str.strip().eq("")
        | frame["id_publicacion"].astype(str).str.strip().eq("")
    )
    if invalid_id.any():
        raise ValueError(f"{int(invalid_id.sum())} registros no tienen fuente/ID")

    frame[schema.CLAVE] = (
        frame["fuente"].astype(str).str.strip()
        + ":"
        + frame["id_publicacion"].astype(str).str.strip()
    )
    for column in schema.NUMERICAS:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["grupo"] = group
    frame = frame.drop_duplicates(subset=[schema.CLAVE], keep="first")

    for column in schema.COLUMNAS:
        if column not in frame.columns:
            frame[column] = pd.NA
    frame = frame[schema.COLUMNAS].reset_index(drop=True)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False, encoding="utf-8")
    return str(output)


def validate_dataset(
    csv_path: str | Path,
    *,
    minimum_rows: int = 1001,
) -> dict:
    frame = pd.read_csv(csv_path)
    _require_columns(frame, schema.REQUERIDAS)

    empty_columns = frame.columns[frame.isna().all()].tolist()
    numeric_columns = frame.select_dtypes(include="number").columns.tolist()
    text_columns = frame.select_dtypes(exclude="number").columns.tolist()
    null_rates = {
        column: round(float(rate), 4)
        for column, rate in frame.isna().mean().sort_values(ascending=False).items()
        if rate > 0
    }

    checks = {
        "clave_unica": bool(frame[schema.CLAVE].is_unique),
        "volumen_suficiente": len(frame) >= minimum_rows,
        "columnas_utiles": frame.shape[1] >= 5,
        "tipos_mixtos": bool(numeric_columns and text_columns),
        "nulos_medidos": True,
        "sin_columnas_vacias": not empty_columns,
        "objetivo_disponible": bool(frame[schema.OBJETIVO].notna().any()),
    }
    failed = [name for name, passed in checks.items() if not passed]
    report = {
        "rows": int(len(frame)),
        "columns": int(frame.shape[1]),
        "checks": checks,
        "numeric_columns": numeric_columns,
        "text_columns": text_columns,
        "null_rates": null_rates,
        "empty_columns": empty_columns,
        "target": schema.OBJETIVO,
    }
    if failed:
        raise ValueError(
            "controles de calidad fallidos: "
            + ", ".join(failed)
            + f"; detalle={report}"
        )
    return report


def _require_columns(frame: pd.DataFrame, columns: Iterable[str]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"faltan columnas requeridas: {', '.join(missing)}")
