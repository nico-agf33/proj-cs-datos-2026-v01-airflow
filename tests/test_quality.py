from __future__ import annotations

import json

import pandas as pd
import pytest

from autos import schema
from autos.quality import consolidate_json_files, validate_dataset


def _record(index: int, source: str = "deruedas") -> dict:
    return {
        "fuente": source,
        "id_publicacion": str(index),
        "marca": "Marca",
        "modelo": "Modelo",
        "version": "Base",
        "fabricado_en": 2020,
        "kilometraje": 50000,
        "precio": 20000000,
        "moneda": "ARS",
        "motor_lt": 1.6,
        "potencia_hp": 110,
        "transmision": "Manual",
        "traccion": "Delantera",
        "combustible": "Nafta",
        "consumo_lt_100km": 8.0,
        "ubicacion": "AR",
        "url": f"https://example.test/{source}/{index}",
        "fecha_ingesta": "2026-09-03T12:00:00+00:00",
    }


def test_consolidation_builds_cross_source_key_and_quality_report(tmp_path):
    records = [_record(index) for index in range(1000)]
    records.extend([_record(0), _record(0, source="carone")])
    source_path = tmp_path / "records.json"
    source_path.write_text(json.dumps(records), encoding="utf-8")
    output_path = tmp_path / "dataset.csv"

    consolidate_json_files([source_path], output_path, group="5K09-03")
    report = validate_dataset(output_path, minimum_rows=1001)
    frame = pd.read_csv(output_path)

    assert len(frame) == 1001
    assert frame[schema.CLAVE].is_unique
    assert {"deruedas:0", "carone:0"}.issubset(set(frame[schema.CLAVE]))
    assert all(report["checks"].values())
    assert report["target"] == "precio"


def test_quality_rejects_an_entirely_empty_column(tmp_path):
    frame = pd.DataFrame([_record(1)])
    frame[schema.CLAVE] = "deruedas:1"
    frame["grupo"] = "5K09-03"
    frame["consumo_lt_100km"] = pd.NA
    output_path = tmp_path / "bad.csv"
    frame[schema.COLUMNAS].to_csv(output_path, index=False)

    with pytest.raises(ValueError, match="sin_columnas_vacias"):
        validate_dataset(output_path, minimum_rows=1)
