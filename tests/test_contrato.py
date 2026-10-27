"""Pruebas del contrato JSON del evento (samples/evento_lectura.schema.json)."""
import copy
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

RAIZ = Path(__file__).resolve().parent.parent
ESQUEMA = json.loads((RAIZ / "samples" / "evento_lectura.schema.json").read_text(encoding="utf-8"))
EJEMPLO = json.loads((RAIZ / "samples" / "evento_lectura_ejemplo.json").read_text(encoding="utf-8"))
validador = Draft202012Validator(ESQUEMA)


def test_ejemplo_cumple_el_contrato():
    assert list(validador.iter_errors(EJEMPLO)) == []


@pytest.mark.parametrize("campo", ESQUEMA["required"])
def test_falta_campo_obligatorio(campo):
    ev = copy.deepcopy(EJEMPLO)
    del ev[campo]
    assert list(validador.iter_errors(ev)) != []


@pytest.mark.parametrize("campo,valor", [
    ("order_id", "PED-1001"),        # formato incorrecto
    ("vehicle_id", "VEH-001"),       # formato incorrecto
    ("product_type", "seco"),        # fuera del enum
    ("temperature_c", 75),           # sobre el maximo permitido por el esquema
    ("temperature_c", "frio"),       # tipo incorrecto
    ("humidity_pct", 120),           # fuera de 0-100
    ("team_id", "otro-equipo"),      # no coincide con la constante
])
def test_valor_invalido_es_rechazado(campo, valor):
    ev = copy.deepcopy(EJEMPLO)
    ev[campo] = valor
    assert list(validador.iter_errors(ev)) != []


def test_campo_extra_es_rechazado():
    ev = copy.deepcopy(EJEMPLO)
    ev["timestamp"] = "2026-10-21T10:00:00Z"
    assert list(validador.iter_errors(ev)) != []
