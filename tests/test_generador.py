"""Pruebas del generador de eventos (no requieren Kafka)."""
import json
from datetime import datetime
from pathlib import Path

from jsonschema import Draft202012Validator

from producer.generador import GeneradorEventos
from spark.alert_rules import CRITICA, FUERA_DE_RANGO, clasificar_lectura

RAIZ = Path(__file__).resolve().parent.parent
validador = Draft202012Validator(
    json.loads((RAIZ / "samples" / "evento_lectura.schema.json").read_text(encoding="utf-8")))


def _eventos(n, **kw):
    g = GeneradorEventos(seed=42, **kw)
    return [g.siguiente_evento() for _ in range(n)]


def test_todos_los_eventos_cumplen_el_contrato():
    for ev in _eventos(500, prob_excursion=0.1):
        assert list(validador.iter_errors(ev)) == []


def test_misma_semilla_produce_los_mismos_eventos():
    a = [(e["order_id"], e["temperature_c"]) for e in _eventos(60)]
    b = [(e["order_id"], e["temperature_c"]) for e in _eventos(60)]
    assert a == b


def test_cada_pedido_mantiene_vehiculo_y_producto():
    vistos = {}
    for ev in _eventos(300):
        par = (ev["vehicle_id"], ev["product_type"])
        assert vistos.setdefault(ev["order_id"], par) == par


def test_event_id_es_unico():
    ids = [e["event_id"] for e in _eventos(500)]
    assert len(set(ids)) == len(ids)


def test_event_ts_nunca_retrocede():
    ts = [datetime.fromisoformat(e["event_ts"].replace("Z", "+00:00")) for e in _eventos(200)]
    assert ts == sorted(ts)


def test_se_generan_lecturas_fuera_de_rango_y_criticas():
    clases = {clasificar_lectura(e["product_type"], e["temperature_c"])
              for e in _eventos(1500, prob_excursion=0.05)}
    assert FUERA_DE_RANGO in clases or CRITICA in clases
    assert CRITICA in clases


def test_sin_excursiones_no_hay_alertas_criticas():
    clases = {clasificar_lectura(e["product_type"], e["temperature_c"])
              for e in _eventos(400, prob_excursion=0.0)}
    assert CRITICA not in clases


def test_parametros_invalidos_lanzan_error():
    import pytest
    with pytest.raises(ValueError):
        GeneradorEventos(num_pedidos=0)
    with pytest.raises(ValueError):
        GeneradorEventos(prob_excursion=1.5)
