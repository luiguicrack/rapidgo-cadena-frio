"""Pruebas de las reglas de alerta de cadena de frio (spark/alert_rules.py)."""
import pytest

from spark.alert_rules import (CRITICA, FUERA_DE_RANGO, NORMAL, clasificar_lectura,
                               duracion_exposicion_min, hay_exposicion_prolongada)


# ---------- Alerta 1: temperatura critica ----------
@pytest.mark.parametrize("producto,temp,esperado", [
    ("refrigerado", 5.0, NORMAL),            # caso normal
    ("refrigerado", 2.0, NORMAL),            # limite inferior incluido
    ("refrigerado", 8.0, NORMAL),            # limite superior incluido
    ("refrigerado", 8.1, FUERA_DE_RANGO),    # apenas fuera
    ("refrigerado", 11.0, FUERA_DE_RANGO),   # 11 aun no es critica (> 11)
    ("refrigerado", 11.1, CRITICA),
    ("refrigerado", -1.0, FUERA_DE_RANGO),   # -1 aun no es critica (< -1)
    ("refrigerado", -1.1, CRITICA),
    ("congelado", -20.0, NORMAL),
    ("congelado", -15.0, NORMAL),            # limite superior incluido
    ("congelado", -14.9, FUERA_DE_RANGO),
    ("congelado", -12.0, FUERA_DE_RANGO),    # -12 aun no es critica (> -12)
    ("congelado", -11.9, CRITICA),
    ("congelado", -25.0, NORMAL),            # limite inferior incluido
])
def test_clasificar_lectura(producto, temp, esperado):
    assert clasificar_lectura(producto, temp) == esperado


def test_producto_invalido():
    with pytest.raises(ValueError):
        clasificar_lectura("seco", 5.0)


@pytest.mark.parametrize("temp", [float("nan"), "5", None, True])
def test_temperatura_invalida(temp):
    with pytest.raises(ValueError):
        clasificar_lectura("refrigerado", temp)


# ---------- Alerta 2: exposicion prolongada (10 min) ----------
def _lecturas(temps, paso_seg=60):
    """Lecturas cada `paso_seg` segundos a partir de las 10:00:00Z."""
    return [(f"2026-10-21T10:{(i * paso_seg) // 60:02d}:{(i * paso_seg) % 60:02d}Z", t)
            for i, t in enumerate(temps)]


def test_sin_lecturas_fuera_de_rango_no_hay_exposicion():
    assert duracion_exposicion_min("refrigerado", _lecturas([5, 5, 6])) == 0.0


def test_exposicion_de_10_minutos_dispara_alerta():
    # 11 lecturas por minuto fuera de rango = 10 minutos continuos
    lect = _lecturas([9.0] * 11)
    assert duracion_exposicion_min("refrigerado", lect) == pytest.approx(10.0)
    assert hay_exposicion_prolongada("refrigerado", lect) is True


def test_exposicion_menor_a_10_minutos_no_dispara_alerta():
    assert hay_exposicion_prolongada("refrigerado", _lecturas([9.0] * 10)) is False  # 9 min


def test_una_lectura_normal_reinicia_la_racha():
    lect = _lecturas([9.0] * 8 + [5.0] + [9.0] * 8)
    assert hay_exposicion_prolongada("refrigerado", lect) is False


def test_exposicion_ya_terminada_no_cuenta_como_actual():
    lect = _lecturas([9.0] * 12 + [5.0])
    assert duracion_exposicion_min("refrigerado", lect) == 0.0


def test_lecturas_desordenadas_se_ordenan_por_tiempo():
    lect = list(reversed(_lecturas([9.0] * 11)))
    assert hay_exposicion_prolongada("refrigerado", lect) is True


def test_el_tipo_de_producto_cambia_el_resultado():
    lect = _lecturas([-10.0] * 11)
    assert hay_exposicion_prolongada("congelado", lect) is True
    assert hay_exposicion_prolongada("refrigerado", lect) is True  # -10 tambien esta fuera de 2-8
    assert hay_exposicion_prolongada("congelado", _lecturas([-20.0] * 11)) is False
