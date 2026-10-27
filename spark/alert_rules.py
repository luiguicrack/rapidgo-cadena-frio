"""Reglas de negocio de la cadena de frio (Python puro, sin Spark ni Kafka).

Se mantienen aqui los umbrales y la logica para poder probarlos con pytest.
Los umbrales son los valores iniciales de la seccion 10 del README y deben
ajustarse (y justificarse) cuando se observen datos reales.
"""
from datetime import datetime

# Rango permitido (minimo, maximo) en grados Celsius, ambos extremos incluidos.
RANGOS_PERMITIDOS = {
    "refrigerado": (2.0, 8.0),
    "congelado": (-25.0, -15.0),
}

# Umbrales criticos (bajo, alto). None significa "sin limite definido".
# refrigerado: critica si < -1 o > 11. congelado: critica si > -12.
UMBRALES_CRITICOS = {
    "refrigerado": (-1.0, 11.0),
    "congelado": (None, -12.0),
}

MINUTOS_EXPOSICION_PROLONGADA = 10

NORMAL = "NORMAL"
FUERA_DE_RANGO = "FUERA_DE_RANGO"
CRITICA = "CRITICA"


def _validar_producto(product_type: str) -> None:
    if product_type not in RANGOS_PERMITIDOS:
        raise ValueError(f"Tipo de producto invalido: {product_type!r}")


def esta_fuera_de_rango(product_type: str, temperatura_c: float) -> bool:
    _validar_producto(product_type)
    minimo, maximo = RANGOS_PERMITIDOS[product_type]
    return temperatura_c < minimo or temperatura_c > maximo


def clasificar_lectura(product_type: str, temperatura_c: float) -> str:
    """Devuelve NORMAL, FUERA_DE_RANGO o CRITICA para una lectura."""
    _validar_producto(product_type)
    if isinstance(temperatura_c, bool) or not isinstance(temperatura_c, (int, float)):
        raise ValueError("La temperatura debe ser numerica")
    if temperatura_c != temperatura_c:  # NaN
        raise ValueError("La temperatura no puede ser NaN")

    bajo, alto = UMBRALES_CRITICOS[product_type]
    if (bajo is not None and temperatura_c < bajo) or (alto is not None and temperatura_c > alto):
        return CRITICA
    if esta_fuera_de_rango(product_type, temperatura_c):
        return FUERA_DE_RANGO
    return NORMAL


def _a_datetime(valor) -> datetime:
    if isinstance(valor, datetime):
        return valor
    return datetime.fromisoformat(valor.replace("Z", "+00:00"))


def duracion_exposicion_min(product_type: str, lecturas) -> float:
    """Minutos de la exposicion continua fuera de rango mas reciente.

    `lecturas` es una lista de (timestamp, temperatura) de UN pedido, en
    cualquier orden. La exposicion continua se mide desde la primera lectura
    fuera de rango de la racha actual hasta la ultima lectura de esa racha.
    Una lectura NORMAL reinicia la racha. Si la ultima lectura es normal,
    la exposicion actual es 0.
    """
    _validar_producto(product_type)
    ordenadas = sorted(((_a_datetime(ts), t) for ts, t in lecturas), key=lambda x: x[0])
    inicio = None
    ultimo = None
    for ts, temp in ordenadas:
        if esta_fuera_de_rango(product_type, temp):
            if inicio is None:
                inicio = ts
            ultimo = ts
        else:
            inicio = None
            ultimo = None
    if inicio is None:
        return 0.0
    return (ultimo - inicio).total_seconds() / 60.0


def hay_exposicion_prolongada(product_type: str, lecturas,
                              minutos: float = MINUTOS_EXPOSICION_PROLONGADA) -> bool:
    return duracion_exposicion_min(product_type, lecturas) >= minutos
