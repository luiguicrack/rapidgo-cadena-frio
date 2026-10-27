"""Generacion de eventos simulados de cadena de frio (sin dependencias de Kafka).

Cada pedido tiene un vehiculo y un tipo de producto fijos. La temperatura
sigue una caminata aleatoria alrededor del valor nominal y, de vez en cuando,
un pedido entra en una "excursion" (varias lecturas seguidas fuera de rango)
para poder probar las alertas de temperatura critica y exposicion prolongada.
"""
import random
import uuid
from datetime import datetime, timedelta, timezone

TEAM_ID = "grupo-1-cadena-frio"

# Temperatura nominal (centro del rango permitido) por tipo de producto.
NOMINAL = {"refrigerado": 5.0, "congelado": -20.0}
# Desplazamiento de las excursiones: (suave, critica) en grados.
EXCURSION = {"refrigerado": (5.0, 8.0), "congelado": (7.0, 12.0)}


class GeneradorEventos:
    def __init__(self, num_pedidos=6, num_vehiculos=3, prob_excursion=0.03,
                 duracion_excursion=(25, 45), paso_simulado_seg=30, seed=None,
                 inicio=None):
        if num_pedidos < 1 or num_vehiculos < 1:
            raise ValueError("num_pedidos y num_vehiculos deben ser >= 1")
        if not 0 <= prob_excursion <= 1:
            raise ValueError("prob_excursion debe estar entre 0 y 1")
        if paso_simulado_seg <= 0:
            raise ValueError("paso_simulado_seg debe ser > 0")
        self.rng = random.Random(seed)
        self.prob_excursion = prob_excursion
        self.duracion_excursion = duracion_excursion
        self.paso = timedelta(seconds=paso_simulado_seg)
        self.reloj = inicio or datetime.now(timezone.utc).replace(microsecond=0)
        self.pedidos = []
        for i in range(num_pedidos):
            producto = "refrigerado" if i % 2 == 0 else "congelado"
            self.pedidos.append({
                "order_id": f"ORD-{100000 + i:06d}",
                "vehicle_id": f"VEH-{(i % num_vehiculos) + 1:02d}",
                "product_type": producto,
                "temp": NOMINAL[producto],
                "excursion_restante": 0,
                "excursion_offset": 0.0,
            })
        self._i = 0

    def _siguiente_temperatura(self, p):
        nominal = NOMINAL[p["product_type"]]
        if p["excursion_restante"] > 0:
            p["excursion_restante"] -= 1
            objetivo = nominal + p["excursion_offset"]
        else:
            if self.rng.random() < self.prob_excursion:
                suave, critica = EXCURSION[p["product_type"]]
                signo = 1 if p["product_type"] == "congelado" else self.rng.choice([1, 1, -1])
                p["excursion_offset"] = signo * self.rng.choice([suave, critica])
                p["excursion_restante"] = self.rng.randint(*self.duracion_excursion)
                objetivo = nominal + p["excursion_offset"]
            else:
                objetivo = nominal
        # Acerca la temperatura al objetivo con algo de ruido.
        p["temp"] += (objetivo - p["temp"]) * 0.5 + self.rng.uniform(-0.4, 0.4)
        return round(max(-40.0, min(60.0, p["temp"])), 2)

    def siguiente_evento(self) -> dict:
        p = self.pedidos[self._i % len(self.pedidos)]
        self._i += 1
        # El reloj simulado avanza una vez por ronda completa de pedidos.
        if self._i > 1 and (self._i - 1) % len(self.pedidos) == 0:
            self.reloj += self.paso
        humedad = round(max(0.0, min(100.0, self.rng.gauss(65.0, 5.0))), 1)
        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "team_id": TEAM_ID,
            "order_id": p["order_id"],
            "vehicle_id": p["vehicle_id"],
            "product_type": p["product_type"],
            "temperature_c": self._siguiente_temperatura(p),
            "humidity_pct": humedad,
            "event_ts": self.reloj.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
