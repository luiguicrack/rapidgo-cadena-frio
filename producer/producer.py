"""Producer de lecturas de cadena de frio hacia Kafka.

Uso (desde la raiz del repositorio):
    python -m producer.producer --dry-run --max-eventos 10
    python -m producer.producer --max-eventos 300 --intervalo 0.2

Desde Windows el broker se alcanza en localhost:29092 (listener PLAINTEXT_HOST).
Dentro de la red de Docker se usaria kafka:9092.
"""
import argparse
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

from producer.generador import GeneradorEventos

RAIZ = Path(__file__).resolve().parent.parent
RUTA_ESQUEMA = RAIZ / "samples" / "evento_lectura.schema.json"


def cargar_validador():
    from jsonschema import Draft202012Validator
    with open(RUTA_ESQUEMA, encoding="utf-8") as f:
        return Draft202012Validator(json.load(f))


def parsear_args(argv=None):
    ap = argparse.ArgumentParser(description="Producer de lecturas de cadena de frio")
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"))
    ap.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "rapidgo.coldchain.readings"))
    ap.add_argument("--max-eventos", type=int, default=0, help="0 = infinito")
    ap.add_argument("--intervalo", type=float, default=1.0, help="segundos reales entre eventos")
    ap.add_argument("--pedidos", type=int, default=6)
    ap.add_argument("--vehiculos", type=int, default=3)
    ap.add_argument("--prob-excursion", type=float, default=0.03)
    ap.add_argument("--paso-simulado", type=int, default=30,
                    help="segundos simulados que avanza event_ts por ronda de pedidos")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true", help="imprime eventos sin usar Kafka")
    ap.add_argument("--sin-validar", action="store_true", help="no validar contra el esquema")
    return ap.parse_args(argv)


def main(argv=None):
    a = parsear_args(argv)
    gen = GeneradorEventos(a.pedidos, a.vehiculos, a.prob_excursion,
                           paso_simulado_seg=a.paso_simulado, seed=a.seed)
    validador = None if a.sin_validar else cargar_validador()

    producer = None
    por_particion = Counter()
    errores = []
    if not a.dry_run:
        from confluent_kafka import Producer
        producer = Producer({"bootstrap.servers": a.bootstrap, "client.id": "coldchain-producer"})

        def al_entregar(err, msg):
            if err is not None:
                errores.append(str(err))
            else:
                por_particion[msg.partition()] += 1

    enviados = 0
    try:
        while a.max_eventos == 0 or enviados < a.max_eventos:
            ev = gen.siguiente_evento()
            if validador is not None:
                errs = list(validador.iter_errors(ev))
                if errs:
                    raise ValueError(f"Evento invalido: {errs[0].message}")
            if a.dry_run:
                print(json.dumps(ev, ensure_ascii=False))
            else:
                producer.produce(a.topic, key=ev["order_id"].encode(),
                                 value=json.dumps(ev).encode(), callback=al_entregar)
                producer.poll(0)
            enviados += 1
            if a.intervalo > 0:
                time.sleep(a.intervalo)
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.", file=sys.stderr)
    finally:
        if producer is not None:
            producer.flush(15)
            print(f"Eventos enviados: {enviados}", file=sys.stderr)
            print(f"Entregados por particion: {dict(sorted(por_particion.items()))}", file=sys.stderr)
            if errores:
                print(f"Errores de entrega: {len(errores)} (primero: {errores[0]})", file=sys.stderr)
                sys.exit(1)


if __name__ == "__main__":
    main()
