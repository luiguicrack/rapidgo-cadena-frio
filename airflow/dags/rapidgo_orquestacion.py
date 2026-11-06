from datetime import datetime, timedelta
from pathlib import Path
import socket

from airflow import DAG
from airflow.operators.python import PythonOperator


KAFKA_HOST = "kafka"
KAFKA_PORT = 9092
DATALAKE = Path("/opt/rapidgo/datalake")


def verificar_kafka():
    """Comprueba que Kafka acepta conexiones desde Airflow."""
    try:
        with socket.create_connection((KAFKA_HOST, KAFKA_PORT), timeout=10):
            print("Kafka está disponible.")
    except OSError as error:
        raise RuntimeError(f"No fue posible conectar con Kafka: {error}")


def verificar_datalake():
    """Comprueba que Spark haya guardado datos Parquet."""
    rutas = {
        "RAW": DATALAKE / "raw" / "events",
        "PROCESSED": DATALAKE / "processed" / "readings",
    }

    for nombre, ruta in rutas.items():
        if not ruta.exists():
            raise FileNotFoundError(f"No existe la ruta {nombre}: {ruta}")

        archivos = list(ruta.rglob("part-*.parquet"))

        if not archivos:
            raise RuntimeError(
                f"No se encontraron archivos Parquet en {nombre}: {ruta}"
            )

        print(f"{nombre}: {len(archivos)} archivo(s) Parquet encontrados.")


with DAG(
    dag_id="rapidgo_orquestacion",
    description="Verifica Kafka y los resultados del procesamiento de RapidGo",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "rapidgo",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["rapidgo", "big-data", "cadena-frio"],
) as dag:

    tarea_verificar_kafka = PythonOperator(
        task_id="verificar_kafka",
        python_callable=verificar_kafka,
    )

    tarea_verificar_datalake = PythonOperator(
        task_id="verificar_datalake",
        python_callable=verificar_datalake,
    )

    tarea_verificar_kafka >> tarea_verificar_datalake