import glob
import os

from pyspark.sql import SparkSession

BASE = "/opt/rapidgo/datalake"

FUENTES = {
    "RAW": f"{BASE}/raw/events",
    "PROCESSED": f"{BASE}/processed/readings",
    "RECHAZADOS": f"{BASE}/processed/rejected",
    "ALERTAS": f"{BASE}/curated/alerts",
    "KPIS": f"{BASE}/curated/kpis",
}

spark = (
    SparkSession.builder
    .appName("InspeccionRapidGo")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    for nombre, ruta in FUENTES.items():
        print(f"\n{'=' * 45}", flush=True)
        print(f"RESULTADO: {nombre}", flush=True)

        archivos = glob.glob(
            os.path.join(ruta, "**", "part-*.parquet"),
            recursive=True,
        )

        if not archivos:
            print("Sin archivos Parquet de datos.", flush=True)
            continue

        df = spark.read.parquet(ruta)

        print(f"Archivos Parquet: {len(archivos)}", flush=True)
        print(f"Total de filas: {df.count()}", flush=True)
        print(f"Columnas: {df.columns}", flush=True)

        if "temperature_status" in df.columns:
            df.groupBy("temperature_status").count().show(
                truncate=False
            )

        if "validation_error" in df.columns:
            df.groupBy("validation_error").count().show(
                truncate=False
            )

        if "alert_type" in df.columns:
            df.groupBy("alert_type").count().show(
                truncate=False
            )

        if nombre == "KPIS":
            df.show(10, truncate=False)
finally:
    spark.stop()
