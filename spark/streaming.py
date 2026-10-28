"""Procesamiento de lecturas de cadena de frío con Spark Structured Streaming."""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    lit,
    sum as spark_sum,
    to_date,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# Configuración del pipeline
KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
)
KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC", "rapidgo.coldchain.readings"
)
STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
DATA_LAKE = os.getenv("DATALAKE_PATH", "datalake")

RAW_PATH = f"{DATA_LAKE}/raw/events"
PROCESSED_PATH = f"{DATA_LAKE}/processed/readings"
REJECTED_PATH = f"{DATA_LAKE}/processed/rejected"
ALERTS_PATH = f"{DATA_LAKE}/curated/alerts"
KPIS_PATH = f"{DATA_LAKE}/curated/kpis"
CHECKPOINTS = f"{DATA_LAKE}/checkpoints/spark"

TEAM_ID = "grupo-1-cadena-frio"

UUID_PATTERN = (
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}$"
)

# Esquema de lectura compatible con los campos del contrato JSON.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("team_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("product_type", StringType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("humidity_pct", DoubleType(), True),
    StructField("event_ts", StringType(), True),
])


def crear_spark():
    return (
        SparkSession.builder
        .appName("RapidGo-CadenaFrio-Streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )


def clasificar_lecturas(df):
    """Clasifica temperaturas según los umbrales iniciales del proyecto."""

    refrigerado = col("product_type") == "refrigerado"
    congelado = col("product_type") == "congelado"
    temperatura = col("temperature_c")

    critica = (
        (
            refrigerado
            & ((temperatura < -1.0) | (temperatura > 11.0))
        )
        | (congelado & (temperatura > -12.0))
    )

    fuera_rango = (
        (
            refrigerado
            & ((temperatura < 2.0) | (temperatura > 8.0))
        )
        | (
            congelado
            & ((temperatura < -25.0) | (temperatura > -15.0))
        )
    )

    return (
        df
        .withColumn("is_critical", critica)
        .withColumn("is_out_of_range", fuera_rango)
        .withColumn(
            "temperature_status",
            when(critica, lit("critica"))
            .when(fuera_rango, lit("fuera_rango"))
            .otherwise(lit("ok")),
        )
        .withColumn("event_date", to_date(col("event_time")))
    )


def main():
    spark = crear_spark()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Leer el flujo de eventos de Kafka.
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .load()
    )

    # 2. Capa Raw: conservar los mensajes tal como llegaron.
    raw_events = kafka_stream.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("raw_value"),
    )

    raw_query = (
        raw_events.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", RAW_PATH)
        .option("checkpointLocation", f"{CHECKPOINTS}/raw")
        .start()
    )

    # 3. Interpretar el JSON usando el contrato del proyecto.
    decoded = (
        kafka_stream
        .select(
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("key").cast("string").alias("message_key"),
            col("value").cast("string").alias("raw_value"),
            from_json(
                col("value").cast("string"),
                EVENT_SCHEMA,
            ).alias("event"),
        )
        .select(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "message_key",
            "raw_value",
            "event.*",
        )
        .withColumn("event_time", to_timestamp(col("event_ts")))
    )

    # 4. Validar campos obligatorios, formatos y rangos básicos.
    missing_fields = (
        col("event_id").isNull()
        | col("team_id").isNull()
        | col("order_id").isNull()
        | col("vehicle_id").isNull()
        | col("product_type").isNull()
        | col("temperature_c").isNull()
        | col("humidity_pct").isNull()
        | col("event_ts").isNull()
    )

    validation_error = (
        when(
            missing_fields,
            lit("JSON invalido o campos obligatorios ausentes"),
        )
        .when(
            ~col("event_id").rlike(UUID_PATTERN),
            lit("event_id no tiene formato UUID"),
        )
        .when(
            col("team_id") != TEAM_ID,
            lit("team_id no corresponde al equipo"),
        )
        .when(
            ~col("order_id").rlike(r"^ORD-[0-9]{6}$"),
            lit("order_id tiene formato invalido"),
        )
        .when(
            ~col("vehicle_id").rlike(r"^VEH-[0-9]{2}$"),
            lit("vehicle_id tiene formato invalido"),
        )
        .when(
            ~col("product_type").isin("refrigerado", "congelado"),
            lit("product_type no permitido"),
        )
        .when(
            col("event_time").isNull(),
            lit("event_ts no es una fecha valida"),
        )
        .when(
            ~col("temperature_c").between(-40.0, 60.0),
            lit("temperature_c fuera de los limites del contrato"),
        )
        .when(
            ~col("humidity_pct").between(0.0, 100.0),
            lit("humidity_pct fuera de los limites del contrato"),
        )
    )

    validated = decoded.withColumn(
        "validation_error", validation_error
    )

    valid_events = validated.filter(
        col("validation_error").isNull()
    )

    rejected_events = validated.filter(
        col("validation_error").isNotNull()
    ).select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "raw_value",
        "validation_error",
    )

    # 5. Guardar eventos rechazados para facilitar su inspección.
    rejected_query = (
        rejected_events.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", REJECTED_PATH)
        .option("checkpointLocation", f"{CHECKPOINTS}/rejected")
        .start()
    )

    # 6. Capa Processed: lecturas válidas y temperaturas clasificadas.
    processed = clasificar_lecturas(valid_events)

    processed_query = (
        processed.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", PROCESSED_PATH)
        .option("checkpointLocation", f"{CHECKPOINTS}/processed")
        .partitionBy("event_date")
        .start()
    )

    # 7. Capa Curated: alertas por lectura fuera de rango.
    alerts = (
        processed
        .filter(col("temperature_status") != "ok")
        .withColumn(
            "alert_type",
            when(
                col("is_critical"),
                lit("TEMPERATURA_CRITICA"),
            ).otherwise(lit("FUERA_RANGO")),
        )
        .select(
            "event_id",
            "order_id",
            "vehicle_id",
            "product_type",
            "temperature_c",
            "event_time",
            "temperature_status",
            "alert_type",
            "event_date",
        )
    )

    alerts_query = (
        alerts.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", ALERTS_PATH)
        .option("checkpointLocation", f"{CHECKPOINTS}/alerts")
        .partitionBy("event_date")
        .start()
    )

    # 8. KPI: agregaciones por ventanas de cinco minutos.
    # El watermark permite cerrar ventanas de tiempo de evento.
    kpis = (
        processed
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            window(col("event_time"), "5 minutes").alias("window"),
            col("product_type"),
        )
        .agg(
            avg("temperature_c").alias("temperatura_promedio_c"),
            count(lit(1)).alias("lecturas_total"),
            spark_sum(
                when(col("is_out_of_range"), 1).otherwise(0)
            ).alias("lecturas_fuera_rango"),
            spark_sum(
                when(col("is_critical"), 1).otherwise(0)
            ).alias("lecturas_criticas"),
        )
        .withColumn(
            "porcentaje_fuera_rango",
            col("lecturas_fuera_rango").cast("double")
            / col("lecturas_total") * lit(100.0),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "product_type",
            "temperatura_promedio_c",
            "lecturas_total",
            "lecturas_fuera_rango",
            "lecturas_criticas",
            "porcentaje_fuera_rango",
        )
    )

    kpis_query = (
        kpis.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", KPIS_PATH)
        .option("checkpointLocation", f"{CHECKPOINTS}/kpis")
        .start()
    )

    print(f"Kafka: {KAFKA_BOOTSTRAP} | Topic: {KAFKA_TOPIC}")
    print("Consultas activas: Raw, Processed, rechazados, alertas y KPIs.")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()