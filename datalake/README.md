# Data Lake local – Cadena de frío

Estructura por capas (se llenará desde la Semana 2):

| Capa | Ruta | Contenido |
|------|------|-----------|
| Raw | `datalake/raw/` | Eventos tal como llegan de Kafka, sin transformar |
| Processed | `datalake/processed/` | Eventos parseados, tipados y validados (Parquet) |
| Curated | `datalake/curated/` | KPIs y alertas listos para consumo (Parquet) |
| Checkpoints | `datalake/checkpoints/` | Estado de Spark Structured Streaming |

Los datos generados no se versionan (ver `.gitignore`).
