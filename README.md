# RapidGo – Cadena de frío (Grupo 1)

Proyecto final de aula · Línea de Profundización I – Big Data · Ingeniería de Sistemas y Computación, Facatativá.
Docente: Ing. Oscar Jobany Gomez Ochoa.

* **Variante:** 3 – Cadena de frío
* **Equipo (Grupo 1):** Brayan Ramírez, Sebastián Bernal, Steban Blanco
* **Estado:** Semana 1 – `v0.1.0-foundation`

## Problema de negocio

RapidGo transporta productos sensibles (refrigerados y congelados) que pueden salir de las condiciones aceptables de temperatura y humedad durante el reparto. Sin monitoreo, estas excursiones se detectan tarde o nunca. Este proyecto construye un pipeline que captura lecturas de sensores, calcula indicadores y genera alertas para actuar a tiempo.

## Arquitectura

```
Generador de lecturas (producer) -> Kafka (rapidgo.coldchain.readings)
   -> Spark Structured Streaming -> Data Lake (Raw -> Processed -> Curated)
   -> Airflow (calidad -> agregación -> reporte) -> KPIs / alertas
Transversal: Docker + Git + GitHub + Kanban + GitHub Actions + Releases
```

Diagrama: `docs/arquitectura.png`

## Evento

Contrato en `samples/evento\_lectura.schema.json`, ejemplo en `samples/evento\_lectura\_ejemplo.json`.
Campos: `event\_id`, `team\_id`, `order\_id`, `vehicle\_id`, `product\_type`, `temperature\_c`, `humidity\_pct`, `event\_ts`.

## Kafka

* Topic: `rapidgo.coldchain.readings`
* Key: `order\_id` · Particiones: 3 · Replicación: 1 (ver `docs/decisions/ADR-001-key-y-particiones-kafka.md`)

## KPIs (mínimo 3)

|KPI|Definición|
|-|-|
|Temperatura media|Promedio de `temperature\_c` por vehículo en ventana de 5 minutos|
|Eventos fuera de rango|Cantidad y % de lecturas fuera del rango del tipo de producto|
|Duración de exposición|Minutos continuos que un pedido permanece fuera de rango|

## Reglas de alerta (umbrales iniciales, se ajustarán con datos reales)

|Producto|Rango aceptable|Temperatura crítica|
|-|-|-|
|Refrigerado|2 °C a 8 °C|< -1 °C o > 11 °C (3 °C fuera del rango)|
|Congelado|-25 °C a -15 °C|> -12 °C (3 °C sobre el límite)|

* **Temperatura crítica:** lectura que supera el umbral crítico de su tipo de producto.
* **Exposición prolongada:** pedido con lecturas fuera de rango durante 10 minutos seguidos o más.

La humedad aceptable inicial es 30 %–80 % y se registra, pero en esta etapa no genera alerta.

## Ejecución local (preliminar)

```bash
docker compose up -d
docker compose logs kafka-init
```

## Tecnologías y rol

Kafka (transporte), Spark Structured Streaming (procesamiento), Parquet/Data Lake (persistencia), Airflow (orquestación), Docker (entorno), GitHub Actions (CI).

## Contribuciones

|Integrante|Responsabilidades|Issues / PR principales|
|-|-|-|
|Brayan Ramírez|*por completar*|*por completar*|
|Sebastián Bernal|*por completar*|*por completar*|
|Steban Blanco|*por completar*|*por completar*|



