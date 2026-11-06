# Checkpoint Semana 3 – Grupo 1: Cadena de frío

## 1. Avance realizado

Durante la Semana 3 se implementó la orquestación del proyecto RapidGo mediante Apache Airflow.

Se creó el DAG `rapidgo_orquestacion`, encargado de verificar componentes fundamentales del pipeline de procesamiento de la cadena de frío. El DAG contiene dos tareas ejecutadas de forma secuencial:

```text
verificar_kafka >> verificar_datalake
```

Las tareas implementadas son:

- `verificar_kafka`: comprueba que Kafka esté disponible mediante una conexión al servicio en `kafka:9092`.
- `verificar_datalake`: valida la existencia de los resultados almacenados en el Data Lake y comprueba que existan archivos Parquet generados por el procesamiento.

El DAG fue reconocido correctamente por Airflow y no presentó errores de importación.

---

## 2. Evidencia de ejecución exitosa

Apache Airflow cargó correctamente el DAG `rapidgo_orquestacion`.

La ejecución manual del DAG finalizó correctamente y las dos tareas quedaron en estado exitoso:

| Tarea | Resultado |
|---|---|
| `verificar_kafka` | Success |
| `verificar_datalake` | Success |

La ejecución confirmó que Kafka estaba disponible y que el Data Lake contenía archivos Parquet generados por el pipeline.

Durante la verificación del Data Lake se confirmó la existencia de resultados en las siguientes zonas:

- `datalake/raw/events`
- `datalake/processed/readings`
- `datalake/processed/rejected`
- `datalake/curated/alerts`
- `datalake/curated/kpis`

La evidencia visual de Airflow muestra las tareas `verificar_kafka` y `verificar_datalake` completadas correctamente.

---

## 3. Prueba de falla y recuperación

Para validar el comportamiento del DAG ante una dependencia no disponible, se realizó una prueba controlada sobre la estructura del Data Lake.

La carpeta utilizada por la tarea `verificar_datalake` es:

```text
datalake/processed/readings
```

Durante la prueba, esta carpeta fue renombrada temporalmente a:

```text
datalake/processed/readings_respaldo
```

La tarea afectada por esta modificación fue:

```text
verificar_datalake
```

El objetivo de la prueba fue comprobar que la validación del Data Lake detectara la ausencia de la ruta esperada.

La tarea anterior del flujo, `verificar_kafka`, no depende de esta ruta y puede completarse de forma independiente. Debido a la secuencia definida en el DAG, la validación del Data Lake se ejecuta después de la comprobación de Kafka.

---

## 4. Recuperación del flujo

Después de la prueba controlada, la estructura original del Data Lake fue restaurada mediante el cambio de nombre de la carpeta:

```text
readings_respaldo -> readings
```

Con la ruta original disponible nuevamente:

```text
datalake/processed/readings
```

los archivos Parquet quedaron nuevamente accesibles para la validación.

Posteriormente, el DAG `rapidgo_orquestacion` se ejecutó correctamente, con las dos tareas en estado exitoso:

```text
verificar_kafka       -> Success
verificar_datalake    -> Success
```

Esto permitió comprobar la recuperación del flujo después de restaurar la dependencia requerida por la tarea de validación.

---

## 5. Tareas afectadas

Durante la prueba de disponibilidad del Data Lake se identificó la siguiente relación entre las tareas:

| Tarea | Dependencia principal | Resultado esperado |
|---|---|---|
| `verificar_kafka` | Disponibilidad de Kafka | Success |
| `verificar_datalake` | Ruta y archivos Parquet del Data Lake | Failed si la ruta no existe / Success después de la recuperación |

La dependencia entre las tareas está definida de la siguiente manera:

```text
verificar_kafka >> verificar_datalake
```

Por esta razón, la verificación del Data Lake forma parte de un flujo ordenado en el que primero se comprueba la disponibilidad del servicio Kafka.

---

## 6. Cambio que resolvió el problema

El cambio aplicado para recuperar el flujo consistió en restaurar el nombre original de la carpeta requerida por la tarea `verificar_datalake`.

La estructura pasó nuevamente de:

```text
datalake/processed/readings_respaldo
```

a:

```text
datalake/processed/readings
```

Con la ruta restaurada y los archivos Parquet disponibles, la validación del Data Lake pudo ejecutarse nuevamente de forma correcta.

---

## 7. Resultado de la Semana 3

La implementación de Apache Airflow incorporó una capa de orquestación al proyecto RapidGo.

El DAG `rapidgo_orquestacion` permite verificar dos componentes importantes del pipeline:

1. La disponibilidad de Kafka.
2. La existencia de resultados almacenados en el Data Lake.

La ejecución realizada confirmó que el DAG puede ser cargado y ejecutado correctamente en Apache Airflow. Además, la prueba controlada sobre la disponibilidad de la ruta del Data Lake permitió validar el proceso de recuperación mediante la restauración de la estructura requerida.

Con esta implementación, el proyecto cuenta con un mecanismo inicial de orquestación y verificación para el pipeline de la cadena de frío.
