# ADR-003: Orquestación y validación del pipeline con Apache Airflow

- **Estado:** Aceptado
- **Fecha:** 2026-11-03
- **Decisión:** Utilizar Apache Airflow para orquestar las validaciones del pipeline de Cadena de frío.

## Contexto

Durante las semanas anteriores se implementó el pipeline de procesamiento de RapidGo utilizando un productor de eventos, Apache Kafka, Spark Structured Streaming y un Data Lake con resultados almacenados en formato Parquet.

En la Semana 3 se incorporó Apache Airflow para organizar y ejecutar tareas de validación sobre componentes importantes del pipeline.

El DAG implementado se denomina:

`rapidgo_orquestacion`

El flujo contiene las siguientes tareas:

1. `verificar_kafka`: comprueba que Kafka acepte conexiones desde el entorno de Airflow.
2. `verificar_datalake`: comprueba la existencia de las rutas de datos y de archivos Parquet generados por el procesamiento.

Las tareas se ejecutan de forma secuencial:

```text
verificar_kafka
        |
        v
verificar_datalake
```

Esta dependencia evita ejecutar la validación del Data Lake antes de comprobar la disponibilidad del servicio de mensajería.

## Decisión

Se decidió utilizar un DAG de Apache Airflow con tareas de validación independientes y dependientes entre sí.

La primera tarea verifica la disponibilidad de Kafka mediante una conexión al host y puerto configurados.

La segunda tarea verifica que las zonas `raw/events` y `processed/readings` del Data Lake existan y contengan archivos Parquet.

La dependencia entre las tareas se define de la siguiente manera:

```python
tarea_verificar_kafka >> tarea_verificar_datalake
```

Además, el DAG se configuró con reintentos para permitir la recuperación ante fallos temporales:

- Dos reintentos.
- Un minuto de espera entre cada intento.

## Evidencia observada

Durante la ejecución del entorno se comprobó que Apache Airflow detectó correctamente el DAG:

`rapidgo_orquestacion`

La consulta de los DAG disponibles mostró el archivo:

`/opt/airflow/dags/rapidgo_orquestacion.py`

También se verificó que no existían errores de importación del DAG.

Durante las validaciones se comprobó la presencia de archivos Parquet generados previamente por el pipeline en las zonas:

- `datalake/raw/events`
- `datalake/processed/readings`

La ejecución permitió comprobar que la orquestación puede utilizar validaciones independientes para identificar problemas en los componentes del pipeline.

## Problema y recuperación

Durante las pruebas de recuperación se simuló la ausencia temporal de la ruta de lecturas procesadas del Data Lake.

Esta situación provoca que la tarea `verificar_datalake` no encuentre la ruta o los archivos Parquet requeridos y finalice con un error.

La tarea afectada es:

`verificar_datalake`

La tarea `verificar_kafka` no resulta afectada, ya que corresponde a una validación independiente que se ejecuta antes de la comprobación del Data Lake.

Después de restaurar la ruta y los archivos requeridos, la validación puede ejecutarse nuevamente.

Este comportamiento permite identificar de forma clara qué componente presenta el problema y facilita la recuperación sin modificar las demás tareas del DAG.

## Consecuencias

### Positivas

- Se establece un flujo ordenado para validar componentes del pipeline.
- Los fallos pueden identificarse por tarea dentro de Airflow.
- La separación entre validaciones facilita el diagnóstico.
- Los reintentos permiten manejar fallos temporales.
- La ejecución del DAG proporciona evidencia de la orquestación del pipeline.

### Limitaciones

- El DAG actual valida la disponibilidad de Kafka y la existencia de resultados Parquet, pero no reemplaza el procesamiento realizado por Spark Structured Streaming.
- La existencia de archivos Parquet no garantiza por sí sola la calidad o exactitud de los datos.
- La orquestación deberá ampliarse en futuras versiones para incluir controles de calidad, monitoreo y otras etapas del pipeline.

## Relación con decisiones anteriores

Esta decisión complementa las decisiones técnicas documentadas anteriormente:

- **ADR-001:** define `order_id` como clave de los mensajes Kafka y establece el uso de tres particiones.
- **ADR-002:** establece la separación de las salidas del pipeline por zonas del Data Lake y el uso de checkpoints para las consultas de streaming.
- **ADR-003:** incorpora la orquestación y validación de componentes del pipeline mediante Apache Airflow.

## Estado final

La decisión queda aceptada como parte de la implementación de la Semana 3.

El DAG `rapidgo_orquestacion` constituye la base inicial para la orquestación del pipeline de Cadena de frío de RapidGo.
