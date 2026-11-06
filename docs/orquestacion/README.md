# Documentación de Orquestación – RapidGo

**Proyecto:** RapidGo – Cadena de frío  
**Componente:** Orquestación con Apache Airflow  
**Responsable:** Brayan Ramírez  
**Versión:** Semana 2 – Entrega #10

## 1. Introducción

Este componente implementa Apache Airflow como herramienta de orquestación y supervisión del proyecto RapidGo, cuyo objetivo es procesar y monitorear información de telemetría relacionada con la cadena de frío.

Airflow permite verificar la disponibilidad de Kafka y validar que los datos procesados por Spark se encuentren almacenados correctamente en el Data Lake.

## 2. Arquitectura implementada

La solución integra los siguientes componentes:

- **Apache Kafka:** recibe los eventos de telemetría.
- **Apache Spark Structured Streaming:** procesa los eventos y genera los resultados.
- **Data Lake:** almacena los datos en formato Parquet, organizados en RAW y PROCESSED.
- **Apache Airflow:** ejecuta verificaciones sobre los servicios y los resultados almacenados.

Airflow funciona como componente de supervisión y validación. No reemplaza ni duplica el procesamiento continuo realizado por Spark.

## 3. Configuración de Docker Compose

Se incorporó el servicio de Airflow utilizando la imagen:

`apache/airflow:2.10.5-python3.11`

Configuración principal:

- Puerto de acceso: 8080.
- Modo de ejecución: standalone.
- DAGs montados desde `./airflow/dags`.
- Data Lake montado en modo de solo lectura.
- Dependencia declarada respecto al servicio Kafka.

## 4. DAG de orquestación

Archivo implementado:

`airflow/dags/rapidgo_orquestacion.py`

Identificador del DAG:

`rapidgo_orquestacion`

El DAG contiene dos tareas principales:

### 4.1 verificar_kafka

Comprueba que Kafka acepte conexiones TCP desde el contenedor de Airflow mediante el host `kafka` y el puerto `9092`.

Si la conexión falla, la tarea genera un error y Airflow puede ejecutar los reintentos configurados.

### 4.2 verificar_datalake

Comprueba la existencia de las siguientes rutas:

- RAW: `/opt/rapidgo/datalake/raw/events`
- PROCESSED: `/opt/rapidgo/datalake/processed/readings`

Posteriormente, busca archivos `part-*.parquet` en cada directorio.

Si alguna ruta no existe o no contiene archivos Parquet, la tarea genera un error.

### 4.3 Dependencias

Las tareas se ejecutan secuencialmente:

`verificar_kafka >> verificar_datalake`

Esto permite comprobar primero la disponibilidad de Kafka y posteriormente validar los resultados almacenados.

## 5. Parámetros del DAG

| Parámetro | Configuración |
|---|---|
| DAG ID | rapidgo_orquestacion |
| Schedule | None (manual) |
| Catchup | False |
| Reintentos | 2 |
| Tiempo entre reintentos | 1 minuto |
| Propietario | rapidgo |

## 6. Ejecución del sistema

Para iniciar los servicios:

```bash
docker compose up -d
```

Para comprobar su estado:

```bash
docker compose ps
```

Para consultar los DAG disponibles:

```bash
docker exec rapidgo-airflow airflow dags list
```

Para consultar las ejecuciones:

```bash
docker exec rapidgo-airflow airflow dags list-runs -d rapidgo_orquestacion
```

La interfaz web de Airflow está disponible en:

`http://localhost:8080`

## 7. Validación y resultados

Durante las pruebas se verificó que los siguientes servicios estuvieran activos:

- Kafka.
- Spark Streaming.
- Airflow.

Se ejecutó manualmente el DAG `rapidgo_orquestacion` y se obtuvo el estado `success`.

La ejecución registrada el 3 de noviembre de 2026 presentó los siguientes resultados:

- DAG: `rapidgo_orquestacion`.
- Estado: SUCCESS.
- Identificador: `manual__2026-11-03T19:49:03.326911+00:00`.
- Inicio: `2026-11-03T19:49:04.891078+00:00`.
- Finalización: `2026-11-03T19:49:17.614639+00:00`.

Esto demuestra que la ejecución completó satisfactoriamente las tareas de verificación configuradas.

## 8. Evidencias

<img width="1895" height="858" alt="evidencia_airflow_dag_exitoso" src="https://github.com/user-attachments/assets/2ce1d916-8480-48a4-90a6-03ec3b6f7aaa" />

## 9. Conclusiones

La implementación de Apache Airflow permite incorporar una capa de orquestación y supervisión al proyecto RapidGo.

Se logró integrar el servicio mediante Docker Compose, desarrollar un DAG con tareas de validación, configurar dependencias y reintentos, y comprobar una ejecución satisfactoria.

Con esta implementación se facilita la supervisión de los componentes principales de la arquitectura y la validación de los resultados almacenados en el Data Lake.
