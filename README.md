# RapidGo – Pipeline Big Data End-to-End: Cadena de frío

**Proyecto final de aula – Línea de Profundización I: Big Data**  
**Variante 3: Cadena de frío**  
**Release de Semana 1:** `v0.1.0-foundation`  
**Release de Semana 2:** `v0.2.0-streaming`  
**Release de Semana 3:** `v0.3.0-orchestration`

---

## 1. Descripción del proyecto

RapidGo es una plataforma de entregas que necesita monitorear las condiciones de transporte de productos refrigerados y congelados. Durante el traslado, las variaciones de temperatura pueden afectar la calidad de los productos y generar pérdidas económicas.

Este proyecto implementa progresivamente una arquitectura Big Data para capturar lecturas de temperatura y humedad, transportarlas mediante Apache Kafka, procesarlas con Apache Spark Structured Streaming y almacenarlas en un Data Lake.

Durante las primeras tres semanas se ha avanzado en el diseño de la arquitectura, la implementación del productor de eventos, la integración del procesamiento en streaming, la persistencia de resultados en formato Parquet y la incorporación de Apache Airflow para la orquestación y validación básica del pipeline.

Los indicadores y las alertas permiten identificar posibles incumplimientos de las condiciones de la cadena de frío.

## 2. Objetivos

### Objetivo general

Diseñar e implementar progresivamente un pipeline Big Data para monitorear la cadena de frío de RapidGo mediante eventos de sensores, procesamiento distribuido, almacenamiento, indicadores operacionales y orquestación.

### Objetivos específicos

- Definir un contrato JSON para las lecturas de temperatura y humedad.
- Utilizar Kafka para transportar eventos de monitoreo.
- Procesar eventos mediante Spark Structured Streaming.
- Definir indicadores y reglas de alerta para detectar condiciones fuera de rango.
- Organizar el almacenamiento de datos en un Data Lake.
- Utilizar Apache Airflow para la orquestación y validación de componentes.
- Mantener trazabilidad del trabajo mediante GitHub, Issues, Pull Requests y releases.

## 3. Equipo de trabajo

| Integrante | Responsabilidades |
|---|---|
| Brayan Ramírez | Coordinación del repositorio, Docker Compose, documentación de ejecución y orquestación con Airflow. |
| Sebastián Bernal | Contrato JSON, eventos, KPIs y reglas de alerta. |
| Steban Blanco | Arquitectura, configuración de Kafka y documentación técnica. |

La participación del equipo se evidencia mediante las Issues, los commits y los Pull Requests correspondientes.

## 4. Problema de negocio

Durante el transporte de productos refrigerados o congelados, es necesario mantener la temperatura dentro de rangos adecuados. Una temperatura excesiva o demasiado baja puede comprometer la conservación de la mercancía.

La solución permite registrar las lecturas de los sensores, detectar desviaciones, calcular indicadores por ventanas de tiempo y conservar los resultados para su consulta y análisis.

### Valor esperado

- Identificar condiciones anormales durante el transporte.
- Medir la frecuencia de las lecturas fuera de rango.
- Detectar exposiciones prolongadas a temperaturas inadecuadas.
- Proporcionar datos para mejorar el control operacional y la trazabilidad.

## 5. Arquitectura del proyecto

La arquitectura de la variante Cadena de frío se organiza en las siguientes etapas:

1. **Producer:** genera eventos simulados de sensores asociados a pedidos y vehículos.
2. **Apache Kafka:** recibe y distribuye los eventos mediante el topic `rapidgo.coldchain.readings`.
3. **Apache Spark Structured Streaming:** consume los eventos, procesa las lecturas y genera resultados para el Data Lake.
4. **Data Lake:** almacena los eventos y resultados en diferentes zonas de procesamiento.
5. **Apache Airflow:** realiza la orquestación y validación básica de componentes del pipeline.
6. **Indicadores y alertas:** permiten identificar condiciones críticas y analizar el comportamiento de la cadena de frío.

### Diagrama de arquitectura

![Arquitectura de RapidGo - Cadena de frío](docs/arquitectura.png)

La arquitectura se ha implementado progresivamente durante el desarrollo del proyecto. Kafka, Spark Structured Streaming y el Data Lake fueron integrados durante la Semana 2, mientras que la orquestación básica mediante Airflow fue incorporada durante la Semana 3.

## 6. Tecnologías utilizadas

| Tecnología | Función |
|---|---|
| Python | Generación, validación y procesamiento de eventos. |
| Apache Kafka | Transporte de eventos mediante un topic particionado. |
| Apache Spark Structured Streaming | Procesamiento de eventos en flujo. |
| Parquet | Almacenamiento de los resultados procesados. |
| Data Lake | Organización de datos Raw, Processed y Curated. |
| Apache Airflow | Orquestación y validación de componentes del pipeline. |
| Docker Compose | Ejecución local de los servicios. |
| Git y GitHub | Control de versiones y colaboración. |
| GitHub Actions | Integración continua y ejecución de validaciones. |
| Pytest | Ejecución de pruebas automatizadas. |

## 7. Contrato del evento

Los eventos representan lecturas de sensores relacionadas con un pedido y un vehículo.

El contrato y el ejemplo del evento se encuentran en:

- Esquema: [`samples/evento_lectura.schema.json`](samples/evento_lectura.schema.json)
- Ejemplo: [`samples/evento_lectura_ejemplo.json`](samples/evento_lectura_ejemplo.json)

El contrato incluye campos obligatorios para identificar el evento, relacionarlo con el pedido y el vehículo, registrar la fecha y hora de la lectura e incluir las mediciones de temperatura y humedad.

Entre los campos definidos se encuentran:

- `event_id`
- `team_id`
- `order_id`
- `vehicle_id`
- `product_type`
- `temperature_c`
- `humidity_pct`
- `event_ts`

El archivo del esquema JSON es la referencia definitiva para la validación de los eventos.

## 8. Kafka: topic, key y particiones

| Parámetro | Configuración |
|---|---|
| Topic | `rapidgo.coldchain.readings` |
| Key | `order_id` |
| Número de particiones | 3 |
| Factor de replicación local | 1 |

### Justificación de la decisión

Se utiliza `order_id` como key para que las lecturas de un mismo pedido se asignen a una misma partición, conservando el orden de los eventos dentro de ella.

Las tres particiones permiten distribuir el procesamiento entre diferentes pedidos. El factor de replicación 1 corresponde al entorno local de desarrollo y no proporciona tolerancia ante la pérdida de un broker.

La decisión está documentada en:

[`docs/decisions/ADR-001-key-y-particiones-kafka.md`](docs/decisions/ADR-001-key-y-particiones-kafka.md)

### Evidencias de ejecución

Durante las pruebas del productor se verificó la publicación de eventos en el tópico `rapidgo.coldchain.readings`.

En una ejecución se enviaron 10 eventos y se observó la siguiente distribución:

- Partición 0: 1 mensaje.
- Partición 1: 2 mensajes.
- Partición 2: 7 mensajes.

En una ejecución posterior se enviaron 20 eventos y se observó:

- Partición 0: 3 mensajes.
- Partición 1: 3 mensajes.
- Partición 2: 14 mensajes.

La distribución puede variar entre ejecuciones. Estas pruebas permiten comprobar la publicación y recepción de los eventos, aunque no garantizan una distribución uniforme entre las particiones.

## 9. Procesamiento con Spark Structured Streaming

Durante la Semana 2 se integró Apache Spark Structured Streaming para consumir y procesar los eventos publicados en Kafka.

El procesamiento utiliza el tópico:

`rapidgo.coldchain.readings`

Las consultas de streaming generan salidas para diferentes propósitos:

- Eventos crudos.
- Lecturas procesadas.
- Registros rechazados.
- Alertas.
- Indicadores KPI.

La separación de estas salidas permite organizar los resultados según su propósito y facilitar la inspección del procesamiento.

## 10. Data Lake

Los resultados generados por el pipeline se almacenan en formato Parquet.

Las principales ubicaciones utilizadas son:

| Ubicación | Propósito |
|---|---|
| `datalake/raw/events` | Eventos crudos recibidos desde Kafka. |
| `datalake/processed/readings` | Lecturas procesadas y validadas. |
| `datalake/processed/rejected` | Registros rechazados durante las validaciones. |
| `datalake/curated/alerts` | Registros relacionados con alertas. |
| `datalake/curated/kpis` | Indicadores y resultados agregados. |

Durante la ejecución del pipeline se verificó la existencia de archivos Parquet en estas zonas del Data Lake.

### Checkpoints

Spark Structured Streaming utiliza directorios de checkpoints asociados a las consultas de streaming.

Los checkpoints permiten conservar información de progreso y estado para facilitar la recuperación de las consultas, siempre que se mantengan las condiciones de compatibilidad requeridas por Spark.

La decisión relacionada con la separación de las zonas del Data Lake y el uso de checkpoints específicos se documentó en:

[`docs/decisions/ADR-002-checkpoints-y-zonas-datalake.md`](docs/decisions/ADR-002-checkpoints-y-zonas-datalake.md)

## 11. KPIs y alertas

El proyecto contempla indicadores para el monitoreo de la cadena de frío.

| KPI | Descripción | Unidad o ventana |
|---|---|---|
| Temperatura promedio | Promedio de temperatura de las lecturas recibidas. | °C por ventana. |
| Lecturas fuera de rango | Cantidad o porcentaje de lecturas que incumplen el rango definido. | Número y porcentaje. |
| Duración de exposición fuera de rango | Tiempo durante el cual un pedido permanece fuera del rango permitido. | Minutos. |

### Temperatura crítica

Se definieron criterios iniciales para identificar condiciones críticas según el tipo de producto.

Para productos refrigerados, se consideran como referencia temperaturas entre `2 °C` y `8 °C`.

Para productos congelados, se consideran como referencia temperaturas entre `-25 °C` y `-15 °C`.

### Exposición prolongada

El proyecto contempla la generación de alertas cuando la temperatura permanezca fuera del rango permitido durante un periodo prolongado.

La evaluación debe considerar el tipo de producto y la secuencia temporal de las lecturas.

## 12. Orquestación con Apache Airflow

Durante la Semana 3 se incorporó Apache Airflow para realizar la orquestación y validación básica de componentes del pipeline.

Se implementó el DAG:

`rapidgo_orquestacion`

El archivo se encuentra en:

`airflow/dags/rapidgo_orquestacion.py`

### Tareas del DAG

El DAG contiene las siguientes tareas:

1. **`verificar_kafka`**  
   Comprueba que el servicio Kafka acepta conexiones desde el entorno de Airflow.

2. **`verificar_datalake`**  
   Comprueba la existencia de las rutas del Data Lake y verifica la presencia de archivos Parquet generados por el procesamiento.

El flujo definido es:

`verificar_kafka → verificar_datalake`

Durante las pruebas se verificó que Apache Airflow detectó correctamente el DAG y permitió su ejecución.

### Fallo y recuperación

Como parte de la validación de la Semana 3 se realizó una prueba de fallo controlado relacionada con la disponibilidad de las lecturas procesadas en el Data Lake.

La condición provocó el fallo de la tarea encargada de verificar los datos procesados. Posteriormente, se restauró la condición necesaria y se comprobó la recuperación del flujo.

Esta prueba permitió documentar el comportamiento del proceso ante una condición de error y su posterior recuperación.

La evidencia detallada se encuentra en:

[`docs/checkpoints/semana3.md`](docs/checkpoints/semana3.md)

La decisión técnica relacionada con la estrategia de orquestación y validación se documentó en:

[`docs/decisions/ADR-003-orquestacion-y-validacion-pipeline.md`](docs/decisions/ADR-003-orquestacion-y-validacion-pipeline.md)

## 13. Estructura del repositorio

```text
rapidgo-cadena-frio/
├── .github/
│   └── workflows/
├── airflow/
│   └── dags/
│       └── rapidgo_orquestacion.py
├── datalake/
│   ├── raw/
│   ├── processed/
│   └── curated/
├── docs/
│   ├── arquitectura.png
│   ├── checkpoints/
│   │   ├── semana1.md
│   │   ├── semana2.md
│   │   └── semana3.md
│   └── decisions/
│       ├── ADR-001-key-y-particiones-kafka.md
│       ├── ADR-002-checkpoints-y-zonas-datalake.md
│       └── ADR-003-orquestacion-y-validacion-pipeline.md
├── producer/
├── samples/
│   ├── evento_lectura.schema.json
│   └── evento_lectura_ejemplo.json
├── spark/
├── tests/
├── .gitignore
├── docker-compose.yml
├── pytest.ini
├── README.md
└── requirements.txt
```

## 14. Requisitos previos

Para ejecutar el proyecto localmente se requiere:

- Git.
- Docker Desktop con Docker Compose.
- Python.
- Conexión a Internet para descargar las imágenes de los servicios la primera vez.

Las dependencias de Python del proyecto se encuentran en:

`requirements.txt`

## 15. Ejecución local

Desde la raíz del repositorio se puede validar la configuración de Docker Compose mediante:

```powershell
docker compose config --quiet
```

Para iniciar los servicios:

```powershell
docker compose up -d
```

Para consultar el estado de los contenedores:

```powershell
docker compose ps
```

Los servicios del proyecto incluyen los componentes necesarios para Kafka, Spark Structured Streaming y Airflow.

### Verificar el tópico Kafka

Para consultar la configuración del tópico:

```powershell
docker exec coldchain-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic rapidgo.coldchain.readings
```

### Ejecutar las pruebas

Las pruebas automatizadas se pueden ejecutar con:

```powershell
python -m pytest -v
```

Durante una de las ejecuciones registradas se obtuvieron 51 pruebas aprobadas.

### Consultar Airflow

Después de iniciar los servicios, Airflow permite verificar la detección del DAG `rapidgo_orquestacion`.

También es posible comprobar los DAG disponibles desde el contenedor mediante:

```powershell
docker compose exec airflow airflow dags list
```

Para consultar errores de importación:

```powershell
docker compose exec airflow airflow dags list-import-errors
```

### Detener los servicios

Para detener y eliminar los contenedores:

```powershell
docker compose down
```

Este comando detiene los servicios y elimina la red creada por Docker Compose. Los archivos locales del repositorio no se eliminan automáticamente.

## 16. Pruebas e integración continua

El proyecto cuenta con pruebas automatizadas para validar componentes del productor y el contrato de eventos.

Las validaciones incluyen pruebas relacionadas con:

- El contrato JSON.
- Campos obligatorios.
- Valores inválidos.
- Campos adicionales.
- Generación de eventos.

GitHub Actions se utiliza para ejecutar validaciones de integración continua en los Pull Requests.

Durante el desarrollo de las Semanas 2 y 3 se verificaron ejecuciones exitosas de la integración continua.

## 17. Decisiones técnicas

Las principales decisiones arquitectónicas del proyecto se documentan mediante Architecture Decision Records (ADR).

### ADR-001: key y particiones de Kafka

Define el uso de `order_id` como clave de los mensajes Kafka y la configuración inicial de tres particiones.

Archivo:

`docs/decisions/ADR-001-key-y-particiones-kafka.md`

### ADR-002: checkpoints y zonas del Data Lake

Documenta la separación de las salidas del pipeline según su propósito y el uso de checkpoints específicos para las consultas de Spark Structured Streaming.

Archivo:

`docs/decisions/ADR-002-checkpoints-y-zonas-datalake.md`

### ADR-003: orquestación y validación del pipeline

Documenta la incorporación de Apache Airflow para validar la disponibilidad de componentes y verificar la existencia de resultados del procesamiento.

Archivo:

`docs/decisions/ADR-003-orquestacion-y-validacion-pipeline.md`

## 18. GitHub Project y planificación

El trabajo se organiza mediante Issues y Pull Requests para mantener la trazabilidad de las actividades realizadas.

El proyecto se desarrolla progresivamente por semanas:

- **Semana 1:** diseño, arquitectura, contrato, KPIs, alertas y organización.
- **Semana 2:** productor, Kafka, Spark Structured Streaming, Data Lake y CI.
- **Semana 3:** orquestación con Airflow, validación y recuperación ante fallos.
- **Semana 4:** integración, validación final y publicación de la versión estable.

## 19. Documentación y entregas

| Entrega | Release | Alcance |
|---|---|---|
| Semana 1 | `v0.1.0-foundation` | Diseño, arquitectura y organización. |
| Semana 2 | `v0.2.0-streaming` | Pipeline Kafka, Spark Structured Streaming y Data Lake. |
| Semana 3 | `v0.3.0-orchestration` | Orquestación con Airflow, validación y recuperación ante fallos. |
| Semana 4 | `v1.0.0` | Integración y validación final. |

### Documentos principales

- [Checkpoint de Semana 1](docs/checkpoints/semana1.md)
- [Checkpoint de Semana 2](docs/checkpoints/semana2.md)
- [Checkpoint de Semana 3](docs/checkpoints/semana3.md)
- [ADR-001: key y particiones de Kafka](docs/decisions/ADR-001-key-y-particiones-kafka.md)
- [ADR-002: checkpoints y zonas del Data Lake](docs/decisions/ADR-002-checkpoints-y-zonas-datalake.md)
- [ADR-003: orquestación y validación del pipeline](docs/decisions/ADR-003-orquestacion-y-validacion-pipeline.md)
- [Esquema JSON](samples/evento_lectura.schema.json)
- [Ejemplo de evento](samples/evento_lectura_ejemplo.json)
- [Diagrama de arquitectura](docs/arquitectura.png)

## 20. Estado actual y trabajo futuro

Hasta la Semana 3, el proyecto cuenta con una arquitectura integrada que incluye:

- Generación y validación de eventos.
- Publicación de eventos mediante Kafka.
- Procesamiento mediante Spark Structured Streaming.
- Persistencia de resultados en formato Parquet.
- Organización de resultados en el Data Lake.
- Pruebas automatizadas.
- Integración continua mediante GitHub Actions.
- Orquestación y validación básica mediante Apache Airflow.
- Evidencia de una condición de fallo y recuperación posterior.

El trabajo restante contempla la integración y validación final de todos los componentes, la revisión de los resultados funcionales del pipeline y la preparación de la versión estable del proyecto.

---

**Repositorio:** https://github.com/luiguicrack/rapidgo-cadena-frio

**Variante:** 3 – Cadena de frío

**Último release documentado:** `v0.3.0-orchestration`
