# Checkpoint Semana 2 – Grupo 1: Cadena de frío

## 1. Avance realizado

Durante la Semana 2 se avanzó en la integración de los componentes de la arquitectura de procesamiento de datos de la cadena de frío de RapidGo.

Se implementó y probó el productor de eventos de temperatura y humedad. Este genera eventos JSON con identificadores de evento, pedido y vehículo, tipo de producto, temperatura, humedad y marca temporal. Los eventos se ajustan al contrato definido en el repositorio.

También se integraron Kafka y Spark Structured Streaming para procesar las lecturas y generar salidas en el Data Lake en formato Parquet.

Los componentes trabajados durante esta semana son:

- Productor de eventos de cadena de frío.
- Tópico Kafka `rapidgo.coldchain.readings`.
- Procesamiento mediante Spark Structured Streaming.
- Persistencia de datos crudos, lecturas procesadas, registros rechazados, alertas e indicadores KPI.
- Pruebas automatizadas y flujo de integración continua mediante GitHub Actions.
- Documentación de las decisiones técnicas y evidencias de ejecución.

## 2. Evidencias de ejecución del productor y Kafka

Se realizaron pruebas para verificar la generación y publicación de eventos.

Los resultados registrados fueron:

- Se ejecutaron 51 pruebas automatizadas, todas aprobadas.
- Se inició Kafka mediante Docker Compose.
- Se verificó la existencia del tópico `rapidgo.coldchain.readings`.
- El tópico utiliza tres particiones y factor de replicación 1.
- En una prueba inicial se enviaron 10 eventos mediante el productor.
- En esa ejecución, el productor informó la siguiente distribución:
  - Partición 0: un mensaje.
  - Partición 1: dos mensajes.
  - Partición 2: siete mensajes.
- Se consumieron los 10 mensajes del tópico y se verificó su estructura JSON.
- En una ejecución posterior se enviaron 20 eventos mediante el productor.
- En esta segunda ejecución, la distribución informada fue:
  - Partición 0: tres mensajes.
  - Partición 1: tres mensajes.
  - Partición 2: catorce mensajes.

La distribución puede variar entre ejecuciones. Estas pruebas permiten comprobar la publicación de mensajes y su distribución entre las particiones, pero no garantizan por sí solas una distribución uniforme.

### Contrato de eventos

El contrato se encuentra en:

`samples/evento_lectura.schema.json`

Incluye los campos obligatorios:

- `event_id`
- `team_id`
- `order_id`
- `vehicle_id`
- `product_type`
- `temperature_c`
- `humidity_pct`
- `event_ts`

El campo `order_id` sigue el patrón establecido en el contrato y se utiliza como clave del mensaje Kafka, de acuerdo con la decisión arquitectónica de la Semana 1.

### Problema encontrado y solución

Al ejecutar directamente `python producer/producer.py --help`, Python presentó un error de importación del paquete `producer`.

El problema se solucionó ejecutando el productor como módulo:

`python -m producer.producer --help`

Posteriormente, el productor se ejecutó correctamente y publicó eventos en Kafka.

## 3. Evidencias de ejecución de Spark Structured Streaming

Durante la revisión del entorno se comprobó que el servicio de Spark permanecía activo.

Los registros mostraron el inicio de las consultas de procesamiento:

- Raw.
- Processed.
- Rechazados.
- Alertas.
- KPI.

También se observaron mensajes que identificaban el tópico Kafka de entrada:

`rapidgo.coldchain.readings`

La ejecución permitió verificar que Spark inició las consultas configuradas para recibir y procesar los eventos.

En los registros aparecieron advertencias relacionadas con la ejecución de consultas de streaming y la configuración de algunas opciones. No se registraron como errores fatales en las evidencias revisadas.

La presencia de consultas activas confirma que el servicio inició el procesamiento configurado, pero no demuestra por sí sola que todos los resultados y reglas de negocio sean correctos.

## 4. Evidencias de persistencia en el Data Lake

Durante la inspección del proyecto se encontraron archivos Parquet en las siguientes ubicaciones:

| Ubicación | Propósito |
|---|---|
| `datalake/raw/events` | Persistencia de eventos crudos recibidos desde Kafka. |
| `datalake/processed/readings` | Persistencia de lecturas procesadas y validadas. |
| `datalake/processed/rejected` | Persistencia de registros rechazados por las validaciones. |
| `datalake/curated/alerts` | Persistencia de registros de alertas. |
| `datalake/curated/kpis` | Persistencia de indicadores agregados. |

También se observaron particiones por fecha en las salidas de lecturas procesadas y alertas.

Estos resultados constituyen evidencia de que las consultas generaron archivos Parquet en las ubicaciones configuradas.

Sin embargo, la existencia de archivos no garantiza que todos sus registros sean correctos ni que se cumplan todos los requisitos funcionales de las alertas y los indicadores.

### Checkpoints de Spark

Se encontraron directorios de checkpoints asociados a las consultas de streaming.

Estos directorios permiten conservar información de progreso y estado para la recuperación de las consultas, siempre que se mantengan las condiciones de compatibilidad y recuperación de Spark Structured Streaming.

Los checkpoints existentes deben conservarse durante las validaciones para evitar perder el estado de las consultas en ejecución.

## 5. Pruebas automatizadas y CI

En una ejecución anterior se obtuvieron los siguientes resultados:

- Pruebas automatizadas: 51 aprobadas.
- Tiempo de ejecución registrado: 0,93 segundos.
- Validación de la configuración de Docker Compose mediante `docker compose config --quiet`: satisfactoria.
- GitHub Actions: se observaron ejecuciones exitosas del flujo de integración continua.

Estos resultados respaldan el avance de las pruebas y de la integración continua.

No obstante, después de los cambios documentales de esta actualización se deben ejecutar nuevamente las pruebas locales y verificar la CI en el Pull Request correspondiente.

## 6. Decisión técnica: ADR-002

Se documenta la decisión técnica en:

`docs/decisions/ADR-002-checkpoints-y-zonas-datalake.md`

La decisión consiste en separar las salidas del pipeline según su propósito y utilizar checkpoints específicos para las consultas de streaming.

La decisión se fundamenta en las evidencias observadas durante la ejecución:

- Consultas de Spark activas para las diferentes salidas.
- Archivos Parquet en las zonas del Data Lake.
- Directorios de checkpoints asociados a las consultas.
- Eventos publicados en el tópico Kafka.

La separación facilita la inspección de los resultados, el diagnóstico de errores y la organización de los datos producidos.

Esta decisión complementa el ADR-001, que establece `order_id` como clave de los mensajes Kafka.

## 7. Trazabilidad del trabajo

- Issue del productor: #7 – Implementar productor de eventos de temperatura.
- Pull Request del productor: #13 – `feat: producer de lecturas de cadena de frío (#7)`.
- Tópico Kafka: `rapidgo.coldchain.readings`.
- Contrato de eventos: `samples/evento_lectura.schema.json`.
- ADR-001: `docs/decisions/ADR-001-key-y-particiones-kafka.md`.
- ADR-002: `docs/decisions/ADR-002-checkpoints-y-zonas-datalake.md`.
- Checkpoint de Semana 2: `docs/checkpoints/semana2.md`.

## 8. Validaciones funcionales pendientes

Aunque se verificaron la ejecución del entorno, la activación de las consultas y la generación de archivos Parquet, es necesario completar o confirmar las siguientes validaciones:

1. Verificar que los KPI calculen el promedio de temperatura por vehículo en ventanas de cinco minutos.
2. Verificar que la regla de exposición prolongada esté integrada correctamente en el flujo de alertas de Spark.
3. Comprobar que las alertas de temperatura crítica y exposición prolongada produzcan los resultados esperados.
4. Ejecutar nuevamente las pruebas automatizadas después de los cambios.
5. Verificar el resultado de GitHub Actions en el Pull Request que incorpora esta documentación.

Estos puntos no se consideran completados hasta disponer de evidencia específica de su validación.

## 9. Próximos pasos

Para finalizar la documentación y validación de la Semana 2 se establece el siguiente orden de trabajo:

1. Crear una rama para los cambios documentales.
2. Incorporar el ADR-002 y actualizar este checkpoint.
3. Ejecutar las pruebas automatizadas y comprobar la configuración del proyecto.
4. Revisar los cambios y realizar el commit.
5. Publicar la rama y crear un Pull Request hacia `main`.
6. Verificar que GitHub Actions finalice correctamente.
7. Revisar las validaciones funcionales pendientes antes de declarar terminado el pipeline de Semana 2.

## 10. Conclusión

Durante la Semana 2 se avanzó en la integración del productor, Kafka, Spark Structured Streaming y el Data Lake. Se comprobó la publicación de eventos, el inicio de las consultas de Spark y la existencia de archivos Parquet en las zonas configuradas.

También se cuenta con pruebas automatizadas y ejecuciones exitosas de integración continua registradas previamente.

La documentación técnica se complementa con el ADR-002, que formaliza la decisión de separar las salidas del procesamiento y mantener checkpoints específicos por consulta.

El cierre definitivo de la Semana 2 requiere confirmar las validaciones funcionales pendientes y verificar nuevamente las pruebas y la CI después de incorporar estos cambios.
