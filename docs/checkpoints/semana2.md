# Checkpoint Semana 2 – Grupo 1: Cadena de frío

## 1. Avance realizado

Durante la Semana 2 se avanzó en la integración de los componentes de la arquitectura de procesamiento de datos de la cadena de frío de RapidGo.

Se implementó y probó el productor de eventos de temperatura y humedad. Este genera eventos JSON con identificadores de evento, pedido y vehículo, tipo de producto, temperatura, humedad y marca temporal, de acuerdo con el contrato definido en el repositorio.

También se integraron Kafka y Spark Structured Streaming para procesar las lecturas y generar salidas en el Data Lake en formato Parquet.

Los componentes trabajados durante esta semana son:

- Productor de eventos de cadena de frío.
- Tópico Kafka `rapidgo.coldchain.readings`.
- Procesamiento mediante Spark Structured Streaming.
- Persistencia de datos crudos, lecturas procesadas, registros rechazados, alertas e indicadores KPI.
- Checkpoints asociados a las consultas de streaming.
- Pruebas automatizadas e integración continua mediante GitHub Actions.
- Documentación de decisiones técnicas y evidencias de ejecución.

## 2. Evidencias de ejecución del productor y Kafka

Se realizaron pruebas para verificar la generación y publicación de eventos.

Los resultados registrados fueron:

- Se ejecutaron 51 pruebas automatizadas, todas aprobadas en una validación anterior.
- Se inició Kafka mediante Docker Compose.
- Se verificó la existencia del tópico `rapidgo.coldchain.readings`.
- El tópico utiliza tres particiones y factor de replicación 1.
- En una prueba inicial se enviaron 10 eventos mediante el productor.
- En esa ejecución, la distribución informada fue:
  - Partición 0: 1 mensaje.
  - Partición 1: 2 mensajes.
  - Partición 2: 7 mensajes.
- En una ejecución posterior se enviaron 20 eventos mediante el productor.
- En esta segunda ejecución, la distribución informada fue:
  - Partición 0: 3 mensajes.
  - Partición 1: 3 mensajes.
  - Partición 2: 14 mensajes.

### 2.1. Valor real de ejecución

En la ejecución de 20 eventos se obtuvo la siguiente distribución:

| Partición Kafka | Eventos publicados |
|---|---:|
| 0 | 3 |
| 1 | 3 |
| 2 | 14 |
| **Total** | **20** |

Este resultado demuestra que, en esa ejecución, el productor publicó 20 eventos distribuidos entre las tres particiones.

La distribución puede variar entre ejecuciones y no implica que las particiones deban recibir la misma cantidad de mensajes.

El comando utilizado fue:

```powershell
python -m producer.producer --bootstrap localhost:29092 --topic rapidgo.coldchain.readings --max-eventos 20 --intervalo 0.2
```

### 2.2. Contrato de eventos

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

### 2.3. Problema encontrado y solución

Al ejecutar directamente el productor mediante:

```powershell
python producer/producer.py --help
```

Python presentó un error relacionado con las importaciones del paquete `producer`.

El problema se solucionó ejecutando el productor como módulo:

```powershell
python -m producer.producer --help
```

Posteriormente, el productor se ejecutó correctamente y publicó eventos en Kafka.

**Pull Request relacionado con el productor:**

[PR #13 – feat: producer de lecturas de cadena de frío](https://github.com/luiguicrack/rapidgo-cadena-frio/pull/13)

Este enlace corresponde al Pull Request del productor. La relación exacta entre ese PR y el problema de importación debe coincidir con los cambios que efectivamente contiene.

## 3. Evidencias de ejecución de Spark Structured Streaming

Durante la revisión del entorno se comprobó que el servicio de Spark permanecía activo.

Los registros mostraron el inicio de las consultas de procesamiento:

- Raw.
- Processed.
- Rechazados.
- Alertas.
- KPI.

También se identificó el tópico Kafka de entrada:

`rapidgo.coldchain.readings`

La ejecución permitió verificar que Spark inició las consultas configuradas para recibir y procesar los eventos.

En los registros aparecieron advertencias relacionadas con la ejecución de consultas de streaming y algunas opciones de configuración. En las evidencias revisadas no se identificaron como errores fatales que impidieran el inicio de las consultas.

La presencia de consultas activas confirma que el servicio inició el procesamiento configurado, pero no demuestra por sí sola que todos los resultados ni todas las reglas de negocio sean correctos.

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

La existencia de archivos no garantiza que todos sus registros sean correctos ni que se cumplan todos los requisitos funcionales de las alertas y los indicadores.

### 4.1. Checkpoints de Spark

Se encontraron directorios de checkpoints asociados a las consultas de streaming.

Estos directorios permiten conservar información del progreso y del estado de las consultas para facilitar su recuperación, siempre que se mantengan las condiciones de compatibilidad requeridas por Spark Structured Streaming.

Los checkpoints existentes deben conservarse durante las validaciones para evitar perder el estado de las consultas y las evidencias de ejecución.

## 5. Pruebas automatizadas e integración continua

En una ejecución anterior se obtuvieron los siguientes resultados:

- **Pruebas automatizadas:** 51 aprobadas.
- **Tiempo de ejecución registrado:** 0,93 segundos.
- **Configuración de Docker Compose:** validada mediante `docker compose config --quiet`.
- **GitHub Actions:** se observaron ejecuciones exitosas del flujo de integración continua.

Estos resultados respaldan el avance de las pruebas y de la integración continua.

Después de los cambios de esta entrega, se deben ejecutar nuevamente las pruebas locales y verificar la CI en el Pull Request correspondiente.

### Comandos de validación

Ejecutar las pruebas:

```powershell
python -m pytest -q
```

Validar Docker Compose:

```powershell
docker compose config --quiet
```

Revisar los cambios:

```powershell
git diff --check
```

Los resultados definitivos deben corresponder a la versión del código que se pretende entregar.

## 6. Decisión técnica: ADR-002

La decisión técnica se documenta en:

`docs/decisions/ADR-002-checkpoints-y-zonas-datalake.md`

### 6.1. Decisión adoptada

Se decidió separar las salidas del pipeline según su propósito y mantener checkpoints específicos para las consultas de streaming.

La organización de las salidas permite distinguir entre:

- Eventos originales.
- Lecturas procesadas y validadas.
- Registros rechazados.
- Alertas.
- Indicadores KPI.

Cada salida dispone de una ubicación de almacenamiento diferenciada y las consultas utilizan sus rutas de checkpoint correspondientes.

### 6.2. Justificación basada en la ejecución

La decisión se fundamenta en las evidencias observadas:

- Spark inició las consultas configuradas para las distintas salidas.
- Se encontraron archivos Parquet en las zonas Raw, Processed y Curated.
- Se identificaron directorios de checkpoints asociados a las consultas.
- El productor publicó eventos en el tópico Kafka configurado.

La separación facilita la inspección de resultados, el diagnóstico de errores y la organización de los datos producidos.

Los checkpoints permiten conservar el progreso de las consultas y facilitan su recuperación bajo condiciones de ejecución compatibles con Spark Structured Streaming.

Esta decisión complementa el ADR-001, que establece `order_id` como clave de los mensajes Kafka.

## 7. Trazabilidad del trabajo

- **Issue del productor:** [#7 – Implementar productor de eventos de temperatura](https://github.com/luiguicrack/rapidgo-cadena-frio/issues/7).
- **Pull Request del productor:** [#13 – feat: producer de lecturas de cadena de frío](https://github.com/luiguicrack/rapidgo-cadena-frio/pull/13).
- **Repositorio:** [RapidGo – Cadena de frío](https://github.com/luiguicrack/rapidgo-cadena-frio).
- **Tópico Kafka:** `rapidgo.coldchain.readings`.
- **Contrato de eventos:** `samples/evento_lectura.schema.json`.
- **ADR-001:** `docs/decisions/ADR-001-key-y-particiones-kafka.md`.
- **ADR-002:** `docs/decisions/ADR-002-checkpoints-y-zonas-datalake.md`.
- **Checkpoint de Semana 2:** `docs/checkpoints/semana2.md`.

Las contribuciones de cada integrante deben verificarse mediante sus respectivos Pull Requests, commits e Issues.

## 8. Validaciones funcionales pendientes

Aunque se verificaron la ejecución del entorno, la activación de las consultas y la generación de archivos Parquet, es necesario completar o confirmar las siguientes validaciones:

1. Verificar que los KPI calculen el promedio de temperatura por vehículo en ventanas de cinco minutos.
2. Verificar que la regla de exposición prolongada esté integrada correctamente en el flujo de alertas de Spark.
3. Comprobar que las alertas de temperatura crítica y exposición prolongada produzcan los resultados esperados con eventos de prueba controlados.
4. Ejecutar nuevamente las pruebas automatizadas después de los últimos cambios.
5. Verificar que GitHub Actions finalice correctamente en el Pull Request que incorpora esta documentación.
6. Confirmar que el pipeline Kafka → Spark → Data Lake procesa eventos y produce resultados verificables de extremo a extremo.

Estos puntos no se consideran completados hasta disponer de evidencia específica de su validación.

## 9. Próximos pasos

Para completar la entrega de la Semana 2 se establece el siguiente orden de trabajo:

1. Confirmar la existencia del ADR-002 y actualizar este checkpoint.
2. Ejecutar las pruebas automatizadas.
3. Validar la configuración de Docker Compose.
4. Revisar los cambios con `git diff --check`.
5. Publicar los cambios documentales mediante una rama y un Pull Request.
6. Verificar que GitHub Actions finalice correctamente en el Pull Request.
7. Confirmar que cada integrante tenga un Pull Request sustancial registrado durante la Semana 2.
8. Completar las validaciones funcionales pendientes de Spark y las alertas.
9. Integrar los cambios aprobados en `main`.

## 10. Conclusión

Durante la Semana 2 se avanzó en la integración del productor, Kafka, Spark Structured Streaming y el Data Lake.

Se comprobó la publicación de eventos, el inicio de las consultas de Spark y la existencia de archivos Parquet en las zonas configuradas. También se dispone de resultados previos de pruebas automatizadas y ejecuciones exitosas de integración continua.

La documentación se complementa con el ADR-002, que formaliza la decisión de separar las salidas del procesamiento y mantener checkpoints específicos por consulta.

El cierre de la Semana 2 requiere confirmar las validaciones funcionales pendientes, verificar las pruebas y la CI sobre los cambios finales y revisar las contribuciones de los integrantes.

---
