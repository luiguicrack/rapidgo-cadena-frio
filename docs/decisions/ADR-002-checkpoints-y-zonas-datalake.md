# ADR-002: Separación de salidas y checkpoints en el pipeline de streaming

- **Estado:** Aceptado
- **Fecha:** 2026-10-29
- **Proyecto:** RapidGo — Cadena de frío
- **Semana:** 2

## 1. Contexto

Durante la ejecución del pipeline de Spark Structured Streaming se observó que el procesamiento mantiene consultas para los datos crudos, las lecturas procesadas, los registros rechazados, las alertas y los indicadores KPI.

También se verificó la existencia de archivos Parquet en las ubicaciones del Data Lake destinadas a estos resultados, así como de directorios de checkpoints asociados a las consultas.

Esta organización permite distinguir los resultados de las diferentes etapas del procesamiento y conservar el estado de cada consulta de streaming.

## 2. Decisión

Se adopta la separación de las salidas del pipeline por propósito y el uso de checkpoints específicos para las consultas de streaming.

Las salidas se organizan en las siguientes zonas:

- `datalake/raw/events`: eventos recibidos desde Kafka.
- `datalake/processed/readings`: lecturas válidas y transformadas.
- `datalake/processed/rejected`: registros que no superan las validaciones.
- `datalake/curated/alerts`: alertas derivadas de las lecturas.
- `datalake/curated/kpis`: indicadores agregados.

Cada consulta debe utilizar la ubicación de checkpoint configurada para ella, evitando compartir deliberadamente el mismo checkpoint entre consultas diferentes.

## 3. Justificación

La decisión se apoya en la ejecución observada del proyecto:

1. Spark informó que las consultas de streaming estaban activas.
2. Se encontraron archivos Parquet en las ubicaciones de datos crudos, procesados, rechazados, alertas y KPI.
3. Se observaron directorios de checkpoints para las consultas correspondientes.
4. La separación facilita identificar qué resultado pertenece a cada etapa y revisar los datos producidos.

Estas evidencias respaldan la organización de las salidas, pero no demuestran por sí solas que todas las reglas de negocio, las alertas o las agregaciones sean correctas.

## 4. Consecuencias

### Positivas

- Los resultados quedan separados según su propósito.
- Se facilita la inspección de los datos y el diagnóstico de errores.
- Los checkpoints permiten a Spark recuperar el progreso de las consultas cuando se cumplen las condiciones de recuperación de Structured Streaming.
- Se reduce el riesgo de mezclar el estado de consultas independientes.

### Consideraciones

- Los checkpoints deben conservarse entre reinicios cuando se pretende recuperar el estado de una consulta.
- Los cambios incompatibles en una consulta pueden requerir una estrategia de migración o un checkpoint nuevo.
- La existencia de archivos Parquet no garantiza por sí sola la corrección de los datos.
- Deben verificarse por separado las reglas de alertas y las agregaciones requeridas por el proyecto.

## 5. Evidencias de validación

Durante la revisión del entorno se observaron:

- Consultas de Spark activas para las distintas salidas del pipeline.
- Archivos Parquet en las zonas del Data Lake.
- Directorios de checkpoints asociados a las consultas.
- Eventos publicados en el tópico Kafka `rapidgo.coldchain.readings`.

Estas evidencias corresponden a las observaciones de ejecución registradas durante la Semana 2. No sustituyen las pruebas automatizadas ni una comprobación funcional de cada regla de negocio.

## 6. Relación con otras decisiones

Esta decisión complementa el ADR-001, que establece `order_id` como clave del mensaje Kafka para mantener la organización de las lecturas por pedido dentro de las particiones.

El ADR-002 aborda la organización de las salidas del procesamiento y el estado de las consultas de streaming.
