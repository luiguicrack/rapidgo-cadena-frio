# RapidGo – Pipeline Big Data End-to-End: Cadena de frío

**Proyecto final de aula – Línea de Profundización I: Big Data**  
**Variante 3: Cadena de frío**  
**Release de Semana 1:** `v0.1.0-foundation`  
**Semana 2:** Implementación del pipeline de eventos y procesamiento inicial.

---

## 1. Descripción del proyecto

RapidGo es una plataforma de entregas que necesita monitorear las condiciones de transporte de productos refrigerados y congelados. Durante el traslado, las variaciones de temperatura pueden afectar la calidad de los productos y generar pérdidas económicas.

Este proyecto desarrolla una arquitectura Big Data para generar lecturas de sensores, transportarlas mediante Apache Kafka, procesarlas con Apache Spark Structured Streaming y almacenarlas en un Data Lake utilizando Parquet.

El pipeline contempla la validación de eventos, la clasificación de lecturas según las condiciones de temperatura, la generación de alertas y el cálculo de indicadores operacionales.

Durante la Semana 1 se definieron la arquitectura, el contrato de eventos, los indicadores iniciales y la organización del repositorio. En la Semana 2 se avanzó en la implementación del productor, la integración con Kafka, el procesamiento con Spark, la persistencia de datos y la automatización de pruebas.

## 2. Objetivos

### Objetivo general

Implementar progresivamente un pipeline Big Data para monitorear la cadena de frío de RapidGo mediante eventos de sensores, procesamiento en flujo, almacenamiento e indicadores operacionales.

### Objetivos específicos

- Definir y utilizar un contrato JSON para las lecturas de temperatura y humedad.
- Generar eventos simulados asociados a pedidos y vehículos.
- Transportar los eventos mediante Apache Kafka.
- Procesar los datos con Apache Spark Structured Streaming.
- Validar los eventos y clasificar las lecturas según sus condiciones de temperatura.
- Definir indicadores y reglas de alerta para detectar incumplimientos.
- Persistir eventos y resultados en formato Parquet.
- Utilizar checkpoints para permitir la recuperación del procesamiento en flujo.
- Incorporar pruebas automatizadas e integración continua mediante GitHub Actions.
- Mantener la trazabilidad mediante GitHub, Issues, Pull Requests, Kanban y releases.

## 3. Equipo de trabajo

| Integrante | Responsabilidades principales |
|---|---|
| Brayan Ramírez | Productor de lecturas, integración con Kafka y documentación de ejecución. |
| Sebastián Bernal | Contrato de eventos, procesamiento con Spark y componentes relacionados con indicadores y reglas de negocio. |
| Steban Blanco | Arquitectura, configuración de infraestructura y documentación técnica. |

Las responsabilidades y contribuciones se evidencian mediante las Issues, las ramas, los commits y los Pull Requests del repositorio.

## 4. Problema de negocio

Durante el transporte de productos refrigerados o congelados es necesario mantener la temperatura dentro de los rangos adecuados. Una temperatura excesiva o demasiado baja puede comprometer la conservación de la mercancía.

La solución registra lecturas de sensores, valida los eventos recibidos, identifica desviaciones de temperatura y almacena los resultados para su consulta y análisis.

### Valor esperado

- Identificar condiciones anormales durante el transporte.
- Medir la frecuencia de las lecturas fuera de rango.
- Detectar exposiciones prolongadas a temperaturas inadecuadas.
- Conservar un historial de lecturas y resultados procesados.
- Facilitar la trazabilidad de las condiciones de transporte.
- Proporcionar información para mejorar el control operacional.

## 5. Arquitectura del proyecto

La arquitectura de RapidGo se organiza en las siguientes etapas:

1. **Producer:** genera eventos simulados de sensores asociados a pedidos y vehículos.
2. **Apache Kafka:** recibe y distribuye los eventos mediante el topic `rapidgo.coldchain.readings`.
3. **Apache Spark Structured Streaming:** consume los eventos, interpreta el JSON, valida los datos, clasifica las lecturas y ejecuta consultas en flujo.
4. **Data Lake:** almacena los eventos y resultados en formato Parquet, organizados en las zonas Raw, Processed y Curated.
5. **Checkpoints:** conservan el estado de progreso de las consultas de streaming.
6. **Pruebas automatizadas:** verifican las reglas de negocio y el comportamiento implementado.
7. **GitHub Actions:** ejecuta validaciones automatizadas asociadas al repositorio.
8. **Apache Airflow:** queda contemplado para la etapa posterior de orquestación.

### Diagrama de arquitectura

![Arquitectura de RapidGo - Cadena de frío](docs/arquitectura.png)

## 6. Tecnologías utilizadas

| Tecnología | Función |
|---|---|
| Python | Generación de eventos, reglas de negocio y pruebas. |
| Apache Kafka | Transporte de eventos mediante un topic particionado. |
| Apache Spark Structured Streaming | Procesamiento de eventos en flujo. |
| Parquet | Almacenamiento columnar de eventos y resultados. |
| Data Lake | Organización de datos Raw, Processed y Curated. |
| Docker Compose | Configuración y ejecución local de los servicios. |
| pytest | Ejecución de pruebas automatizadas. |
| Git y GitHub | Control de versiones y colaboración. |
| GitHub Projects | Seguimiento de tareas mediante Kanban. |
| GitHub Actions | Integración continua. |
| Apache Airflow | Tecnología prevista para la orquestación posterior. |

## 7. Contrato de eventos

Los eventos representan lecturas de temperatura y humedad relacionadas con un pedido, un vehículo y un tipo de producto.

Los archivos de referencia son:

- Esquema: [`samples/evento_lectura.schema.json`](samples/evento_lectura.schema.json)
- Ejemplo: [`samples/evento_lectura_ejemplo.json`](samples/evento_lectura_ejemplo.json)

El contrato contempla los siguientes campos:

| Campo | Descripción |
|---|---|
| `event_id` | Identificador único del evento. |
| `team_id` | Identificador del equipo productor. |
| `order_id` | Identificador del pedido. |
| `vehicle_id` | Identificador del vehículo. |
| `product_type` | Tipo de producto: refrigerado o congelado. |
| `temperature_c` | Temperatura registrada en grados Celsius. |
| `humidity_pct` | Humedad relativa registrada. |
| `event_ts` | Fecha y hora del evento. |

Los nombres, tipos, restricciones y valores permitidos definitivos son los establecidos en los archivos del contrato.

### Generación de eventos

El productor permite generar lecturas simuladas y publicarlas en Kafka. También permite configurar parámetros de ejecución, como la cantidad de eventos y el intervalo entre publicaciones.

La ejecución del productor se realiza como módulo de Python para resolver correctamente las importaciones del paquete.

## 8. Kafka: topic, key y particiones

| Parámetro | Configuración |
|---|---|
| Topic | `rapidgo.coldchain.readings` |
| Key | `order_id` |
| Número de particiones | 3 |
| Factor de replicación local | 1 |
| Broker desde el host | `localhost:29092` |
| Broker dentro de Docker Compose | `kafka:9092` |

### Justificación técnica

Se utiliza `order_id` como clave de Kafka para que las lecturas de un mismo pedido se asignen a la misma partición, manteniendo su orden relativo dentro de ella.

Las tres particiones permiten distribuir el procesamiento entre diferentes pedidos. El factor de replicación 1 corresponde al entorno local y no ofrece tolerancia a la pérdida de un broker.

La decisión inicial está documentada en [`docs/decisions/ADR-001-key-y-particiones-kafka.md`](docs/decisions/ADR-001-key-y-particiones-kafka.md).

### Evidencias de ejecución

Durante las pruebas del proyecto se obtuvieron los siguientes resultados:

- `docker compose config --quiet` finalizó sin errores.
- Docker Compose inició los servicios de Kafka y Spark.
- Se verificó el topic `rapidgo.coldchain.readings`.
- La descripción del topic confirmó tres particiones y factor de replicación 1.
- Una ejecución del productor publicó 20 eventos.
- En esa ejecución, los eventos se distribuyeron entre las particiones de la siguiente manera:

| Partición | Eventos observados |
|---|---:|
| 0 | 3 |
| 1 | 3 |
| 2 | 14 |
| **Total** | **20** |

La distribución anterior corresponde a una ejecución concreta. No implica que las particiones deban recibir exactamente la misma cantidad de eventos en cada prueba.

## 9. Procesamiento con Spark Structured Streaming

El procesamiento implementado se encuentra en `spark/streaming.py`.

La aplicación consume eventos de Kafka y ejecuta consultas de streaming para gestionar las lecturas y producir resultados en el Data Lake.

### 9.1. Lectura y almacenamiento Raw

La aplicación lee los mensajes del topic y conserva los eventos recibidos en:

`datalake/raw/events`

Esta zona permite conservar los datos de entrada antes de considerar únicamente los registros válidos para el procesamiento posterior.

### 9.2. Interpretación y validación

Los mensajes JSON se interpretan utilizando el esquema de eventos y se convierten los campos temporales necesarios para el procesamiento.

Entre las validaciones implementadas se encuentran:

- Presencia de los campos requeridos.
- Formato del identificador del evento.
- Identificador del equipo esperado.
- Formato del identificador del pedido.
- Formato del identificador del vehículo.
- Tipo de producto permitido.
- Validez de la fecha y hora.
- Límites admitidos para temperatura y humedad.

Los registros que no cumplen las validaciones se escriben en:

`datalake/processed/rejected`

### 9.3. Clasificación de lecturas

Las lecturas válidas se clasifican mediante las transformaciones definidas en el procesamiento.

Se calculan campos como:

- `is_critical`: indica si la lectura cumple la condición de temperatura crítica.
- `is_out_of_range`: indica si la lectura está fuera del rango permitido.
- `temperature_status`: clasifica la lectura como `critica`, `fuera_rango` u `ok`.
- `event_date`: fecha utilizada para organizar los resultados.

Los registros procesados se escriben en:

`datalake/processed/readings`

Los datos se particionan por `event_date`.

### 9.4. Generación de alertas

La consulta de alertas identifica lecturas cuyo estado de temperatura no es `ok`.

Los resultados incluyen información del evento, pedido, vehículo, tipo de producto, temperatura, fecha del evento y tipo de alerta.

Los resultados se escriben en:

`datalake/curated/alerts`

**Nota:** la integración completa de la regla de exposición prolongada y su validación de extremo a extremo deben verificarse antes de afirmar que todas las reglas de alerta requeridas están implementadas en el flujo de Spark.

### 9.5. Indicadores

La aplicación incluye una consulta de indicadores que utiliza ventanas temporales de cinco minutos y una marca de agua de diez minutos.

Entre las métricas calculadas se encuentran:

- Temperatura promedio.
- Cantidad de lecturas.
- Cantidad de lecturas fuera de rango.
- Cantidad de lecturas críticas.
- Porcentaje de lecturas fuera de rango.

Los resultados se escriben en:

`datalake/curated/kpis`

La agregación actualmente descrita agrupa por ventana temporal y tipo de producto. El cálculo del promedio por vehículo debe verificarse o ajustarse para cumplir ese requisito específico de la guía.

## 10. Reglas de temperatura y alertas

Los rangos de referencia definidos para el proyecto son:

| Tipo de producto | Temperatura mínima | Temperatura máxima |
|---|---:|---:|
| Refrigerado | 2 °C | 8 °C |
| Congelado | -25 °C | -15 °C |

### 10.1. Temperatura crítica

Los umbrales definidos en las reglas de negocio son:

- **Refrigerado:** temperatura inferior a `-1 °C` o superior a `11 °C`.
- **Congelado:** temperatura superior a `-12 °C`.

Estos umbrales se diferencian de los rangos normales de conservación y permiten identificar situaciones de mayor severidad.

### 10.2. Exposición prolongada

El proyecto contempla generar una alerta cuando la temperatura permanezca continuamente fuera del rango permitido durante al menos diez minutos.

La lógica de negocio se encuentra en `spark/alert_rules.py`.

Esta lógica considera el tipo de producto, la secuencia temporal de las lecturas y el reinicio de la exposición cuando aparece una lectura normal.

La existencia de pruebas unitarias para esta regla no significa, por sí sola, que la alerta ya esté integrada en la consulta de Spark. Esa integración debe verificarse de forma independiente.

## 11. Data Lake y persistencia

Los resultados se organizan en las siguientes zonas:

| Zona | Ruta | Propósito |
|---|---|---|
| Raw | `datalake/raw/events` | Conservar los eventos recibidos. |
| Processed | `datalake/processed/readings` | Almacenar las lecturas válidas y clasificadas. |
| Processed | `datalake/processed/rejected` | Conservar los registros rechazados por las validaciones. |
| Curated | `datalake/curated/alerts` | Almacenar las alertas generadas. |
| Curated | `datalake/curated/kpis` | Almacenar los indicadores calculados. |

Los datos se persisten en formato Parquet. Las lecturas procesadas y las alertas se organizan por fecha del evento.

### Checkpoints

Las consultas de streaming utilizan rutas de checkpoint para conservar información del progreso de procesamiento y facilitar la recuperación de las consultas.

Los checkpoints se encuentran asociados a las salidas correspondientes. Durante las pruebas no deben eliminarse indiscriminadamente, ya que contienen información necesaria para el estado del procesamiento.

## 12. Pruebas automatizadas e integración continua

El proyecto incorpora pruebas automatizadas mediante `pytest` y un workflow de GitHub Actions.

### Ejecutar las pruebas

Desde la raíz del repositorio:

```powershell
python -m pytest -q
```

La ejecución anterior registró **51 pruebas aprobadas** en una de las validaciones realizadas. El resultado debe confirmarse nuevamente después de cada modificación relevante.

### Validar Docker Compose

```powershell
docker compose config --quiet
```

### Revisar el estado de Git

```powershell
git status --short
```

### Revisar los cambios antes de un commit

```powershell
git diff --check
git diff
```

### Integración continua

El workflow de CI permite ejecutar las validaciones automatizadas cuando se realizan los eventos configurados en GitHub Actions.

Los resultados de las ejecuciones pueden consultarse en la pestaña **Actions** del repositorio.

Cada Pull Request debe revisarse antes de integrarse en `main`, comprobando las pruebas, la configuración y las evidencias relacionadas con la funcionalidad modificada.

## 13. Requisitos previos

Para ejecutar el proyecto localmente se requiere:

- Git.
- Docker Desktop con Docker Compose.
- Python compatible con las dependencias del proyecto.
- Las dependencias especificadas en `requirements.txt`.
- Conexión a Internet para descargar las imágenes de Docker y las dependencias necesarias.

## 14. Ejecución local

Los siguientes comandos se ejecutan desde la raíz del repositorio.

### 14.1. Validar la configuración

```powershell
docker compose config --quiet
```

### 14.2. Iniciar los servicios

```powershell
docker compose up -d
```

### 14.3. Consultar el estado de los contenedores

```powershell
docker compose ps
```

### 14.4. Consultar los logs de Kafka y Spark

```powershell
docker compose logs --tail=50 kafka
docker compose logs --tail=50 spark-streaming
```

### 14.5. Verificar el topic

```powershell
docker exec coldchain-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic rapidgo.coldchain.readings
```

### 14.6. Publicar eventos de prueba

El siguiente comando publica 20 eventos, con un intervalo de 0,2 segundos entre publicaciones:

```powershell
python -m producer.producer --bootstrap localhost:29092 --topic rapidgo.coldchain.readings --max-eventos 20 --intervalo 0.2
```

La salida del productor permite comprobar cuántos eventos fueron enviados y cómo se distribuyeron entre las particiones.

### 14.7. Ejecutar las pruebas automatizadas

```powershell
python -m pytest -q
```

### 14.8. Detener los servicios

```powershell
docker compose down
```

Este comando detiene y elimina los contenedores y la red creados por Docker Compose. No debe confundirse con un borrado general de los archivos del repositorio.

Antes de ejecutar operaciones de limpieza adicionales, verifica las rutas de datos y checkpoints para evitar perder evidencias de ejecución.

## 15. Estructura del repositorio

```text
rapidgo-cadena-frio/
├── .github/
│   └── workflows/
│       └── ci.yml
├── airflow/
│   └── dags/
├── datalake/
│   ├── README.md
│   ├── raw/
│   ├── processed/
│   └── curated/
├── docs/
│   ├── arquitectura.png
│   ├── checkpoints/
│   │   ├── semana1.md
│   │   └── semana2.md
│   └── decisions/
│       ├── ADR-001-key-y-particiones-kafka.md
│       └── ADR-002-checkpoints-y-zonas-datalake.md
├── producer/
│   ├── generador.py
│   └── producer.py
├── samples/
│   ├── evento_lectura.schema.json
│   └── evento_lectura_ejemplo.json
├── spark/
│   ├── alert_rules.py
│   └── streaming.py
├── tests/
│   └── test_reglas_alerta.py
├── .gitignore
├── docker-compose.yml
├── README.md
└── requirements.txt
```

La estructura representa los componentes principales del proyecto. La presencia de directorios o documentos de referencia no implica que todas las funcionalidades asociadas estén terminadas.

## 16. Organización y trazabilidad del trabajo

El equipo utiliza GitHub Issues, Pull Requests y un tablero Kanban para organizar el desarrollo.

Las tareas se distribuyen entre cuatro semanas:

- **Semana 1:** diseño, arquitectura, contrato de eventos, indicadores, reglas de negocio y organización del repositorio.
- **Semana 2:** productor, integración con Kafka, procesamiento con Spark, persistencia, checkpoints y pruebas automatizadas.
- **Semana 3:** orquestación con Airflow, controles de calidad y recuperación ante fallos.
- **Semana 4:** integración final, validación de extremo a extremo y publicación de la versión estable.

Las contribuciones deben quedar registradas en las ramas, commits, Issues y Pull Requests correspondientes a cada integrante.

## 17. Documentación y entregas

| Documento o entrega | Propósito |
|---|---|
| `docs/checkpoints/semana1.md` | Evidencias y decisiones de la primera semana. |
| `docs/checkpoints/semana2.md` | Evidencias de ejecución y avances de la segunda semana. |
| `docs/decisions/ADR-001-key-y-particiones-kafka.md` | Justificación de la clave y las particiones de Kafka. |
| `docs/decisions/ADR-002-checkpoints-y-zonas-datalake.md` | Decisión técnica relacionada con checkpoints y zonas del Data Lake. |
| `samples/evento_lectura.schema.json` | Contrato JSON de los eventos. |
| `samples/evento_lectura_ejemplo.json` | Ejemplo de evento. |
| `docs/arquitectura.png` | Diagrama de arquitectura. |

### Releases previstas

| Entrega | Versión prevista | Alcance |
|---|---|---|
| Semana 1 | `v0.1.0-foundation` | Diseño, arquitectura y organización. |
| Semana 2 | `v0.2.0-streaming` | Pipeline de eventos y procesamiento inicial. |
| Semana 3 | Según la guía del proyecto | Orquestación y controles de calidad. |
| Semana 4 | `v1.0.0` | Integración y validación final. |

Las versiones de Semana 2 y posteriores deben publicarse una vez cumplidos los requisitos de la entrega y verificadas las evidencias correspondientes.

## 18. Limitaciones y trabajo pendiente

Aunque el productor, Kafka, Spark, la persistencia Parquet y las pruebas automatizadas ya presentan avances verificables, todavía deben completarse o comprobarse los siguientes puntos antes de considerar finalizado el pipeline:

- Confirmar que el KPI de temperatura promedio se agrupa por vehículo, además de la ventana temporal requerida.
- Integrar y validar de extremo a extremo la alerta por exposición prolongada.
- Verificar que las alertas críticas y las de exposición prolongada se produzcan correctamente con eventos de prueba controlados.
- Confirmar la recuperación de las consultas después de reinicios, utilizando sus checkpoints.
- Validar los resultados almacenados en Parquet y contrastarlos con los eventos publicados.
- Mantener actualizado el checkpoint de Semana 2 con resultados reales.
- Documentar la decisión técnica de ADR-002 con base en la ejecución observada.
- Ejecutar las pruebas y comprobar el estado de CI después de los cambios.
- Desarrollar la orquestación con Airflow en la etapa correspondiente.
- Realizar una validación integral antes de publicar la versión estable.

## 19. Conclusión

El proyecto RapidGo establece una solución Big Data para monitorear la cadena de frío mediante eventos de sensores, transporte con Kafka, procesamiento con Spark Structured Streaming y persistencia en Parquet.

Los avances de la Semana 2 permiten ejecutar y observar componentes fundamentales del pipeline, mantener los resultados organizados por zonas y ejecutar pruebas automatizadas. La siguiente etapa consiste en cerrar las validaciones funcionales pendientes, integrar las reglas de alerta en el flujo completo y fortalecer la recuperación y la orquestación.

La evolución del proyecto se documenta mediante los checkpoints, las decisiones técnicas, las pruebas y las contribuciones registradas en GitHub.

---

**Repositorio:** https://github.com/luiguicrack/rapidgo-cadena-frio  
**Variante:** 3 – Cadena de frío  
**Equipo:** Brayan Ramírez, Sebastián Bernal y Steban Blanco
