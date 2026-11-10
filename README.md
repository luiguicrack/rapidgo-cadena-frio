# RapidGo – Cadena de Frío

Proyecto académico para simular y validar el monitoreo de temperatura y humedad durante el transporte de productos refrigerados y congelados.

El proyecto utiliza Python y Apache Kafka para generar y transmitir eventos de lectura. También incluye un contrato JSON, reglas de alerta y pruebas automatizadas.

## 1. Estado actual del proyecto

Actualmente se encuentran implementados y verificados:

- Generador de eventos simulados en Python.
- Productor Kafka para publicar lecturas.
- Validación de eventos mediante JSON Schema.
- Configuración de Apache Kafka en Docker Compose.
- Tema Kafka `rapidgo.coldchain.readings`, con 3 particiones.
- Reglas de evaluación de lecturas y excursiones de temperatura.
- Pruebas automatizadas con Pytest.
- Integración continua mediante GitHub Actions.
- Documentación de pruebas, checkpoints, decisiones y evidencias.

**Resultado de pruebas:** 51 pruebas aprobadas.

> Nota: Spark Structured Streaming, la escritura real al Data Lake y los DAG de Airflow todavía no están implementados en esta versión del repositorio. El archivo `spark/alert_rules.py` contiene reglas Python, pero no constituye por sí mismo una integración con Spark.

## 2. Tecnologías utilizadas

- Python
- Apache Kafka
- Docker Compose
- JSON Schema
- Pytest
- Git y GitHub
- GitHub Actions

## 3. Estructura del repositorio

```text
rapidgo-cadena-frio/
├── .github/
│   └── workflows/
│       └── ci.yml
├── airflow/
│   └── dags/
│       └── rapidgo_orquestacion.py
├── datalake/
│   ├── raw/
│   ├── processed/
│   └── curated/
├── docs/
│   ├── arquitectura.png
│   ├── pruebas-y-ci.md
│   ├── checkpoints/
│   │   ├── semana1.md
│   │   └── semana4.md
│   ├── decisions/
│   │   ├── ADR-001-key-y-particiones-kafka.md
│   │   └── ADR-002-validacion-integracion.md
│   └── evidencias/
├── producer/
│   ├── generador.py
│   └── producer.py
├── samples/
│   ├── evento_lectura.schema.json
│   └── evento_lectura_ejemplo.json
├── spark/
│   └── alert_rules.py
├── tests/
│   ├── test_contrato.py
│   ├── test_generador.py
│   └── test_reglas_alerta.py
├── docker-compose.yml
├── pytest.ini
├── requirements.txt
└── README.md
```

## 4. Requisitos previos

Para ejecutar el proyecto se necesita:

- Python 3.13 o una versión compatible.
- Docker Desktop con Docker Compose.
- Git.

## 5. Instalación

Clonar el repositorio:

```bash
git clone https://github.com/luiguicrack/rapidgo-cadena-frio.git
cd rapidgo-cadena-frio
```

Crear y activar un entorno virtual.

En Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Instalar dependencias:

```powershell
python -m pip install -r requirements.txt
```

## 6. Iniciar Apache Kafka

Validar la configuración:

```powershell
docker compose -f docker-compose.yml config --quiet
```

Iniciar los servicios:

```powershell
docker compose -f docker-compose.yml up -d
```

Consultar el estado:

```powershell
docker compose -f docker-compose.yml ps
```

Comprobar que existe el tema:

```powershell
docker compose -f docker-compose.yml exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Consultar sus particiones:

```powershell
docker compose -f docker-compose.yml exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic rapidgo.coldchain.readings
```

El tema configurado es `rapidgo.coldchain.readings` y utiliza 3 particiones.

## 7. Ejecutar el productor

Para simular cinco eventos sin enviarlos a Kafka:

```powershell
python -m producer.producer --dry-run --max-eventos 5
```

Para publicar eventos en Kafka:

```powershell
python -m producer.producer --max-eventos 10
```

El productor utiliza por defecto `localhost:29092` y el tema `rapidgo.coldchain.readings`.

## 8. Ejecutar las pruebas

Desde la raíz del repositorio:

```powershell
python -m pytest -v
```

La última ejecución documentada obtuvo:

```text
51 passed
```

Las pruebas cubren:

- Validación del contrato de eventos.
- Generación de lecturas.
- Reglas de alerta.
- Manejo de lecturas normales y excursiones.
- Ordenamiento de lecturas por tiempo.
- Evaluación según el tipo de producto.

## 9. Integración continua

El flujo de GitHub Actions está configurado en:

`.github/workflows/ci.yml`

Se ejecuta al realizar push o pull request hacia `main`, y también permite ejecución manual.

El flujo instala las dependencias y ejecuta las pruebas automatizadas.

La documentación adicional está en `docs/pruebas-y-ci.md`.

## 10. Checkpoints, decisiones y evidencias

La documentación del proyecto se encuentra en:

- `docs/checkpoints/`: seguimiento de avances por semana.
- `docs/decisions/`: decisiones técnicas del proyecto.
- `docs/evidencias/`: capturas de pantalla de las pruebas y verificaciones.
- `docs/arquitectura.png`: diagrama de arquitectura.
- `docs/pruebas-y-ci.md`: descripción de pruebas e integración continua.

## 11. Decisiones técnicas

Entre las decisiones documentadas se encuentran:

- Utilizar `order_id` como clave de los mensajes Kafka.
- Configurar el tema con tres particiones.
- Validar los eventos antes de su publicación.
- Mantener pruebas automatizadas para verificar el contrato, el generador y las reglas de alerta.

Las decisiones completas se encuentran en `docs/decisions/`.

## 12. Detener los servicios

Para detener Kafka:

```powershell
docker compose -f docker-compose.yml down
```

Este comando detiene y elimina los contenedores de Compose. No elimina el código del proyecto.

## 13. Trabajo pendiente

Para completar la arquitectura planteada inicialmente se requiere:

- Implementar el procesamiento mediante Spark Structured Streaming.
- Integrar la persistencia de resultados en el Data Lake.
- Crear y probar los DAG de Airflow.
- Verificar el flujo completo desde la generación hasta el almacenamiento de eventos.

Estas actividades no se consideran terminadas en la versión actual.

## 14. Repositorio

GitHub: https://github.com/luiguicrack/rapidgo-cadena-frio

Rama de trabajo: `steban`
