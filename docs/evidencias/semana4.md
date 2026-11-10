# Evidencias de integración - Semana 4

**Proyecto:** RapidGo - Cadena de Frío
**Grupo:** 1
**Rama:** `steban`

## 1. Preparación del entorno

Se trabajó desde el repositorio del proyecto y se verificó la conexión con GitHub y la actualización de la rama de trabajo.

## 2. Ejecución de Kafka

Se inició el entorno mediante Docker Compose.

Se verificó que el servicio Kafka estuviera funcionando y que existiera el tópico `rapidgo.coldchain.readings`, configurado con tres particiones y una réplica.

**Resultado:** tópico creado y disponible para recibir eventos.

## 3. Generación de eventos

Se ejecutó el productor en modo de prueba:

```powershell
python -m producer.producer --dry-run --max-eventos 5
```

**Resultado:** se generaron cinco eventos simulados en modo `dry-run`. Esta ejecución permitió comprobar la generación de datos, pero no representa cinco mensajes publicados en Kafka.

## 4. Envío y recepción de mensajes

Se realizaron pruebas de publicación y recepción de mensajes mediante Kafka y su consumidor de consola.

En una ejecución del consumidor se reportó la recepción de un total de 10 mensajes.

**Resultado:** se comprobó la recepción de mensajes en Kafka durante la prueba realizada.

## 5. Pruebas automatizadas

Se ejecutaron las pruebas del proyecto mediante:

```powershell
python -m pytest -v
```

**Resultado:** 51 pruebas aprobadas y 0 fallidas.

Las pruebas cubren el contrato de eventos, la generación de lecturas y las reglas de alerta.

## 6. Integración continua

Se verificó el workflow de GitHub Actions configurado en `.github/workflows/ci.yml`.

**Resultado:** ejecución correcta del workflow, según la revisión realizada en GitHub.

## 7. Evidencias gráficas

En esta carpeta se conservan capturas relacionadas con:

- Inicio de los servicios Docker.
- Configuración del tópico Kafka.
- Generación de eventos.
- Envío y recepción de mensajes.
- Ejecución de las pruebas automatizadas.
- Resultado del workflow de GitHub Actions.

## 8. Conclusión

Durante la Semana 4 se validaron los componentes implementados del proyecto RapidGo - Cadena de Frío.

Se comprobó el funcionamiento local de Kafka, la generación de eventos, las pruebas de comunicación con el tópico y la ejecución de las pruebas automatizadas.

La ejecución de Pytest finalizó con 51 pruebas aprobadas. Las evidencias recopiladas permiten documentar el avance realizado.

Las integraciones con Spark Structured Streaming, Data Lake y Airflow quedan identificadas como trabajo pendiente.
