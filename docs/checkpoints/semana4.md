# Checkpoint - Semana 4

## Proyecto RapidGo - Cadena de Frío

### Objetivo

Validar los componentes implementados del proyecto de cadena de frío, comprobando el funcionamiento de Kafka, la generación y transmisión de eventos, y las pruebas automatizadas.

### Actividades realizadas

1. Se inició el entorno de Kafka utilizando Docker Compose.
2. Se comprobó que los contenedores estuvieran funcionando.
3. Se verificó la creación del tópico `rapidgo.coldchain.readings`.
4. Se comprobó que el tópico contara con tres particiones.
5. Se ejecutó el productor de eventos para generar lecturas simuladas de temperatura y humedad.
6. Se realizaron pruebas de recepción de mensajes mediante un consumidor de Kafka.
7. Se ejecutaron las pruebas automatizadas con Pytest.
8. Se organizaron las capturas de pantalla como evidencias del desarrollo.
9. Se documentaron las decisiones técnicas y los resultados de validación.

### Resultados comprobados

- Kafka se inició correctamente mediante Docker Compose.
- El tópico `rapidgo.coldchain.readings` quedó disponible con tres particiones.
- El productor generó eventos simulados en modo de prueba.
- Se realizaron pruebas de envío y recepción de mensajes.
- La ejecución de Pytest finalizó con **51 pruebas aprobadas y 0 fallidas**.
- El workflow de GitHub Actions se ejecutó correctamente, según la verificación realizada en GitHub.

### Aspectos pendientes

La integración con Spark Structured Streaming, la escritura real en el Data Lake y la programación de tareas mediante DAG de Airflow no forman parte de los componentes implementados y verificados en esta versión.

### Evidencias

Las capturas de las actividades se encuentran en `docs/evidencias/`. El detalle de cada captura está descrito en `docs/evidencias/semana4.md`.

### Estado

Semana 4: validación de los componentes implementados y documentación de evidencias completadas. Las integraciones pendientes quedan identificadas para una siguiente etapa.