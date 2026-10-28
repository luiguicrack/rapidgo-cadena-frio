# Checkpoint Semana 2 – Grupo 1: Cadena de frío

## 1. Avance realizado

Se implementó y probó el productor de eventos de temperatura y humedad para la variante Cadena de frío. El productor genera eventos JSON con identificadores de evento, pedido y vehículo, tipo de producto, temperatura, humedad y marca temporal. Los eventos se validan mediante el contrato definido en el repositorio.

## 2. Evidencias de ejecución

- Se ejecutaron 51 pruebas automatizadas, todas aprobadas.
- Se inició Kafka mediante Docker Compose.
- Se verificó la existencia del tópico `rapidgo.coldchain.readings`.
- Se enviaron 10 eventos mediante el productor.
- El productor informó la entrega de los mensajes en tres particiones: partición 0, un mensaje; partición 1, dos mensajes; partición 2, siete mensajes.
- Se consumieron 10 mensajes desde el tópico y se verificó su estructura JSON.

## 3. Decisión técnica observada

Se utilizó `order_id` como clave del mensaje Kafka, de acuerdo con la decisión arquitectónica establecida en la Semana 1. La configuración de tres particiones permite distribuir los mensajes entre ellas.

La ejecución observada confirma que los mensajes llegaron al tópico y se distribuyeron entre sus particiones. Esta prueba no demuestra por sí sola que el flujo completo de Spark y el Data Lake esté funcionando.

## 4. Problema encontrado y solución

Al ejecutar directamente `python producer/producer.py --help`, Python presentó un error de importación del paquete `producer`. Se solucionó ejecutando el programa como módulo:

`python -m producer.producer --help`

Posteriormente, el productor se ejecutó correctamente y envió los eventos a Kafka.

## 5. Trazabilidad

- Issue: #7 – Implementar productor de eventos de temperatura.
- Pull Request: #13 – `feat: producer de lecturas de cadena de frío (#7)`.
- Tópico Kafka: `rapidgo.coldchain.readings`.

## 6. Trabajo pendiente de integración

La validación del productor no implica que el pipeline completo esté terminado. La integración con Spark Structured Streaming, las agregaciones, la persistencia en Parquet, los checkpoints y la integración continua deben verificarse según el avance del equipo.
