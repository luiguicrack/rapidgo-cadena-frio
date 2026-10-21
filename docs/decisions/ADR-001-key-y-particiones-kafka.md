# ADR-001: Key y particiones de Kafka

## Estado
Aceptada para la arquitectura inicial de Semana 1.

## Contexto y problema
RapidGo necesita transportar lecturas de temperatura y humedad asociadas a pedidos que contienen productos refrigerados o congelados. Se requiere conservar el orden de las lecturas de un mismo pedido dentro de una partición y permitir el procesamiento paralelo.

## Opciones consideradas
- Usar `order_id` como key: agrupa las lecturas de un mismo pedido en una misma partición.
- Usar `vehicle_id` como key: agrupa las lecturas por vehículo, pero no necesariamente por pedido.
- No usar una key: permite una distribución menos controlada y no garantiza que las lecturas de un pedido queden en la misma partición.

## Decisión
Se utilizará `order_id` como key del topic `rapidgo.coldchain.readings`, con tres particiones y factor de replicación 1 en el entorno local inicial.

## Justificación y evidencia
Se ejecutó `docker compose config --quiet` sin errores y se inició Kafka mediante `docker compose up -d`. El servicio `kafka-init` creó el topic `rapidgo.coldchain.readings`. La consulta `kafka-topics.sh --describe` confirmó tres particiones, factor de replicación 1 y líder asignado en cada partición.

Esta prueba valida la creación y configuración inicial del topic. La distribución real de eventos y el orden por `order_id` se verificarán cuando se implemente el productor.

## Consecuencias y limitaciones
La key permite que los eventos de un mismo pedido se asignen a una misma partición, conservando su orden dentro de ella. Las tres particiones permiten procesamiento paralelo entre pedidos. La replicación 1 es adecuada para las pruebas locales, pero no ofrece tolerancia a la pérdida de un broker.
