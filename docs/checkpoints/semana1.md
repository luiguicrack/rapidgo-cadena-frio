# Checkpoint Semana 1 – Grupo 1: Cadena de frío

**Resultado:** se preparó la arquitectura de la variante 3, el contrato JSON, tres KPIs, dos reglas de alerta y el entorno inicial de Kafka.

**Evidencia:** `docker compose config --quiet` terminó sin errores. `docker compose up -d` inició Kafka y el servicio `kafka-init` creó `rapidgo.coldchain.readings`. La consulta `kafka-topics.sh --describe` confirmó tres particiones y factor de replicación 1, con líder asignado en cada partición.

**Decisión principal:** utilizar `order_id` como key y tres particiones para agrupar las lecturas de cada pedido y permitir procesamiento paralelo. La evidencia valida la configuración del topic; el comportamiento de los eventos se verificará en la Semana 2.

**Riesgo principal:** una distribución desigual de pedidos entre particiones o una cantidad insuficiente de lecturas fuera de rango para evaluar las alertas.

**Plan de Semana 2:** implementar el productor; consumir y transformar lecturas con Spark Structured Streaming; calcular KPIs y alertas; persistir resultados en Parquet con checkpoints; y preparar pruebas automatizadas e integración continua.

**Repositorio:** https://github.com/luiguicrack/rapidgo-cadena-frio  
**Tablero:** proyecto GitHub *RapidGo - Cadena de frío*.  
**Planificación:** 12 Issues distribuidas entre las cuatro semanas.  
**Release publicada:** `v0.1.0-foundation`.
