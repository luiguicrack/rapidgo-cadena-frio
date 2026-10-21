# Checkpoint Semana 1 – Grupo 1: Cadena de frío

# 

# 1\. Qué quedó preparado y verificado

# 

# Se definió la arquitectura inicial de RapidGo para la variante Cadena de frío, el contrato JSON de las lecturas, tres KPIs y dos reglas de alerta. Se preparó Docker Compose y se verificó la creación del topic de Kafka.

# 

# 2\. Evidencia real

# 

# docker compose config --quiet: terminó sin errores.

# 

# docker compose up -d: inició el contenedor coldchain-kafka.

# 

# El servicio kafka-init creó rapidgo.coldchain.readings.

# 

# La consulta kafka-topics.sh --describe confirmó 3 particiones y factor de replicación 1; las tres particiones tenían líder e ISR asignados.

# 

# 3\. Decisión técnica

# 

# Se eligió order\_id como key y 3 particiones para mantener las lecturas de cada pedido en una misma partición y permitir procesamiento paralelo. La decisión y sus ventajas y limitaciones están documentadas en ADR-001.

# 

# 4\. Riesgo principal

# 

# Los datos simulados podrían producir pocos episodios fuera de rango o una distribución desigual entre particiones. En la Semana 2 se comprobará la distribución real y se ajustarán las reglas si es necesario.

# 

# 5\. Plan para la Semana 2

# 

# Implementar el productor de eventos con la key order\_id.

# 

# Consumir y transformar eventos con Spark Structured Streaming.

# 

# Calcular indicadores y generar las dos alertas.

# 

# Persistir resultados en Parquet y configurar checkpoints.

# 

# Añadir pruebas automatizadas y GitHub Actions.

# 

# 6\. Enlaces

# 

# Repositorio: https://github.com/luiguicrack/rapidgo-cadena-frio

# 

# Tablero Kanban: pendiente de crear.

# 

# Issues: pendientes de crear.

# 

# Release v0.1.0-foundation: pendiente de publicar.

