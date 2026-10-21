# Evidencia utilizada

# 

# Se ejecutó docker compose config --quiet y la configuración terminó sin errores. Después, docker compose up -d inició Kafka y el servicio kafka-init creó el topic rapidgo.coldchain.readings.

# 

# La consulta kafka-topics.sh --describe mostró:

# 

# PartitionCount: 3

# 

# ReplicationFactor: 1

# 

# Particiones 0, 1 y 2 con líder asignado y réplicas en ISR.

# 

# La prueba confirma la creación del topic y su configuración inicial. Todavía no demuestra el orden de los mensajes por order\_id ni el balance de eventos entre particiones; esas propiedades se verificarán al implementar el productor en la Semana 2.

