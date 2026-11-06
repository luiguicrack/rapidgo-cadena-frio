# Calidad y recuperación del streaming

## Validaciones de calidad

Spark conserva cada mensaje recibido en `datalake/raw/events`. Después valida
campos requeridos, formatos, equipo, rangos del contrato y que la clave de
Kafka coincida con `order_id`. Los campos de texto vacíos o compuestos solo por
espacios y los valores numéricos NaN se rechazan. Cada rechazo se guarda con su
motivo en `datalake/processed/rejected`; únicamente los eventos aceptados pasan
a lecturas procesadas, alertas e indicadores.

El esquema JSON también exige que los campos de texto requeridos no estén
vacíos. Las pruebas del contrato cubren estos casos junto con valores fuera de
rango y formatos inválidos.

## Recuperación de Spark

Cada salida de Structured Streaming tiene su propio checkpoint persistido bajo
`datalake/checkpoints/spark`. El servicio `spark-streaming` usa la política
`restart: unless-stopped`. Cuando una consulta falla, la aplicación registra la
excepción y termina con error; Docker reinicia el contenedor y Spark reanuda las
consultas desde sus checkpoints.

Para inspeccionar una recuperación:

```bash
docker compose ps
docker compose logs --tail=100 spark-streaming
docker inspect --format='{{.RestartCount}}' coldchain-spark-streaming
```

Para reiniciar manualmente el servicio después de corregir la causa del fallo:

```bash
docker compose restart spark-streaming
```

No elimines ni cambies las rutas de checkpoints para intentar recuperarte de un
fallo. Spark requiere que una consulta se reanude con el checkpoint compatible
con su configuración; borrar el estado puede volver a procesar datos o perder
el progreso registrado. Si el checkpoint está dañado o cambió la consulta,
detén el servicio y conserva una copia del directorio antes de definir una ruta
nueva y validar el reprocesamiento.

La política de reinicio recupera fallos del proceso, pero no reemplaza la
supervisión del estado del DAG de Airflow ni demuestra por sí sola una
recuperación de extremo a extremo. Esa comprobación requiere provocar un
reinicio controlado y verificar offsets, salidas y logs con el entorno Kafka,
Spark y Data Lake disponible.
