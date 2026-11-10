# ADR-004: Validación de la integración del proyecto



- **Estado:** Aceptada

- **Semana:** 4

- **Proyecto:** RapidGo - Cadena de Frío



## Contexto



Durante el desarrollo del proyecto se necesitaba comprobar que el productor de lecturas pudiera comunicarse con Kafka y que los eventos generados llegaran correctamente al tópico.



También era necesario verificar que las reglas de validación y las pruebas automatizadas funcionaran correctamente.



## Decisión



Se decidió utilizar Docker Compose para levantar Kafka localmente y realizar las pruebas de integración utilizando el productor desarrollado en Python y un consumidor de consola de Kafka.



Además, se utilizaron pruebas automatizadas con Pytest para validar el contrato de eventos, el generador y las reglas de alerta.



## Justificación



Esta alternativa permite comprobar el envío y la recepción de lecturas sin necesidad de instalar Kafka directamente en Windows.



También facilita repetir las pruebas, identificar errores y conservar evidencias del funcionamiento de los componentes.



## Resultado



Se verificó la disponibilidad del tópico `rapidgo.coldchain.readings`, configurado con tres particiones.



La ejecución de las pruebas automatizadas finalizó con 51 pruebas aprobadas.



Las pruebas de envío y recepción de mensajes permitieron validar el flujo entre el productor y Kafka.



## Consecuencias



- Se puede probar Kafka en un entorno local.

- Se facilita la detección de errores.

- Se pueden repetir las pruebas durante el desarrollo.

- Se mantiene una validación automatizada del código.

- Las evidencias permiten documentar los resultados obtenidos.



## Limitaciones identificadas



Esta decisión valida los componentes disponibles en el repositorio, pero no implica que estén implementadas las integraciones con Spark Structured Streaming, Data Lake o Airflow.
