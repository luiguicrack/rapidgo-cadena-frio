\# Evidencias de integración – Semana 4



\*\*Proyecto:\*\* RapidGo – Cadena de frío  

\*\*Grupo:\*\* 1  

\*\*Responsable de la evidencia:\*\* Steban Blanco  

\*\*Rama:\*\* steban



\## 1. Preparación del entorno



Se trabajó desde el repositorio clonado en un segundo equipo. Se verificó la conexión con GitHub y se comprobó que la rama de trabajo estuviera actualizada.



\## 2. Ejecución de Kafka



Se inició el entorno mediante Docker Compose.



Se verificó que el servicio Kafka estuviera funcionando y que existiera el topic `rapidgo.coldchain.readings`, configurado con tres particiones y una réplica.



\*\*Resultado:\*\* \[Completar con el resultado obtenido].



\## 3. Generación y envío de eventos



Se ejecutó el productor para generar lecturas simuladas de temperatura y humedad.



Los eventos fueron validados utilizando el contrato JSON del proyecto y enviados a Kafka utilizando el identificador del pedido como clave.



\*\*Cantidad de eventos enviados:\*\* \[Completar].



\*\*Resultado:\*\* \[Indicar si los mensajes llegaron correctamente].



\## 4. Recepción de mensajes



Se utilizó un consumidor de Kafka para comprobar que los eventos enviados por el productor llegaran correctamente al topic.



\*\*Resultado:\*\* \[Completar].



\## 5. Pruebas automatizadas



Se ejecutaron las pruebas del proyecto mediante pytest.



\*\*Resultado:\*\* \[Cantidad de pruebas aprobadas y fallidas].



También se verificó el workflow de integración continua de GitHub Actions.



\*\*Resultado del workflow:\*\* \[Completar].



\## 6. Evidencias



Se adjuntan capturas de:



\- Servicios de Docker en ejecución.

\- Configuración del topic de Kafka.

\- Productor enviando eventos.

\- Consumidor recibiendo mensajes.

\- Ejecución de las pruebas automatizadas.

\- Workflow de GitHub Actions.



\## 7. Conclusión



Durante esta etapa se realizaron pruebas para comprobar la generación de eventos, su envío a Kafka y la recepción de los mensajes. También se verificó el funcionamiento de las pruebas automatizadas.



Estas actividades permitieron comprobar el funcionamiento de los componentes probados y dejar evidencia del avance realizado durante la Semana 4.

