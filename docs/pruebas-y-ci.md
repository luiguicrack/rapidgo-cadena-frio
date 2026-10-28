\# Pruebas automatizadas e integración continua



\## 1. Objetivo



Verificar automáticamente los componentes de RapidGo – Cadena de frío mediante pruebas automatizadas y un workflow de integración continua con GitHub Actions.



\## 2. Herramientas utilizadas



\- Python 3.13

\- pytest 8.3.3

\- jsonschema 4.23.0

\- GitHub Actions



Las dependencias del proyecto se encuentran en `requirements.txt`.



\## 3. Pruebas automatizadas



Las pruebas se encuentran en la carpeta `tests/`.



\- `test\_contrato.py`: verifica la validación de eventos frente al contrato de datos, incluidos campos obligatorios, valores inválidos y propiedades adicionales.

\- `test\_generador.py`: verifica la generación de eventos, su validez frente al contrato, la unicidad de identificadores y el orden de las marcas de tiempo.

\- `test\_reglas\_alerta.py`: verifica la lógica de las reglas de alerta.



\## 4. Ejecución local



Instalar las dependencias:



```bash

python -m pip install -r requirements.txt

```



Ejecutar las pruebas:



```bash

python -m pytest -v

```



\## 5. Integración continua



El workflow está definido en `.github/workflows/ci.yml`.



Se ejecuta ante eventos `push` a la rama `main`, solicitudes de integración (`pull\_request`) hacia `main` y ejecuciones manuales mediante `workflow\_dispatch`.



El workflow utiliza Ubuntu, configura Python 3.13, instala las dependencias y ejecuta `python -m pytest -v`.



\## 6. Resultado de la validación



En la validación local realizada se ejecutaron 51 pruebas y todas finalizaron correctamente.



La ejecución de GitHub Actions correspondiente al workflow también se verificó con resultado exitoso.



\## 7. Evidencias



\- Workflow: `.github/workflows/ci.yml`

\- Pruebas: carpeta `tests/`

\- Dependencias: `requirements.txt`

\- Ejecuciones: \[GitHub Actions del proyecto](https://github.com/luiguicrack/rapidgo-cadena-frio/actions)



Para futuras ejecuciones, se debe comprobar el resultado de las pruebas tanto localmente como en GitHub Actions.



