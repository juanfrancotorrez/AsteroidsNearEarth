# Asteroids Near Earth ETL Project

## Descripción
Este proyecto es un pipeline ETL (Extract, Transform, Load) diseñado para extraer información sobre asteroides cercanos a la Tierra utilizando la API de la NASA, transformar los datos y cargar los resultados en una base de datos Redshift.

## Tabla de Contenidos
- [Instalación](#instalación)
- [Uso](#uso)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Variables de Entorno](#variables-de-entorno)
- [Tests](#tests)
- [Automatización con GitHub Actions](#automatización-con-github-actions)
- [Contribuciones](#contribuciones)
- [Licencia](#licencia)

## Instalación

Pasos para ejecutar el proceso:

1. Clona el repositorio:
   ```bash
   git clone https://github.com/juanfrancotorrez/AsteroidsNearEarth.git
   cd AsteroidsNearEarth
2. Agregar las credenciales enviadas en el archivo .env ubicado en la carpeta \PythonCode
3. Abrir terminal
4. Ubicarse en AsteroidsNearEarth\Airflow\
5. Correr: docker compose up 
    -Esto va a bajar las images y levantar los containers de postgress, reddit  y servicios de airflow necesarios para la ejecucion de applicativo.
6. En un web browser dirigirse a la dirección: http://localhost:8080/ , esto abrira la interfaz web de Airflow. Ingresar con las siguientes credenciales enviadas.





