# Asteroids Near Earth ETL Project

## Descripción
Este proyecto es un pipeline ETL (Extract, Transform, Load) diseñado para extraer información sobre asteroides cercanos a la Tierra utilizando la API de la NASA, transformar los datos y cargar los resultados en una base de datos Redshift.

## Modelo de datos


El proyecto utiliza dos tablas principales para almacenar la información relacionada con los asteroides y sus acercamientos.

### Tabla 1: `dim_asteroids`

| Columna                   | Tipo de Datos   | Descripción                                         |
|---------------------------|-----------------|-----------------------------------------------------|
| `asteroid_id`              | `int4`          | Identificador único del asteroide.                  |
| `asteroid_name`            | `varchar(256)`  | Nombre del asteroide.                               |
| `absolute_magnitude_h`     | `float8`        | Magnitud absoluta del asteroide.                    |
| `min_estimate_diameter_km` | `float8`        | Diámetro estimado mínimo en kilómetros.             |
| `max_estimate_diameter_km` | `float8`        | Diámetro estimado máximo en kilómetros.             |

### Tabla 2: `fact_asteroidsnearearth`

| Columna                              | Tipo de Datos   | Descripción                                         |
|--------------------------------------|-----------------|-----------------------------------------------------|
| `date`                               | `timestamp`     | Fecha y hora del acercamiento del asteroide.        |
| `asteroid_id`                        | `int4`          | Identificador único del asteroide (FK con `dim_asteroids`). |
| `is_potentially_hazardous_asteroid`  | `bool`          | Indica si el asteroide es potencialmente peligroso. |
| `velocity_km_sec`                    | `float8`        | Velocidad del asteroide en kilómetros por segundo.   |
| `miss_lunar_distance`                | `float8`        | Distancia a la que pasó el asteroide medida en distancias lunares. |
| `miss_km_distance`                   | `float8`        | Distancia a la que pasó el asteroide en kilómetros.  |
| `miss_astronomical_distance`         | `float8`        | Distancia a la que pasó el asteroide en unidades astronómicas. |




## Tabla de Contenidos
- [Instalación](#instalación)
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
2. Agregar las credenciales enviadas por slack en el archivo .env ubicado en la carpeta \PythonCode
3. Abrir terminal
4. Ubicarse en AsteroidsNearEarth\Airflow\
5. Correr: 
    ```bash
    docker compose up

Esto va a bajar las images y levantar los containers de postgress, reddit  y servicios de airflow necesarios para la ejecucion de applicativo.

6. Una vez instalado en un web browser dirigirse a la dirección: http://localhost:8080/ , esto abrira la interfaz web de Airflow. Ingresar con las credenciales de Airflow enviadas.


## Estructura del Proyecto
    ```bash
    ASTEROIDSNEAREARTH/
    │
    ├── .github/
    │   └── workflows/
    │       └── run-test.yaml          # Archivo YAML para automatización de tests
    │
    ├── Airflow/                       # Carpeta para DAGS y configuración de Airflow
    │
    ├── PythonCode/
    │   ├── dags/                      # DAGS de Airflow
    │   ├── tests/                     # Carpeta de tests para pytest
    │   ├── AsteroidsNearEarth_ETL.py  # Script principal del pipeline ETL
    │   └── GenericTools.py            # Herramientas o utilidades genéricas para el proyecto
    │
    ├── SqlScripts/                    # Scripts SQL utilizados por el proyecto
    │
    ├── pytest.ini                     # Archivo de configuración para pytest
    │
    ├── README.md                      # Archivo README con documentación del proyecto
    │
    └── requirements.txt               # Archivo con las dependencias del proyecto


## Variables de Entorno

El proyecto utiliza algunas variables de entorno que deben configurarse en un archivo .env en la raíz del proyecto.

    ```bash
    API_NASA_URL=   #Direccion de la API
    API_NASA_KEY=   #Key necesaria para hacer el request a la API
    DB_USER=        #Usuario de base de datos
    DB_PASSWORD=    #Password para la base de datos
    DB_HOST=        #Host de redshift
    DB_PORT=        #Puerto de redshift
    DB_NAME=        #Nombre de la base da datos.

## Test

El proyecto incluye un conjunto de tests definidos en la carpeta PythonCode/tests. Para ejecutarlos, usa pytest:
    ```bash
    
    pytest


## Automatización con GitHub Actions
El proyecto está configurado para ejecutar los tests automáticamente en cada push y pull request utilizando GitHub Actions. La configuración se encuentra en .github/workflows/python-app.yml.