from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
#from AsteroidsNearEarth_ETL import main_etl_asteriods_near_earth
import requests
import pandas as pd
from sqlalchemy import create_engine
#from GenericTools import database_conection
import json
from sqlalchemy import create_engine


def database_conection():

    #Connection data: 
    user = '2024_juan_franco_torrez'
    password = 'L9&!2^Q$x4R'
    host = 'redshift-pda-cluster.cnuimntownzt.us-east-2.redshift.amazonaws.com'
    port = '5439'
    database = 'pda'

    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)

    return engine



def extract_api_conection(apikey:str):
    
    # API data
    url = 'https://api.nasa.gov/neo/rest/v1/feed?'
    paramurl= 'start_date=2024-09-11&end_date=2024-09-12&'
    #apikey = 'api_key=c50CVxpAev2KP5F4GOnAVrCSHvQCgx0bA0tg1xwq'

    finalurl = url+paramurl+apikey

    data = requests.get(finalurl)
    data = data.json()

    return data


def transform_main_dataframe(data:any):

    asteroid_near_earth = []

    # Iterar sobre todas las fechas en el JSON
    for date, objects in data['near_earth_objects'].items():
        # Para cada objeto (asteroide) en la fecha, extraer 'id', 'name' y agregar la fecha
        for obj in objects:
            asteroid_near_earth.append({
                'date': date,
                'asteroid_id': obj['neo_reference_id'],
                'asteroid_name': obj['name'],
                'absolute_magnitude_h': obj['absolute_magnitude_h'],
                'is_potentially_hazardous_asteroid': obj['is_potentially_hazardous_asteroid'],
                'min_estimate_diameter_km': obj['estimated_diameter']['kilometers']['estimated_diameter_min'],
                'max_estimate_diameter_km': obj['estimated_diameter']['kilometers']['estimated_diameter_max'],

                'velocity_km_sec': obj['close_approach_data'][0]['relative_velocity']['kilometers_per_second'],
                'miss_lunar_distance': obj['close_approach_data'][0]['miss_distance']['lunar'],
                'miss_km_distance': obj['close_approach_data'][0]['miss_distance']['kilometers'],
                'miss_astronomical_distance': obj['close_approach_data'][0]['miss_distance']['astronomical']
                
            })

        # Convertir la lista a un DataFrame
        asteroid_near_earthd_df = pd.DataFrame(asteroid_near_earth)
    return asteroid_near_earthd_df


def transform_dimensions(engine:any, asteroid_near_earthd_df:any):
    
    #dejo lo que necesito y saco los duplicados.
    asteroids_js = asteroid_near_earthd_df[['asteroid_id', 'asteroid_name','absolute_magnitude_h','min_estimate_diameter_km','min_estimate_diameter_km']]
    asteroids_js = asteroids_js.drop_duplicates()

    #Conversiones
    asteroids_js['asteroid_id'] = pd.to_numeric(asteroids_js['asteroid_id'])

    #Me conecto a la base
    try:
        with engine.connect() as connection:
            print("Connection to Redshift successful!")
            # Example query to test connection
            #result = connection.execute("SELECT current_date;")
            #for row in result:
            #    print("Current date in Redshift:", row[0])
    except Exception as e:
        print(f"Error connecting to Redshift: {e}") 


    # Verificar los IDs existentes en la tabla
    existing_asteroids = pd.read_sql('select distinct asteroid_id from "2024_juan_franco_torrez_schema".dim_asteroids', engine)

    # los convierto a un set
    existing_ids_set = set(existing_asteroids['asteroid_id'])

    # Filtrar los nuevos datos que no esten en la tabla
    asteroids_to_insert = asteroids_js[~asteroids_js['asteroid_id'].isin(existing_ids_set)]

    return asteroids_to_insert


def transform_fact(engine:any, asteroid_near_earthd_df:any):

    #dejo lo que necesito y saco los duplicados.
    fact_js = asteroid_near_earthd_df[['date', 'asteroid_id','is_potentially_hazardous_asteroid','velocity_km_sec','miss_lunar_distance','miss_km_distance','miss_astronomical_distance']]
    fact_to_insert = fact_js.drop_duplicates()

    #Conversiones
    fact_to_insert['date'] = pd.to_datetime(fact_to_insert['date'])
    fact_to_insert['asteroid_id'] = pd.to_numeric(fact_to_insert['asteroid_id'])
    fact_to_insert['is_potentially_hazardous_asteroid'] = pd.to_numeric(fact_to_insert['is_potentially_hazardous_asteroid'])
    fact_to_insert['velocity_km_sec'] = pd.to_numeric(fact_to_insert['velocity_km_sec'])
    fact_to_insert['miss_lunar_distance'] = pd.to_numeric(fact_to_insert['miss_lunar_distance'])
    fact_to_insert['miss_km_distance'] = pd.to_numeric(fact_to_insert['miss_km_distance'])
    fact_to_insert['miss_astronomical_distance'] = pd.to_numeric(fact_to_insert['miss_astronomical_distance'])

    # Extraer las fechas únicas del DataFrame
    dates_to_delete_lst = fact_to_insert['date'].dt.strftime('%Y-%m-%d').unique().tolist()

    # genero un string
    dates_str = "', '".join(dates_to_delete_lst)
    query = f'DELETE FROM "2024_juan_franco_torrez_schema".fact_asteroidsnearearth WHERE date IN (\'{dates_str}\')'

    #Me conecto a la base
    try:
        with engine.connect() as connection:
            print("Connection to Redshift successful!")
            # Example query to test connection
            #result = connection.execute("SELECT current_date;")
            #for row in result:
            #    print("Current date in Redshift:", row[0])
    except Exception as e:
        print(f"Error connecting to Redshift: {e}") 

    # Ejecuto el delete
    engine.execute(query)

    return fact_to_insert


def load_tables(engine:any, asteroids_to_insert:any, fact_to_insert:any):

    #Me conecto a la base
    try:
        with engine.connect() as connection:
            print("Connection to Redshift successful!")
            # Example query to test connection
            #result = connection.execute("SELECT current_date;")
            #for row in result:
            #    print("Current date in Redshift:", row[0])
    except Exception as e:
        print(f"Error connecting to Redshift: {e}") 

    
    try: #validacion de inserts

        # Insertando asteroides
        asteroids_to_insert.to_sql(name='dim_asteroids', con=engine, schema='2024_juan_franco_torrez_schema', if_exists='append', index=False)

        # inserto fact table 
        fact_to_insert.to_sql(name='fact_asteroidsnearearth', con=engine, schema='2024_juan_franco_torrez_schema', if_exists='append', index=False)

        # Si esta todo ok, retorna true
        return True, "Data inserted successfully."
    
    except Exception as e:
        # si hay error, retorna False y el mensaje de error
        return False, f"Error during insert: {str(e)}"

    
def main_etl_asteriods_near_earth():

    js = extract_api_conection('api_key=c50CVxpAev2KP5F4GOnAVrCSHvQCgx0bA0tg1xwq')

    df = transform_main_dataframe(js)
    engine = database_conection()


    dim_df=transform_dimensions(engine,df)
    fact_df=transform_fact(engine,df)

    success,message = load_tables(engine,dim_df,fact_df)

    # Verificar el resultado
    if success:
        print("Inserts were successful.")
        return
    else:
        print(f"Inserts failed: {message}")
        return


########################################################################################
###################### DAG
########################################################################################


with DAG(
    'etl_asteroids_near_earth_ETL',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description= 'ETL pipeline to extract, transform and load data on the asteroids datawarehouse created by Juan Franco Torrez',
    schedule_interval= '@daily',
    start_date= datetime(2024,9,27),
    catchup=True,
)as dag:

    # Task 1 main execution
    main_etl_asteriods_near_earth = PythonOperator(
        task_id = 'main_etl_asteriods_near_earth',
        python_callable= main_etl_asteriods_near_earth

    )

    #set task dependencies
