import requests
import pandas as pd
from sqlalchemy import create_engine
from GenericTools import database_conection
import json
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta


def extract_obtain_last_date(engine:any):

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

    # en caso de no haber registros, se tomará como fecha base 14 días hacia atras.
    max_date_df = pd.read_sql('select isnull(cast(max(date) as date), cast(dateadd(d,-14,getdate()) as date)) from "2024_juan_franco_torrez_schema".fact_asteroidsnearearth', engine)
    
    max_date = max_date_df[max][0]

    print(f"Max date obtained from table: {max_date}")

    return max_date


def extract_range_of_dates(from_date:any):

    from_date = pd.to_datetime(from_date,format="%Y-%m-%d")
        
    # Definir la to_date como hoy
    to_date = pd.to_datetime(datetime.now().date(),format="%Y-%m-%d")

    ranges = []

    while from_date <= to_date:

        end_date = from_date + timedelta(days=7)
        
        # Asegurarse de que la fecha_fin no exceda fecha_hasta
        if end_date > to_date:
            end_date = to_date
            
        # Agregar el rango a la lista
        ranges.append({'from_date': from_date, 'to_date': end_date})
        
        # Avanzar a la siguiente fecha_desde (7 días después)
        from_date += timedelta(days=7)

    # Crear un DataFrame a partir de la lista de rangos
    df_rangos = pd.DataFrame(ranges)

    return df_rangos

def extract_api_conection(from_date:any, to_date:any):

    load_dotenv()
    
    # API data
    url = os.getenv("API_NASA_URL")
    apikey = 'api_key=' + os.getenv("API_NASA_KEY")
    
    paramurl= 'start_date='+from_date+'&end_date='+to_date+'&'
    finalurl = url+paramurl+apikey

    data = requests.get(finalurl)
    data = data.json()
    print(finalurl)
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

    js = extract_api_conection()

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

#main_etl_asteriods_near_earth()



engine = database_conection()
from_date = extract_obtain_last_date(engine)

df = extract_range_of_dates(from_date)

print(df)

for index, row in df.iterrows():
    from_date_str = str(row['from_date'])
    to_date_str = str(row['to_date'])

    js = extract_api_conection(from_date_str,to_date_str)
    df = transform_main_dataframe(js)
    dim_df=transform_dimensions(engine,df)
    fact_df=transform_fact(engine,df)
    success,message = load_tables(engine,dim_df,fact_df)

     # Verificar el resultado
    if success:
        print(f"Inserts for {from_date_str} -  {to_date_str} were successful.")
    else:
        print(f"Inserts failed: {message}")









