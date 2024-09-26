import requests
import pandas as pd
from sqlalchemy import create_engine
import json

# API data
url = 'https://api.nasa.gov/neo/rest/v1/feed?'
paramurl= 'start_date=2024-09-09&end_date=2024-09-11&'
apikey = 'api_key=c50CVxpAev2KP5F4GOnAVrCSHvQCgx0bA0tg1xwq'

finalurl = url+paramurl+apikey

print(finalurl)

#Connection data: 
user = '2024_juan_franco_torrez'
password = 'L9&!2^Q$x4R'
host = 'redshift-pda-cluster.cnuimntownzt.us-east-2.redshift.amazonaws.com'
port = '5439'
database = 'pda'

connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)


data = requests.get(finalurl)
data = data.json()

# Inicializar una lista para almacenar los datos
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


#dejo lo que necesito y saco los duplicados.
asteroids_js = asteroid_near_earthd_df[['asteroid_id', 'asteroid_name','absolute_magnitude_h','min_estimate_diameter_km','min_estimate_diameter_km']]
asteroids_js = asteroids_js.drop_duplicates()

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
existing_asteroids = pd.read_sql('select distinct asteroid_id from "2024_juan_franco_torrez_schema".asteroids', engine)
existing_ids_set = set(existing_asteroids['asteroid_id'])

# Filtrar los nuevos datos que no esten en la tabla
asteroids_toinsert_df = asteroids_js[~asteroids_js['asteroid_id'].isin(existing_ids_set)]

print(asteroids_toinsert_df)

# Insertando solo asteroides que no existen
asteroids_toinsert_df.to_sql(name='asteroids', con=engine, schema='2024_juan_franco_torrez_schema', if_exists='append', index=False)

