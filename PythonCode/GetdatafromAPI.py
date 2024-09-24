import requests
import pandas as pd
from sqlalchemy import create_engine
import json

# API data
url = 'https://api.nasa.gov/neo/rest/v1/feed?'
paramurl= 'start_date=2024-09-10&end_date=2024-09-11&'
apikey = 'api_key=c50CVxpAev2KP5F4GOnAVrCSHvQCgx0bA0tg1xwq'

finalurl = url+paramurl+apikey

#print(finalurl)

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
asteroid_info = []

# Iterar sobre todas las fechas en el JSON
for date, objects in data['near_earth_objects'].items():
    # Para cada objeto (asteroide) en la fecha, extraer 'id', 'name' y agregar la fecha
    for obj in objects:
        asteroid_info.append({
            'id': obj['neo_reference_id'],
            'name': obj['name'],
            'fecha': date
        })

# Convertir la lista a un DataFrame
df = pd.DataFrame(asteroid_info)

# Mostrar el DataFrame
print(df)


#"""
try:
    with engine.connect() as connection:
        print("Connection to Redshift successful!")
        # Example query to test connection
        result = connection.execute("SELECT current_date;")
        for row in result:
            print("Current date in Redshift:", row[0])
except Exception as e:
    print(f"Error connecting to Redshift: {e}") 

df.to_sql(name='JsonFormateado', con=engine, schema='2024_juan_franco_torrez_schema', if_exists='append', index=False)

#"""