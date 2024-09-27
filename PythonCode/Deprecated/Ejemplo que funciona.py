import requests
import pandas as pd
from sqlalchemy import create_engine

# API data
url = 'https://api.nasa.gov/neo/rest/v1/feed?'
paramurl= 'start_date=2024-09-10&end_date=2024-09-11&'
apikey = 'api_key=c50CVxpAev2KP5F4GOnAVrCSHvQCgx0bA0tg1xwq'

finalurl = url+paramurl+apikey

#Connection data: 
user = '2024_juan_franco_torrez'
password = 'L9&!2^Q$x4R'
host = 'redshift-pda-cluster.cnuimntownzt.us-east-2.redshift.amazonaws.com'
port = '5439'
database = 'pda'

connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)


try:
    with engine.connect() as connection:
        print("Connection to Redshift successful!")
        # Example query to test connection
        result = connection.execute("SELECT current_date;")
        for row in result:
            print("Current date in Redshift:", row[0])
except Exception as e:
    print(f"Error connecting to Redshift: {e}")


data = requests.get(finalurl)
#data = data.json()

sizes_dict = {
	"Argentina": 2780400,
	"Brasil": 8515767,
	"Francia": 551695,
	"Uruguay": 176215,
	"China": 9596960
}


population_dict = {
	"Argentina": 46044763,
	"Brasil": 203062512,
	"Francia": 68128000,
	"Uruguay": 3554915,
	"China": 1411750000
}

sizes = pd.Series(sizes_dict)
population = pd.Series(population_dict)

countries_dict = {"sized": sizes, "population": population}



df = pd.DataFrame(countries_dict)

print(df)

df.to_sql(name='NasaTestTable', con=engine, schema='2024_juan_franco_torrez_schema', if_exists='append', index=False)