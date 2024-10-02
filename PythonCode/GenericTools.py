from sqlalchemy import create_engine
from dotenv import load_dotenv  
import os



def database_conection():

    load_dotenv()

    #Connection data: 
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    database = os.getenv("DB_NAME")

    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)

    return engine
