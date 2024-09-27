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
