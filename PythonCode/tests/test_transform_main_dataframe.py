import pandas as pd
import sys
import os

# Agregar la carpeta un nivel superior al sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
import AsteroidsNearEarth_ETL as my_module

# Test para transformar los datos principales
def test_transform_main_dataframe():
    data = {
        'near_earth_objects': {
            '2024-09-01': [
                {'neo_reference_id': '123', 'name': 'Asteroid 1', 'absolute_magnitude_h': 22.1, 'is_potentially_hazardous_asteroid': False,
                 'estimated_diameter': {'kilometers': {'estimated_diameter_min': 0.1, 'estimated_diameter_max': 0.3}},
                 'close_approach_data': [{'relative_velocity': {'kilometers_per_second': '25.0'}, 'miss_distance': {'lunar': '10', 'kilometers': '384400', 'astronomical': '0.0026'}}]
                }
            ]
        }
    }
    
    df = my_module.transform_main_dataframe(data)
    
    assert isinstance(df, pd.DataFrame) , "no se creo un datafram"
    assert not df.empty , "main dataframe vacio"
    assert 'asteroid_id' in df.columns , "no se generaron las columnas correspondientes"
    assert 'asteroid_name' in df.columns , "no se generaron las columnas correspondientes"