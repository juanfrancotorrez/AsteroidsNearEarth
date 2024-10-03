import pytest
from unittest import mock
from datetime import datetime
import pandas as pd
import sys
import os

# Agregar la carpeta un nivel superior al sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
import AsteroidsNearEarth_ETL as my_module

# Test para generar rangos de fechas
def test_extract_range_of_dates():
    from_date = datetime(2024, 9, 1)
    ranges = my_module.extract_range_of_dates(from_date)

    assert 'from_date' in ranges.columns , "No existe la columna from_date en el rango"
    assert 'to_date' in ranges.columns, "No existe la columna to_date en el rango"
    assert not ranges.empty , "El rango esta vacio, revisar la generacion de rangos"
