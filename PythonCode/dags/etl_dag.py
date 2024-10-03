from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from AsteroidsNearEarth_ETL import main_etl_asteriods_near_earth


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
    catchup=False,
)as dag:

    # Task 1 main execution
    main_etl_asteriods_near_earth = PythonOperator(
        task_id = 'main_etl_asteriods_near_earth',
        python_callable= main_etl_asteriods_near_earth

    )

    #set task dependencies
