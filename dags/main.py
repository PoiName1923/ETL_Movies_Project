import textwrap
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

from medalion import *

default_args = {
    'owner': 'ndtien',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "email": ["htien225@gmail.com"],
}

with DAG(
    dag_id='etl_process',
    default_args=default_args,
    description='First Project With Airflow',
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 10),
    end_date=datetime(2025, 4, 11),
    catchup=False,
    tags=['project'],
) as dag:

    t1 = BashOperator(
        task_id='bronze_process',
        bash_command='python /opt/airflow/dags/medaliop/bronze.py'
    )

    t2 = BashOperator(
        task_id='sliver_process',
        bash_command='python /opt/airflow/dags/medalion/sliver.py'
    )

    t3 = BashOperator(
        task_id='gold_process',
        bash_command='python /opt/airflow/dags/medaliop/gold.py'
    )

    # Thiết lập thứ tự thực hiện: get_movies → bronze → mongodb
    t1 >> t2 >> t3