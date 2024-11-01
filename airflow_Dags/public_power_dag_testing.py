from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Set default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'public_power_data_ingestion',
    default_args=default_args,
    description='A DAG to run the public power data ingestion script',
    schedule_interval=timedelta(days=1),  # Adjust the schedule as needed
    catchup=False,
)

# Define the path to your Python virtual environment and script
venv_path = '/Users/izrumhaider/Desktop/final/pipeline'  # Replace with the absolute path to your virtual environment
script_path = '/Users/izrumhaider/Desktop/final/scr/public.power.py'  # Replace with the absolute path to your script

# Define the task to execute your Python script
fetch_data_task = BashOperator(
    task_id='fetch_and_save_power_data',
    bash_command=f'source {venv_path}/bin/activate && python {script_path}',
    dag=dag,
)

# Set task dependencies if needed (for now, it's just a single task)
