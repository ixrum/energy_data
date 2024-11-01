from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess

# Define the function that will run your Spark job
#def run_spark_job():
#    subprocess.run([
#        "/opt/anaconda3/bin/spark-submit",  # Replace with the actual path to your spark-submit
        #"--packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-client:3.3.1",
#        "/Users/izrumhaider/Desktop/new/power_data_ingestion.py"  # Replace with your actual script path
#    ], check=True)

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'spark_public_power_dag',
    default_args=default_args,
    description='DAG to run Spark job',
    schedule=timedelta(days=1),  # Use 'schedule' instead of 'schedule_interval'
    catchup=False,
) as dag:

    # Define the task
#    run_spark_task = PythonOperator(
#        task_id='run_spark_job',
#        python_callable=run_spark_job,
#    )
    run_script = BashOperator(
        task_id='Run_PowerDataIngestion_Script',
        bash_command='python ~/Desktop/new/power_data_ingestion.py'
    )
    # Add task dependencies (if there are more tasks)
    run_script

