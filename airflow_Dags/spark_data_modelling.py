from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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
    'Fact_and_Dimensions',
    default_args=default_args,
    description='DAG to run Spark job for fact and dimension tables.',
    schedule=timedelta(days=1),  # Use 'schedule' instead of 'schedule_interval'
    catchup=False,
) as dag:


    run_dim_table = BashOperator(
        task_id='Run_dim_Production_Type_Script',
        bash_command='python ~/Desktop/new/production_type.py'
    )

    run_fact_table = BashOperator(
        task_id='Run_Fact_Public_Power_Script',
        bash_command='python ~/Desktop/new/fact_public_power.py'
    )
    # Add task dependencies (if there are more tasks)
    run_dim_table >> run_fact_table