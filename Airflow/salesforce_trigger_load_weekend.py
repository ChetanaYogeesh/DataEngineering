from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from product_salesforce_to_sf_functions import *
from airflow.models import Variable
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime, timedelta

# DAG: Weekends Orchestration
dag = DAG(
    'product_salesforce_orchestration_dag_weekend',
    start_date=datetime(2024, 1, 29, 8, 0, 0),
    max_active_runs=1,
    schedule_interval='0 */4 * * 6,7',  # This schedule runs every 4 hours on Saturday and Sunday
    tags=['FIVETRAN', 'DBT', 'TABLEAU'],
    default_args={
                    'owner': 'airflow',
                    'depends_on_past': False,
                    'max_active_tasks': 10,
                    'retries': 3,
                    'on_failure_callback': on_failure_callback,
                    'retry_delay': timedelta(minutes=1)
    },
)

DBT_JOB = Variable.get('product_salesforce_dbt_job')

# DBT operator to run DBT job
run_dbt_task_weekend = DbtCloudRunJobOperator(
            task_id='run_dbt_task',
            job_id = DBT_JOB,
            dbt_cloud_conn_id='dbt_product_conn',
            trigger_reason="Triggered via Apache Airflow in the example_fivetran_dbt_dag DAG",
            trigger_rule='all_success',
            check_interval=30,
            wait_for_termination=True,
            timeout=3600,
            dag = dag,
        )

# Define the tasks
trigger_fivetran_op_weekend = PythonOperator(
    task_id='trigger_fivetran_sync',
    python_callable=trigger_fivetran_sync,
    dag=dag,
)

refresh_tableau_op_weekend = PythonOperator(
    task_id='refresh_tableau',
    python_callable=refresh_tableau,
    dag=dag,
)

trigger_fivetran_op_weekend >> run_dbt_task_weekend >> refresh_tableau_op_weekend 
