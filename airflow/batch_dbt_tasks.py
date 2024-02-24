from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.operators.bash_operator import BashOperator 

spark_master = "spark://... : ..."  #Replace (...) with yours

default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2024,1,11),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes=5)
}


dag=DAG(
'spark_and_dbt_dag',
default_args=default_args,
schedule_interval='0 0 * * *',
catchup=False

)

install_requirements_task=BashOperator(
    task_id='install_requirements',
    bash_command='pip install -r /opt/airflow/spark/app/requirement.txt',
    dag=dag
)


spark_task=SparkSubmitOperator(
    task_id='run_spark_task',
    conn_id='spark_default',
    application='/opt/airflow/spark/app/spark_airbnb_amesterdam.py',
    verbose=1,
    conf={"spark.master":spark_master},
    name='spark_job', 
    dag=dag
#)

dbt_task=DbtCloudRunJobOperator(
    task_id='run_dbt_command',
    dbt_cloud_api_token='YOUR dbt_cloud_api_token',
    dbt_cloud_conn_id='dbt_conn',
    job_id='YOUR job_id',
    dag=dag

)



install_requirements_task >> spark_task >> dbt_task

