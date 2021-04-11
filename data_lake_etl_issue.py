from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'asamoilov'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2012, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_data_lake_etl_issue',
    default_args=default_args,
    description='Data Lake ETL tasks',
    schedule_interval="0 0 1 1 *",
)

ods_issue = DataProcHiveOperator(
    task_id='ods_issue',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE asamoilov.ods_issue PARTITION (year = {{ execution_date.year }})
        SELECT CAST(user_id AS BIGINT) as user_id,
            CAST(start_time AS TIMESTAMP) AS start_time,
            CAST(end_time AS TIMESTAMP) AS end_time,
            title,
            description,
            service
        FROM asamoilov.stg_issue WHERE year(start_time) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_issue_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)
