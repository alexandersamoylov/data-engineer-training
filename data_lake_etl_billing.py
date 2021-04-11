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
    USERNAME + '_data_lake_etl_billing',
    default_args=default_args,
    description='Data Lake ETL tasks',
    schedule_interval="0 0 1 1 *",
)

ods_billing = DataProcHiveOperator(
    task_id='ods_billing',
    dag=dag,
    query="""        
        INSERT OVERWRITE TABLE asamoilov.ods_billing PARTITION (year = {{ execution_date.year }})
        SELECT user_id,
            CAST(from_unixtime(unix_timestamp(billing_period, 'yyyy-MM')) AS DATE) AS billing_period,
            service, 
            tariff, 
            CAST(sum AS DECIMAL(10,2)) AS sum,
            CAST(created_at AS TIMESTAMP) AS created_at
        FROM asamoilov.stg_billing WHERE year(created_at) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_billing_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)
