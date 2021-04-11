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
    USERNAME + '_data_lake_payment_etl',
    default_args=default_args,
    description='Data Lake ETL tasks',
    schedule_interval="0 0 1 1 *",
)

ods_payment = DataProcHiveOperator(
    task_id='ods_payment',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE asamoilov.ods_payment PARTITION (year = {{ execution_date.year }})
        SELECT user_id,
            pay_doc_type,
            pay_doc_num,
            account,
            phone,
            CAST(from_unixtime(unix_timestamp(billing_period, 'yyyy-MM')) AS DATE) AS billing_period,
            CAST(pay_date AS TIMESTAMP) AS pay_date,
            CAST(sum AS DECIMAL(10,2)) AS sum
        FROM asamoilov.stg_payment WHERE year(pay_date) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_payment_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)
