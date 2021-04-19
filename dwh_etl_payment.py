from datetime import timedelta, datetime
# from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'asamoilov'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2012, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_dwh_etl_payment',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
)

start_load = DummyOperator(task_id="start_load", dag=dag)

dds_hub_user = PostgresOperator(
    task_id="dds_hub_user",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT INTO asamoilov.dds_hub_user
        WITH record_source AS (
            SELECT s.user_pk, s.user_key, s.load_date, s.record_source
            FROM (
                SELECT p.user_pk, p.user_key, p.load_date, p.record_source,
                    row_number() OVER (PARTITION BY p.user_pk ORDER BY p.effective_from ASC) AS row_number
                FROM asamoilov.ods_payment_v p
                WHERE p.date_part_year = {{ execution_date.year }}
            ) s
            WHERE s.row_number = 1
        )
        SELECT rs.user_pk, rs.user_key, rs.load_date, rs.record_source
        FROM record_source rs
        LEFT JOIN asamoilov.dds_hub_user dhu ON rs.user_pk = dhu.user_pk
        WHERE dhu.user_pk IS NULL;
    """
)

dds_hub_account = PostgresOperator(
    task_id="dds_hub_account",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT INTO asamoilov.dds_hub_account
        WITH record_source AS (
            SELECT s.account_pk, s.account_key, s.load_date, s.record_source
            FROM (
                SELECT p.account_pk, p.account_key, p.load_date, p.record_source,
                    row_number() OVER (PARTITION BY p.account_pk ORDER BY p.effective_from ASC) AS row_number
                FROM asamoilov.ods_payment_v p
                WHERE p.date_part_year = {{ execution_date.year }}
            ) s
            WHERE s.row_number = 1
        )
        SELECT rs.account_pk, rs.account_key, rs.load_date, rs.record_source
        FROM record_source rs
        LEFT JOIN asamoilov.dds_hub_account dha ON rs.account_pk = dha.account_pk
        WHERE dha.account_pk IS NULL;
    """
)

dds_hub_billing_period = PostgresOperator(
    task_id="dds_hub_billing_period",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT INTO asamoilov.dds_hub_billing_period
        WITH record_source AS (
            SELECT s.billing_period_pk, s.billing_period_key, s.load_date, s.record_source
            FROM (
                SELECT p.billing_period_pk, p.billing_period_key, p.load_date, p.record_source,
                    row_number() OVER (PARTITION BY p.billing_period_pk ORDER BY p.effective_from ASC) AS row_number
                FROM asamoilov.ods_payment_v p
                WHERE p.date_part_year = {{ execution_date.year }}
            ) s
            WHERE s.row_number = 1
        )
        SELECT rs.billing_period_pk, rs.billing_period_key, rs.load_date, rs.record_source
        FROM record_source rs
        LEFT JOIN asamoilov.dds_hub_billing_period dhbp ON rs.billing_period_pk = dhbp.billing_period_pk
        WHERE dhbp.billing_period_pk IS NULL;
    """
)

all_hub_loaded = DummyOperator(task_id="all_hub_loaded", dag=dag)

start_load >> dds_hub_payment >> all_hub_loaded
start_load >> dds_hub_account >> all_hub_loaded
start_load >> dds_hub_billing_period >> all_hub_loaded

dds_link_payment = PostgresOperator(
    task_id="dds_link_payment",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT INTO asamoilov.dds_link_payment
        WITH record_source AS (
            SELECT s.payment_pk, s.user_pk, s.account_pk, s.billing_period_pk, s.load_date, s.record_source
            FROM (
                SELECT p.payment_pk, p.user_pk, p.account_pk, p.billing_period_pk, p.load_date, p.record_source,
                    row_number() OVER (PARTITION BY p.payment_pk ORDER BY p.effective_from ASC) AS row_number
                FROM asamoilov.ods_payment_v p
                WHERE p.date_part_year = {{ execution_date.year }}
            ) s
            WHERE s.row_number = 1
        )
        SELECT rs.payment_pk, rs.user_pk, rs.account_pk, rs.billing_period_pk, rs.load_date, rs.record_source
        FROM record_source rs
        LEFT JOIN asamoilov.dds_link_payment dlp ON rs.payment_pk = dlp.payment_pk
        WHERE dlp.payment_pk IS NULL;
    """
)

all_link_loaded = DummyOperator(task_id="all_link_loaded", dag=dag)

all_hub_loaded >> dds_link_payment >> all_link_loaded

dds_sat_user_details = PostgresOperator(
    task_id="dds_sat_user_details",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT INTO asamoilov.dds_sat_user_details
        WITH record_source AS (
            SELECT s.user_pk, s.user_hashdiff, s.phone, s.effective_from, s.load_date, s.record_source
            FROM (
                SELECT p.user_pk, p.user_hashdiff, p.phone, p.effective_from, p.load_date, p.record_source,
                    rank() OVER (PARTITION BY p.user_pk ORDER BY p.effective_from DESC) AS rank_1
                FROM asamoilov.ods_payment_v p
                WHERE p.date_part_year = {{ execution_date.year }}
            ) s
            WHERE s.rank_1 = 1
        )
        SELECT rs.user_pk, rs.user_hashdiff, rs.phone, rs.effective_from, rs.load_date, rs.record_source
        FROM record_source rs
        LEFT JOIN asamoilov.dds_sat_user_details dsud ON rs.user_pk = dsud.user_pk
            AND rs.user_hashdiff = dsud.user_hashdiff
        WHERE dsud.user_hashdiff IS NULL;
    """
)

dds_sat_payment_details = PostgresOperator(
    task_id="dds_sat_payment_details",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        INSERT INTO asamoilov.dds_sat_payment_details
        WITH record_source AS (
            SELECT s.payment_pk, s.payment_hashdiff,
                s.pay_doc_type, s.pay_doc_num, s.sum, s.effective_from, s.load_date, s.record_source
            FROM (
                SELECT p.payment_pk, p.payment_hashdiff,
                p.pay_doc_type, p.pay_doc_num, p.sum, p.effective_from, p.load_date, p.record_source,
                    row_number() OVER (PARTITION BY p.payment_pk, p.payment_hashdiff ORDER BY p.effective_from ASC) AS row_number
                FROM asamoilov.ods_payment_v p
                WHERE p.date_part_year = {{ execution_date.year }}
            ) s
            WHERE s.row_number = 1
        )
        SELECT rs.payment_pk, rs.payment_hashdiff,
            rs.pay_doc_type, rs.pay_doc_num, rs.sum, rs.effective_from, rs.load_date, rs.record_source
        FROM record_source rs
        LEFT JOIN asamoilov.dds_sat_payment_details dspd ON rs.payment_pk = dspd.payment_pk
            AND rs.payment_hashdiff = dspd.payment_hashdiff
        WHERE dspd.payment_hashdiff IS NULL;
    """
)

all_loaded = DummyOperator(task_id="all_loaded", dag=dag)

all_link_loaded >> dds_sat_user_details >> all_loaded
all_link_loaded >> dds_sat_payment_details >> all_loaded