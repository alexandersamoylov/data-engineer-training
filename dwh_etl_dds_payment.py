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
    max_active_runs = 1
)

SQL_ODS_PAYMENT = """
TRUNCATE asamoilov.ods_payment;

INSERT INTO asamoilov.ods_payment
SELECT user_id::bigint,
    pay_doc_type::text,
    pay_doc_num::bigint,
    account::text,
    phone::text,
    to_date(billing_period, 'YYYY-MM') AS billing_period,
    pay_date::timestamp without time zone,
    sum::decimal(10,2),
    date_part('year', pay_date) AS date_part_year
FROM asamoilov.stg_payment
WHERE date_part('year', pay_date) = {{ execution_date.year }};
"""

SQL_DDS_HUB_USER = """
INSERT INTO asamoilov.dds_hub_user
WITH source_data AS (
    SELECT sd.user_pk, 
        sd.user_key, 
        sd.load_date, 
        sd.record_source
    FROM (
        SELECT s.user_pk, 
            s.user_key, 
            s.load_date, 
            s.record_source,
            row_number() OVER (PARTITION BY s.user_pk ORDER BY s.effective_from ASC) AS row_number
        FROM asamoilov.ods_payment_hash s
        WHERE s.date_part_year = {{ execution_date.year }}
    ) sd
    WHERE sd.row_number = 1
)
SELECT sd0.user_pk, 
    sd0.user_key, 
    sd0.load_date, 
    sd0.record_source
FROM source_data sd0
LEFT JOIN asamoilov.dds_hub_user ud0 ON sd0.user_pk = ud0.user_pk
WHERE ud0.user_pk IS NULL;
"""

SQL_DDS_HUB_ACCOUNT = """
INSERT INTO asamoilov.dds_hub_account
WITH source_data AS (
    SELECT sd.account_pk, 
        sd.account_key, 
        sd.load_date, 
        sd.record_source
    FROM (
        SELECT s.account_pk, 
            s.account_key, 
            s.load_date, 
            s.record_source,
            row_number() OVER (PARTITION BY s.account_pk ORDER BY s.effective_from ASC) AS row_number
        FROM asamoilov.ods_payment_hash s
        WHERE s.date_part_year = {{ execution_date.year }}
    ) sd
    WHERE sd.row_number = 1
)
SELECT sd0.account_pk,
    sd0.account_key, 
    sd0.load_date, 
    sd0.record_source
FROM source_data sd0
LEFT JOIN asamoilov.dds_hub_account ud0 ON sd0.account_pk = ud0.account_pk
WHERE ud0.account_pk IS NULL;
"""

SQL_DDS_HUB_BILLING_PERIOD = """
INSERT INTO asamoilov.dds_hub_billing_period
WITH source_data AS (
    SELECT sd.billing_period_pk, 
        sd.billing_period_key, 
        sd.load_date, 
        sd.record_source
    FROM (
        SELECT s.billing_period_pk, 
            s.billing_period_key, 
            s.load_date, 
            s.record_source,
            row_number() OVER (PARTITION BY s.billing_period_pk ORDER BY s.effective_from ASC) AS row_number
        FROM asamoilov.ods_payment_hash s
        WHERE s.date_part_year = {{ execution_date.year }}
    ) sd
    WHERE sd.row_number = 1
)
SELECT sd0.billing_period_pk, 
    sd0.billing_period_key, 
    sd0.load_date, 
    sd0.record_source
FROM source_data sd0
LEFT JOIN asamoilov.dds_hub_billing_period ud0 ON sd0.billing_period_pk = ud0.billing_period_pk
WHERE ud0.billing_period_pk IS NULL;
"""

SQL_DDS_LINK_PAYMENT = """
INSERT INTO asamoilov.dds_link_payment
WITH source_data AS (
    SELECT sd.payment_pk, 
        sd.user_pk, 
        sd.account_pk, 
        sd.billing_period_pk, 
        sd.load_date, 
        sd.record_source
    FROM (
        SELECT s.payment_pk, 
            s.user_pk, 
            s.account_pk, 
            s.billing_period_pk, 
            s.load_date, 
            s.record_source,
            row_number() OVER (PARTITION BY s.payment_pk ORDER BY s.effective_from ASC) AS row_number
        FROM asamoilov.ods_payment_hash s
        WHERE s.date_part_year = {{ execution_date.year }}
    ) sd
    WHERE sd.row_number = 1
)
SELECT sd0.payment_pk, 
    sd0.user_pk, 
    sd0.account_pk, 
    sd0.billing_period_pk, 
    sd0.load_date, 
    sd0.record_source
FROM source_data sd0
LEFT JOIN asamoilov.dds_link_payment ud0 ON sd0.payment_pk = ud0.payment_pk
WHERE ud0.payment_pk IS NULL;
"""

SQL_DDS_SAT_USER_PHONE = """
INSERT INTO asamoilov.dds_sat_user_phone
WITH source_data AS (
    SELECT sd.user_pk,
        sd.user_hashdiff,
        sd.phone,
        sd.effective_from,
        sd.load_date,
        sd.record_source
    FROM (
        SELECT s.user_pk,
            s.user_hashdiff,
            s.phone,
            s.effective_from,
            s.load_date,
            s.record_source,
            CASE
                WHEN lag(s.user_hashdiff, 1, 'none') OVER (PARTITION BY s.user_pk ORDER BY s.effective_from) = s.user_hashdiff
                    THEN 'N'
                ELSE 'Y'
            END AS is_update
        FROM asamoilov.ods_payment_hash s
        WHERE s.date_part_year = {{ execution_date.year }}
    ) sd
    WHERE sd.is_update = 'Y'
),
source_pk AS (
    SELECT DISTINCT sd1.user_pk FROM source_data sd1
),
update_data AS (
    SELECT ud.user_pk,
        ud.user_hashdiff,
        ud.phone,
        ud.effective_from,
        ud.load_date,
        ud.record_source,
        ud.effective_to
    FROM (
        SELECT u.user_pk, 
            u.user_hashdiff,
            u.phone,
            u.effective_from,
            u.load_date,
            u.record_source,
            rank() OVER (PARTITION BY u.user_pk, u.user_hashdiff ORDER BY u.effective_from DESC) AS rank_1,
            lead(u.effective_from) OVER (PARTITION BY u.user_pk ORDER BY u.effective_from) AS effective_to
        FROM asamoilov.dds_sat_user_phone u
        JOIN source_pk sp ON u.user_pk = sp.user_pk
    ) ud WHERE ud.rank_1 = 1
)
SELECT sd0.user_pk,
    sd0.user_hashdiff,
    sd0.phone,
    sd0.effective_from,
    sd0.load_date,
    sd0.record_source
FROM source_data sd0
LEFT JOIN update_records ud0 ON sd0.user_pk = ud0.user_pk
    AND sd0.user_hashdiff = ud0.user_hashdiff
WHERE ud0.user_hashdiff IS NULL OR sd0.effective_from > ud0.effective_to;
"""

SQL_DDS_SAT_PAYMENT_DETAILS = """
INSERT INTO asamoilov.dds_sat_payment_details
WITH source_data AS (
    SELECT sd.payment_pk, 
        sd.payment_hashdiff,
        sd.pay_doc_type, 
        sd.pay_doc_num, 
        sd.sum, 
        sd.effective_from, 
        sd.load_date, 
        sd.record_source
    FROM (
        SELECT s.payment_pk, 
            s.payment_hashdiff,
            s.pay_doc_type, 
            s.pay_doc_num, 
            s.sum, 
            s.effective_from, 
            s.load_date, 
            s.record_source,
            row_number() OVER (PARTITION BY s.payment_pk, s.payment_hashdiff ORDER BY s.effective_from ASC) AS row_number
        FROM asamoilov.ods_payment_hash s
        WHERE s.date_part_year = {{ execution_date.year }}
    ) sd
    WHERE sd.row_number = 1
)
SELECT sd0.payment_pk, 
    sd0.payment_hashdiff,
    sd0.pay_doc_type, 
    sd0.pay_doc_num, 
    sd0.sum, 
    sd0.effective_from, 
    sd0.load_date, 
    sd0.record_source
FROM source_data sd0
LEFT JOIN asamoilov.dds_sat_payment_details ud0 ON sd0.payment_pk = ud0.payment_pk
    AND sd0.payment_hashdiff = ud0.payment_hashdiff
WHERE ud0.payment_hashdiff IS NULL;
"""

start_load = DummyOperator(task_id="start_load", dag=dag)

ods_payment = PostgresOperator(
    task_id = "ods_payment",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_ODS_PAYMENT
)

dds_hub_user = PostgresOperator(
    task_id = "dds_hub_user",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_HUB_USER
)

dds_hub_account = PostgresOperator(
    task_id = "dds_hub_account",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_HUB_ACCOUNT
)

dds_hub_billing_period = PostgresOperator(
    task_id = "dds_hub_billing_period",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_HUB_BILLING_PERIOD
)

dds_link_payment = PostgresOperator(
    task_id = "dds_link_payment",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_LINK_PAYMENT
)

dds_sat_user_phone = PostgresOperator(
    task_id = "dds_sat_user_phone",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_SAT_USER_PHONE
)

dds_sat_payment_details = PostgresOperator(
    task_id = "dds_sat_payment_details",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_SAT_PAYMENT_DETAILS
)

all_loaded = DummyOperator(task_id="all_loaded", dag=dag)

start_load >> ods_payment

ods_payment >> dds_hub_user >> all_loaded
ods_payment >> dds_hub_account >> all_loaded
ods_payment >> dds_hub_billing_period >> all_loaded
ods_payment >> dds_link_payment >> all_loaded
ods_payment >> dds_sat_user_phone >> all_loaded
ods_payment >> dds_sat_payment_details >> all_loaded
