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
    USERNAME + '_dwh_etl_dds_billing',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
    max_active_runs = 1
)

SQL_ODS_BILLING = """
TRUNCATE asamoilov.ods_billing;

INSERT INTO asamoilov.ods_billing
SELECT user_id::bigint,
    to_date(billing_period, 'YYYY-MM') AS billing_period,
    service::text,
    tariff::text,
    sum::decimal(10,2) AS billing_sum,
    created_at::timestamp without time zone,
    date_part('year', created_at) AS date_part_year    
FROM asamoilov.stg_billing
WHERE date_part('year', created_at) = {{ execution_date.year }};
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
        FROM asamoilov.ods_billing_hash s
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
        FROM asamoilov.ods_billing_hash s
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

SQL_DDS_HUB_SERVICE = """
INSERT INTO asamoilov.dds_hub_service
WITH source_data AS (
    SELECT sd.service_pk, 
        sd.service_key, 
        sd.load_date, 
        sd.record_source
    FROM (
        SELECT s.service_pk, 
            s.service_key, 
            s.load_date, 
            s.record_source,
            row_number() OVER (PARTITION BY s.service_pk ORDER BY s.effective_from ASC) AS row_number
        FROM asamoilov.ods_billing_hash s
        WHERE s.date_part_year = {{ execution_date.year }}
    ) sd
    WHERE sd.row_number = 1
)
SELECT sd0.service_pk, 
    sd0.service_key, 
    sd0.load_date, 
    sd0.record_source
FROM source_data sd0
LEFT JOIN asamoilov.dds_hub_service ud0 ON sd0.service_pk = ud0.service_pk
WHERE ud0.service_pk IS NULL;
"""

SQL_DDS_HUB_TARIFF = """
INSERT INTO asamoilov.dds_hub_tariff
WITH source_data AS (
    SELECT sd.tariff_pk, 
        sd.tariff_key, 
        sd.load_date, 
        sd.record_source
    FROM (
        SELECT s.tariff_pk, 
            s.tariff_key, 
            s.load_date, 
            s.record_source,
            row_number() OVER (PARTITION BY s.tariff_pk ORDER BY s.effective_from ASC) AS row_number
        FROM asamoilov.ods_billing_hash s
        WHERE s.date_part_year = {{ execution_date.year }}
    ) sd
    WHERE sd.row_number = 1
)
SELECT sd0.tariff_pk, 
    sd0.tariff_key, 
    sd0.load_date, 
    sd0.record_source
FROM source_data sd0
LEFT JOIN asamoilov.dds_hub_tariff ud0 ON sd0.tariff_pk = ud0.tariff_pk
WHERE ud0.tariff_pk IS NULL;
"""

SQL_DDS_LINK_BILLING = """
INSERT INTO asamoilov.dds_link_billing
WITH source_data AS (
    SELECT sd.billing_pk, 
        sd.user_pk,
        sd.billing_period_pk,
        sd.service_pk,
        sd.tariff_pk,
        sd.load_date, 
        sd.record_source
    FROM (
        SELECT s.billing_pk, 
            s.user_pk,
            s.billing_period_pk,
            s.service_pk,
            s.tariff_pk,
            s.load_date, 
            s.record_source,
            row_number() OVER (PARTITION BY s.billing_pk ORDER BY s.effective_from ASC) AS row_number
        FROM asamoilov.ods_billing_hash s
        WHERE s.date_part_year = {{ execution_date.year }}
    ) sd
    WHERE sd.row_number = 1
)
SELECT sd0.billing_pk,
    sd0.user_pk,
    sd0.billing_period_pk,
    sd0.service_pk,
    sd0.tariff_pk,
    sd0.load_date,
    sd0.record_source
FROM source_data sd0
LEFT JOIN asamoilov.dds_link_billing ud0 ON sd0.billing_pk = ud0.billing_pk
WHERE ud0.billing_pk IS NULL;
"""

SQL_DDS_SAT_BILLING_DETAILS = """
INSERT INTO asamoilov.dds_sat_billing_details
WITH source_data AS (
    SELECT sd.billing_pk,
        sd.billing_hashdiff,
        sd.billing_sum,
        sd.effective_from,
        sd.load_date,
        sd.record_source
    FROM (
        SELECT s.billing_pk,
            s.billing_hashdiff,
            s.billing_sum,
            s.effective_from, 
            s.load_date, 
            s.record_source,
            row_number() OVER (PARTITION BY s.billing_pk, s.billing_hashdiff ORDER BY s.effective_from ASC) AS row_number
        FROM asamoilov.ods_billing_hash s
        WHERE s.date_part_year = {{ execution_date.year }}
    ) sd
    WHERE sd.row_number = 1
)
SELECT sd0.billing_pk,
    sd0.billing_hashdiff,
    sd0.billing_sum,
    sd0.effective_from,
    sd0.load_date,
    sd0.record_source
FROM source_data sd0
LEFT JOIN asamoilov.dds_sat_billing_details ud0 ON sd0.billing_pk = ud0.billing_pk
    AND sd0.billing_hashdiff = ud0.billing_hashdiff
WHERE ud0.billing_hashdiff IS NULL;
"""

start_load = DummyOperator(task_id="start_load", dag=dag)

ods_billing = PostgresOperator(
    task_id = "ods_billing",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_ODS_BILLING
)

dds_hub_user = PostgresOperator(
    task_id = "dds_hub_user",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_HUB_USER
)

dds_hub_billing_period = PostgresOperator(
    task_id = "dds_hub_billing_period",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_HUB_BILLING_PERIOD
)

dds_hub_service = PostgresOperator(
    task_id = "dds_hub_service",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_HUB_SERVICE
)

dds_hub_tariff = PostgresOperator(
    task_id = "dds_hub_tariff",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_HUB_TARIFF
)

dds_link_billing = PostgresOperator(
    task_id = "dds_link_billing",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_LINK_BILLING
)

dds_sat_billing_details = PostgresOperator(
    task_id = "dds_sat_billing_details",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_SAT_BILLING_DETAILS
)

all_loaded = DummyOperator(task_id="all_loaded", dag=dag)

start_load >> ods_billing

ods_billing >> dds_hub_user >> all_loaded
ods_billing >> dds_hub_billing_period >> all_loaded
ods_billing >> dds_hub_service >> all_loaded
ods_billing >> dds_hub_tariff >> all_loaded
ods_billing >> dds_link_billing >> all_loaded
ods_billing >> dds_sat_billing_details >> all_loaded

