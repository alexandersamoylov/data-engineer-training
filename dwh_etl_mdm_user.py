from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'asamoilov'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2012, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_dwh_etl_mdm_user',
    default_args = default_args,
    description = 'DWH ETL tasks',
    schedule_interval = "0 0 1 1 *",
    max_active_runs = 1
)

SQL_ODS_MDM_USER="""
TRUNCATE asamoilov.ods_mdm_user;

INSERT INTO asamoilov.ods_mdm_user
SELECT
    id AS user_id,
    legal_type AS legal_type,
    district AS district,
    registered_at AS registered_at,
    billing_mode AS billing_mode,
    is_vip AS is_vip
FROM mdm.user;
"""

SQL_DDS_HUB_USER="""
INSERT INTO asamoilov.dds_hub_user
WITH source_data AS (
    SELECT
        s1.user_pk, 
        s1.user_key, 
        s1.load_date, 
        s1.record_source
    FROM (
        SELECT
            s0.user_pk, 
            s0.user_key, 
            s0.load_date, 
            s0.record_source,
            row_number() OVER (PARTITION BY s0.user_pk ORDER BY s0.effective_from ASC) AS row_number
        FROM asamoilov.ods_mdm_user_hash s0
        -- WHERE s0.date_part_year = 2013
    ) s1
    WHERE s1.row_number = 1
)
SELECT sd.user_pk, 
    sd.user_key, 
    sd.load_date, 
    sd.record_source
FROM source_data sd
LEFT JOIN asamoilov.dds_hub_user d ON sd.user_pk = d.user_pk
WHERE d.user_pk IS NULL;
"""

SQL_DDS_SAT_USER_MDM_DETAILS="""
INSERT INTO asamoilov.dds_sat_user_mdm_details
WITH source_data AS (
    SELECT 
        s1.user_pk, 
        s1.user_hashdiff, 
        s1.legal_type,
        s1.district,
        s1.billing_mode,
        s1.is_vip,
        s1.effective_from, 
        s1.load_date, 
        s1.record_source
    FROM (
        SELECT s0.user_pk, 
            s0.user_hashdiff, 
            s0.legal_type,
            s0.district,
            s0.billing_mode,
            s0.is_vip,
            s0.effective_from, 
            s0.load_date, 
            s0.record_source,
            row_number() OVER (PARTITION BY s0.user_pk, s0.user_hashdiff ORDER BY s0.effective_from ASC) AS row_number
        FROM asamoilov.ods_mdm_user_hash s0
        -- WHERE s0.date_part_year = 2013
    ) s1
    WHERE s1.row_number = 1
)
SELECT 
    sd.user_pk,
    sd.user_hashdiff,
    sd.legal_type,
    sd.district,
    sd.billing_mode,
    sd.is_vip,
    sd.effective_from, 
    sd.load_date, 
    sd.record_source
FROM source_data sd
LEFT JOIN asamoilov.dds_sat_user_mdm_details d ON sd.user_pk = d.user_pk AND sd.user_hashdiff = d.user_hashdiff
WHERE d.user_hashdiff IS NULL;
"""

start_load = DummyOperator(task_id="start_load", dag=dag)

ods_mdm_user = PostgresOperator(
    task_id = "ods_mdm_user",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_ODS_MDM_USER
)

dds_hub_user = PostgresOperator(
    task_id = "dds_hub_user",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_HUB_USER
)

dds_sat_user_mdm_details = PostgresOperator(
    task_id = "dds_sat_user_mdm_details",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_SAT_USER_MDM_DETAILS
)

all_loaded = DummyOperator(task_id="all_loaded", dag=dag)

start_load >> ods_mdm_user >> dds_hub_user >> dds_sat_user_mdm_details >> all_loaded

