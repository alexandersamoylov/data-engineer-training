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
    USERNAME + '_dwh_etl_dds_traffic',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
    max_active_runs = 1
)

SQL_ODS_TRAFFIC = """
TRUNCATE asamoilov.ods_traffic;

INSERT INTO asamoilov.ods_traffic
SELECT user_id::bigint,
    to_timestamp(timestamp/1000) AS traffic_time,
    device_id::text,
    device_ip_addr::text,
    bytes_sent::bigint,
    bytes_received::bigint,
    date_part('year', to_timestamp(timestamp/1000)) AS date_part_year
FROM asamoilov.stg_traffic
WHERE date_part('year', to_timestamp(timestamp/1000)) = {{ execution_date.year }};
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
        FROM asamoilov.ods_traffic_hash s
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

SQL_DDS_HUB_DEVICE = """
INSERT INTO asamoilov.dds_hub_device
WITH source_data AS (
    SELECT sd.device_pk, 
        sd.device_key, 
        sd.load_date, 
        sd.record_source
    FROM (
        SELECT s.device_pk, 
            s.device_key, 
            s.load_date, 
            s.record_source,
            row_number() OVER (PARTITION BY s.device_pk ORDER BY s.effective_from ASC) AS row_number
        FROM asamoilov.ods_traffic_hash s
        WHERE s.date_part_year = {{ execution_date.year }}
    ) sd
    WHERE sd.row_number = 1
)
SELECT sd0.device_pk, 
    sd0.device_key, 
    sd0.load_date, 
    sd0.record_source
FROM source_data sd0
LEFT JOIN asamoilov.dds_hub_device ud0 ON sd0.device_pk = ud0.device_pk
WHERE ud0.device_pk IS NULL;
"""

SQL_DDS_LINK_TRAFFIC = """
INSERT INTO asamoilov.dds_link_traffic
WITH source_data AS (
    SELECT sd.traffic_pk, 
        sd.user_pk,
        sd.device_pk,
        sd.load_date, 
        sd.record_source
    FROM (
        SELECT s.traffic_pk, 
            s.user_pk,
            s.device_pk,
            s.load_date, 
            s.record_source,
            row_number() OVER (PARTITION BY s.traffic_pk ORDER BY s.effective_from ASC) AS row_number
        FROM asamoilov.ods_traffic_hash s
        WHERE s.date_part_year = {{ execution_date.year }}
    ) sd
    WHERE sd.row_number = 1
)
SELECT sd0.traffic_pk, 
    sd0.user_pk,
    sd0.device_pk,
    sd0.load_date,
    sd0.record_source
FROM source_data sd0
LEFT JOIN asamoilov.dds_link_traffic ud0 ON sd0.traffic_pk = ud0.traffic_pk
WHERE ud0.traffic_pk IS NULL;
"""

SQL_DDS_SAT_TRAFFIC_DETAILS = """
INSERT INTO asamoilov.dds_sat_traffic_details
WITH source_data AS (
    SELECT sd.traffic_pk,
        sd.traffic_hashdiff,
        sd.device_ip_addr,
        sd.bytes_sent,
        sd.bytes_received,
        sd.effective_from,
        sd.load_date,
        sd.record_source
    FROM (
        SELECT s.traffic_pk,
            s.traffic_hashdiff,
            s.device_ip_addr,
            s.bytes_sent,
            s.bytes_received,
            s.effective_from,
            s.load_date, 
            s.record_source,
            row_number() OVER (PARTITION BY s.traffic_pk, s.traffic_hashdiff ORDER BY s.effective_from ASC) AS row_number
        FROM asamoilov.ods_traffic_hash s
        WHERE s.date_part_year = {{ execution_date.year }}
    ) sd
    WHERE sd.row_number = 1
)
SELECT sd0.traffic_pk,
    sd0.traffic_hashdiff,
    sd0.device_ip_addr,
    sd0.bytes_sent,
    sd0.bytes_received,
    sd0.effective_from,
    sd0.load_date,
    sd0.record_source
FROM source_data sd0
LEFT JOIN asamoilov.dds_sat_traffic_details ud0 ON sd0.traffic_pk = ud0.traffic_pk
    AND sd0.traffic_hashdiff = ud0.traffic_hashdiff
WHERE ud0.traffic_hashdiff IS NULL;
"""

start_load = DummyOperator(task_id="start_load", dag=dag)

ods_traffic = PostgresOperator(
    task_id = "ods_traffic",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_ODS_TRAFFIC
)

dds_hub_user = PostgresOperator(
    task_id = "dds_hub_user",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_HUB_USER
)

dds_hub_device = PostgresOperator(
    task_id = "dds_hub_device",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_HUB_DEVICE
)

dds_link_traffic = PostgresOperator(
    task_id = "dds_link_traffic",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_LINK_TRAFFIC
)

dds_sat_traffic_details = PostgresOperator(
    task_id = "dds_sat_traffic_details",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_SAT_TRAFFIC_DETAILS
)

all_loaded = DummyOperator(task_id="all_loaded", dag=dag)

start_load >> ods_traffic

ods_traffic >> dds_hub_user >> all_loaded
ods_traffic >> dds_hub_device >> all_loaded
ods_traffic >> dds_link_traffic >> all_loaded
ods_traffic >> dds_sat_traffic_details >> all_loaded

