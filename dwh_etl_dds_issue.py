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
    USERNAME + '_dwh_etl_dds_issue',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
    max_active_runs = 1
)

SQL_ODS_ISSUE = """
TRUNCATE asamoilov.ods_issue;

INSERT INTO asamoilov.ods_issue
SELECT user_id::bigint,
    start_time::timestamp without time zone,
    end_time::timestamp without time zone,
    title::text,
    description::text,
    service::text,
    date_part('year', start_time) AS date_part_year    
FROM asamoilov.stg_issue
WHERE date_part('year', start_time) = {{ execution_date.year }};
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
        FROM asamoilov.ods_issue_hash s
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
        FROM asamoilov.ods_issue_hash s
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

SQL_DDS_LINK_ISSUE = """
INSERT INTO asamoilov.dds_link_issue
WITH source_data AS (
    SELECT sd.issue_pk, 
        sd.user_pk,
        sd.service_pk,
        sd.load_date, 
        sd.record_source
    FROM (
        SELECT s.issue_pk, 
            s.user_pk,
            s.service_pk,
            s.load_date, 
            s.record_source,
            row_number() OVER (PARTITION BY s.issue_pk ORDER BY s.effective_from ASC) AS row_number
        FROM asamoilov.ods_issue_hash s
        WHERE s.date_part_year = {{ execution_date.year }}
    ) sd
    WHERE sd.row_number = 1
)
SELECT sd0.issue_pk,
    sd0.user_pk,
    sd0.service_pk,
    sd0.load_date,
    sd0.record_source
FROM source_data sd0
LEFT JOIN asamoilov.dds_link_issue ud0 ON sd0.issue_pk = ud0.issue_pk
WHERE ud0.issue_pk IS NULL;
"""

SQL_DDS_SAT_ISSUE_DETAILS = """
INSERT INTO asamoilov.dds_sat_issue_details
WITH source_data AS (
    SELECT sd.issue_pk,
        sd.issue_hashdiff,
        sd.start_time,
        sd.end_time,
        sd.title,
        sd.description,
        sd.effective_from,
        sd.load_date,
        sd.record_source
    FROM (
        SELECT s.issue_pk,
            s.issue_hashdiff,
            s.start_time,
            s.end_time,
            s.title,
            s.description,
            s.effective_from, 
            s.load_date, 
            s.record_source,
            row_number() OVER (PARTITION BY s.issue_pk, s.issue_hashdiff ORDER BY s.effective_from DESC) AS row_number
        FROM asamoilov.ods_issue_hash s
        WHERE s.date_part_year = {{ execution_date.year }}
    ) sd
    WHERE sd.row_number = 1
)
SELECT sd0.issue_pk,
    sd0.issue_hashdiff,
    sd0.start_time,
    sd0.end_time,
    sd0.title,
    sd0.description,
    sd0.effective_from,
    sd0.load_date,
    sd0.record_source
FROM source_data sd0
LEFT JOIN asamoilov.dds_sat_issue_details ud0 ON sd0.issue_pk = ud0.issue_pk
    AND sd0.issue_hashdiff = ud0.issue_hashdiff
WHERE ud0.issue_hashdiff IS NULL OR sd0.effective_from > ud0.effective_from;
"""

start_load = DummyOperator(task_id="start_load", dag=dag)

ods_issue = PostgresOperator(
    task_id = "ods_issue",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_ODS_ISSUE
)

dds_hub_user = PostgresOperator(
    task_id = "dds_hub_user",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_HUB_USER
)

dds_hub_service = PostgresOperator(
    task_id = "dds_hub_service",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_HUB_SERVICE
)

dds_link_issue = PostgresOperator(
    task_id = "dds_link_issue",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_LINK_ISSUE
)

dds_sat_issue_details = PostgresOperator(
    task_id = "dds_sat_issue_details",
    dag = dag,
    # postgres_conn_id="postgres_default",
    sql = SQL_DDS_SAT_BILLING_DETAILS
)

all_loaded = DummyOperator(task_id="all_loaded", dag=dag)

start_load >> ods_issue

ods_issue >> dds_hub_user >> all_loaded
ods_issue >> dds_hub_service >> all_loaded
ods_issue >> dds_link_issue >> all_loaded
ods_issue >> dds_sat_issue_details >> all_loaded

