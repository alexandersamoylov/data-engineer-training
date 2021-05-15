-- ods_issue

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
-- WHERE date_part('year', start_time) = {{ execution_date.year }}
;


-- dds_hub_user

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
        -- WHERE s.date_part_year = {{ execution_date.year }}
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


-- dds_hub_service

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
        -- WHERE s.date_part_year = {{ execution_date.year }}
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


-- dds_link_issue

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
        -- WHERE s.date_part_year = {{ execution_date.year }}
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


-- dds_sat_issue_details

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
        -- WHERE s.date_part_year = {{ execution_date.year }}
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

