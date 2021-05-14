-- ods_billing

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
-- WHERE date_part('year', created_at) = {{ execution_date.year }}
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
        FROM asamoilov.ods_billing_hash s
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


-- dds_hub__billing_period

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
        -- WHERE s.date_part_year = {{ execution_date.year }}
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
        FROM asamoilov.ods_billing_hash s
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


-- dds_hub_tariff

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
        -- WHERE s.date_part_year = {{ execution_date.year }}
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


-- dds_link_billing

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
        -- WHERE s.date_part_year = {{ execution_date.year }}
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


-- dds_sat_billing_details

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
        -- WHERE s.date_part_year = {{ execution_date.year }}
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
