-- ods_mdm_user

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


-- dds_hub_user

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


-- dds_sat_user_mdm_details

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

