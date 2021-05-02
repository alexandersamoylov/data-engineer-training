-- asamoilov.dds_sat_user_mdm_details

-- DROP TABLE IF EXISTS asamoilov.dds_sat_user_mdm_details;

CREATE TABLE asamoilov.dds_sat_user_mdm_details(
    user_pk TEXT,
    user_hashdiff TEXT,
    legal_type TEXT,
    district TEXT,
    billing_mode TEXT,
    is_vip BOOLEAN,
    effective_from TIMESTAMP WITHOUT TIME ZONE,
    load_date TIMESTAMP WITH TIME ZONE,
    record_source TEXT
);

