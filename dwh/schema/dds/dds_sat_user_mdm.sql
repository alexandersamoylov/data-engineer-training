-- asamoilov.dds_sat_user_mdm

-- DROP TABLE IF EXISTS asamoilov.dds_sat_user_mdm;

CREATE TABLE asamoilov.dds_sat_user_mdm(
    user_pk text,
    user_hashdiff text,
    legal_type text,
    district text,
    billing_mode text,
    is_vip boolean,
    effective_from timestamp without time zone,
    load_date timestamp with time zone,
    record_source text
);

