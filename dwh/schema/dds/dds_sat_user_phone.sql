-- dds_sat_user_phone

-- DROP TABLE IF EXISTS asamoilov.dds_sat_user_phone;

CREATE TABLE asamoilov.dds_sat_user_phone(
    user_pk text,
    user_hashdiff text,
    phone text,
    effective_from timestamp without time zone,
    load_date timestamp with time zone,
    record_source text
);

