-- dds_hub_user

-- DROP TABLE IF EXISTS asamoilov.dds_hub_user;

CREATE TABLE asamoilov.dds_hub_user(
    user_pk text CONSTRAINT dds_hub_user_pk PRIMARY KEY,
    user_key bigint,
    load_date timestamp with time zone,
    record_source text
);

