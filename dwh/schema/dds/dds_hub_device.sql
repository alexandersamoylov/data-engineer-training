-- dds_hub_divice

-- DROP TABLE IF EXISTS asamoilov.dds_hub_device;

CREATE TABLE asamoilov.dds_hub_device(
    device_pk text CONSTRAINT dds_hub_device_pk PRIMARY KEY,
    device_key text,
    load_date timestamp with time zone,
    record_source text
);

