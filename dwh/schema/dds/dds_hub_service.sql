-- dds_hub_service

-- DROP TABLE IF EXISTS asamoilov.dds_hub_service;

CREATE TABLE asamoilov.dds_hub_service(
    service_pk text CONSTRAINT dds_hub_service_pk PRIMARY KEY,
    service_key text,
    load_date timestamp with time zone,
    record_source text
);

