-- dds_hub_tariff

-- DROP TABLE IF EXISTS asamoilov.dds_hub_tariff;

CREATE TABLE asamoilov.dds_hub_tariff(
    tariff_pk text CONSTRAINT dds_hub_tariff_pk PRIMARY KEY,
    tariff_key text,
    load_date timestamp with time zone,
    record_source text
);

