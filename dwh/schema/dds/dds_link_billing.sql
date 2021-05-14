-- dds_link_billing

-- DROP TABLE IF EXISTS asamoilov.dds_link_billing;

CREATE TABLE asamoilov.dds_link_billing(
    billing_pk text CONSTRAINT dds_link_billing_pk PRIMARY KEY,
    user_pk text,
    billing_period_pk text,
    service_pk text,
    tariff_pk text,
    load_date timestamp with time zone,
    record_source text
);

