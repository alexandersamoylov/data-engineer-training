-- dds_hub_billing_period

-- DROP TABLE IF EXISTS asamoilov.dds_hub_billing_period;

CREATE TABLE asamoilov.dds_hub_billing_period(
    billing_period_pk text CONSTRAINT dds_hub_billing_period_pk PRIMARY KEY,
    billing_period_key date,
    load_date timestamp with time zone,
    record_source text
);

