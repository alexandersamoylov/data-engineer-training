-- dds_sat_billing_details

-- DROP TABLE IF EXISTS asamoilov.dds_sat_billing_details;

CREATE TABLE asamoilov.dds_sat_billing_details(
    billing_pk text,
    billing_hashdiff text,
    billing_sum decimal(10,2),
    effective_from timestamp without time zone,
    load_date timestamp with time zone,
    record_source text
);

