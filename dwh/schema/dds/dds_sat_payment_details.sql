-- dds_sat_payment_details

-- DROP TABLE IF EXISTS asamoilov.dds_sat_payment_details;

CREATE TABLE asamoilov.dds_sat_payment_details(
    payment_pk text,
    payment_hashdiff text,
    pay_doc_type text,
    pay_doc_num bigint,
    payment_sum decimal(10,2),
    effective_from timestamp without time zone,
    load_date timestamp with time zone,
    record_source text
);

