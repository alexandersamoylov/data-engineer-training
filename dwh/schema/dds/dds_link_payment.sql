-- dds_link_payment

-- DROP TABLE IF EXISTS asamoilov.dds_link_payment;

CREATE TABLE asamoilov.dds_link_payment(
    payment_pk text CONSTRAINT dds_link_payment_pk PRIMARY KEY,
    user_pk text,
    account_pk text,
    billing_period_pk text,
    load_date timestamp with time zone,
    record_source text
);

