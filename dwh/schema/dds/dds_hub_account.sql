-- dds_hub_account

-- DROP TABLE IF EXISTS asamoilov.dds_hub_account;

CREATE TABLE asamoilov.dds_hub_account(
    account_pk text CONSTRAINT dds_hub_account_pk PRIMARY KEY,
    account_key text,
    load_date timestamp with time zone,
    record_source text
);

