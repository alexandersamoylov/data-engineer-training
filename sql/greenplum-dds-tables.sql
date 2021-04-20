DROP TABLE IF EXISTS asamoilov.dds_hub_user;

CREATE TABLE asamoilov.dds_hub_user(
    user_pk TEXT CONSTRAINT dds_hub_user_pk PRIMARY KEY,
    user_key BIGINT,
    load_date TIMESTAMP WITH TIME ZONE,
    record_source TEXT
);

DROP TABLE IF EXISTS asamoilov.dds_sat_user_details;

CREATE TABLE asamoilov.dds_sat_user_details(
    user_pk TEXT,
    user_hashdiff TEXT,
    phone TEXT,
    effective_from TIMESTAMP WITHOUT TIME ZONE,
    load_date TIMESTAMP WITH TIME ZONE,
    record_source TEXT
);

DROP TABLE IF EXISTS asamoilov.dds_hub_account;

CREATE TABLE asamoilov.dds_hub_account(
    account_pk TEXT CONSTRAINT dds_hub_account_pk PRIMARY KEY,
    account_key TEXT,
    load_date TIMESTAMP WITH TIME ZONE,
    record_source TEXT
);

DROP TABLE IF EXISTS asamoilov.dds_hub_billing_period;

CREATE TABLE asamoilov.dds_hub_billing_period(
    billing_period_pk TEXT CONSTRAINT dds_hub_billing_period_pk PRIMARY KEY,
    billing_period_key DATE,
    load_date TIMESTAMP WITH TIME ZONE,
    record_source TEXT
);

DROP TABLE IF EXISTS asamoilov.dds_link_payment;

CREATE TABLE asamoilov.dds_link_payment(
    payment_pk TEXT CONSTRAINT dds_link_payment_pk PRIMARY KEY,
    user_pk TEXT,
    account_pk TEXT,
    billing_period_pk TEXT,
    load_date TIMESTAMP WITH TIME ZONE,
    record_source TEXT
);

DROP TABLE IF EXISTS asamoilov.dds_sat_payment_details;

CREATE TABLE asamoilov.dds_sat_payment_details(
    payment_pk TEXT,
    payment_hashdiff TEXT,
    pay_doc_type TEXT,
    pay_doc_num BIGINT,
    sum DECIMAL(10,2),
    effective_from TIMESTAMP WITHOUT TIME ZONE,
    load_date TIMESTAMP WITH TIME ZONE,
    record_source TEXT
);
