-- stg_payment

DROP EXTERNAL TABLE IF EXISTS asamoilov.stg_payment;

CREATE EXTERNAL TABLE asamoilov.stg_payment(
    user_id int,
    pay_doc_type text,
    pay_doc_num int,
    account text,
    phone text,
    billing_period text,
    pay_date timestamp without time zone,
    sum float8
) LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/payment/*/?PROFILE=gs:parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
