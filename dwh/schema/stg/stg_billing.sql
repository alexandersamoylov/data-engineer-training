-- stg_billing

DROP EXTERNAL TABLE IF EXISTS asamoilov.stg_billing;

CREATE EXTERNAL TABLE asamoilov.stg_billing(
    user_id int,
    billing_period text,
    service text,
    tariff text,
    sum decimal(10,2),
    created_at timestamp without time zone
) LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/billing/*/?PROFILE=gs:parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
