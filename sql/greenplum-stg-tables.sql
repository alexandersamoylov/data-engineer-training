-- STG

-- stg_billing

-- DROP EXTERNAL TABLE asamoilov.stg_billing;

CREATE EXTERNAL TABLE asamoilov.stg_billing(
    user_id bigint,
    billing_period date,
    service text,
    tariff text,
    sum decimal(10,2),
    created_at timestamp without time zone
) LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-asamoilov/data_lake/ods/billing/*/?PROFILE=gs:parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- stg_issue

-- DROP EXTERNAL TABLE asamoilov.stg_issue;

CREATE EXTERNAL TABLE asamoilov.stg_issue(
    user_id bigint,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    title text,
    description text,
    service text
) LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-asamoilov/data_lake/ods/issue/*/?PROFILE=gs:parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- stg_payment

-- DROP EXTERNAL TABLE asamoilov.stg_payment;
 
CREATE EXTERNAL TABLE asamoilov.stg_payment(
    user_id bigint,
    pay_doc_type text,
    pay_doc_num bigint,
    account text,
    phone text,
    billing_period date,
    pay_date timestamp without time zone,
    sum decimal(10,2)
) LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-asamoilov/data_lake/ods/payment/*/?PROFILE=gs:parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- stg_traffic

-- DROP EXTERNAL TABLE asamoilov.stg_traffic;

CREATE EXTERNAL TABLE asamoilov.stg_traffic(
    user_id bigint,
    traffic_time timestamp without time zone,
    device_id text,
    device_ip_addr text,
    bytes_sent bigint,
    bytes_received bigint
) LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-asamoilov/data_lake/ods/traffic/*/?PROFILE=gs:parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
