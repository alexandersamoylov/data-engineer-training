-- stg_traffic

DROP EXTERNAL TABLE IF EXISTS asamoilov.stg_traffic;

CREATE EXTERNAL TABLE asamoilov.stg_traffic(
    user_id int,
    timestamp bigint,
    device_id text,
    device_ip_addr text,
    bytes_sent int,
    bytes_received int
) LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/traffic/*/?PROFILE=gs:parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
