-- stg_issue

DROP EXTERNAL TABLE IF EXISTS asamoilov.stg_issue;

CREATE EXTERNAL TABLE asamoilov.stg_issue(
    user_id text,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    title text,
    description text,
    service text
) LOCATION ('pxf://rt-2021-03-25-16-47-29-sfunu-final-project/issue/*/?PROFILE=gs:parquet')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
