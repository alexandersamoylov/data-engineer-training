-- ods_issue

-- DROP TABLE IF EXISTS asamoilov.ods_issue CASCADE;
 
CREATE TABLE asamoilov.ods_issue(
    user_id bigint,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    title text,
    description text,
    service text,
    date_part_year smallint
) DISTRIBUTED BY (user_id);

-- DROP VIEW IF EXISTS asamoilov.ods_issue_hash;

CREATE VIEW asamoilov.ods_issue_hash AS
SELECT

    user_id AS user_key,
    service AS service_key,

    cast(md5(nullif(upper(trim(cast(user_id AS varchar))), '')) AS text) AS user_pk,
    cast(md5(nullif(upper(trim(cast(service AS varchar))), '')) AS text) AS service_pk,

    cast(md5(concat_ws('||',
        coalesce(nullif(upper(trim(cast(user_id AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(service AS varchar))), ''), '^^')
    )) AS TEXT) AS issue_pk,

    cast(md5(concat_ws('||',
        coalesce(nullif(upper(trim(cast(start_time AS varchar))), ''), '^^'),
        -- coalesce(nullif(upper(trim(cast(end_time AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(title AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(description AS varchar))), ''), '^^')
    )) AS TEXT) AS issue_hashdiff,

    start_time,
    end_time,
    title,
    description,

    CASE
        WHEN end_time IS NULL THEN start_time
        ELSE end_time
    END AS effective_from,

    'ISSUE - DATA LAKE'::TEXT AS record_source,
    current_timestamp AS load_date,

    date_part_year

FROM asamoilov.ods_issue;
