-- ods_traffic

-- DROP TABLE IF EXISTS asamoilov.ods_traffic CASCADE;
 
CREATE TABLE asamoilov.ods_traffic(
    user_id bigint,
    traffic_time timestamp without time zone,
    device_id text,
    device_ip_addr text,
    bytes_sent bigint,
    bytes_received bigint,
    date_part_year smallint
) DISTRIBUTED BY (user_id);

-- DROP VIEW IF EXISTS asamoilov.ods_traffic_hash;

CREATE VIEW asamoilov.ods_traffic_hash AS
SELECT

    user_id AS user_key,
    device_id AS device_key,

    cast(md5(nullif(upper(trim(cast(user_id AS varchar))), '')) AS text) AS user_pk,
    cast(md5(nullif(upper(trim(cast(device_id AS varchar))), '')) AS text) AS device_pk,

    cast(md5(concat_ws('||',
        coalesce(nullif(upper(trim(cast(user_id AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(device_id AS varchar))), ''), '^^')
    )) AS TEXT) AS traffic_pk,

    cast(md5(concat_ws('||',
        coalesce(nullif(upper(trim(cast(device_ip_addr AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(bytes_sent AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(bytes_received AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(traffic_time AS varchar))), ''), '^^')
    )) AS TEXT) AS traffic_hashdiff,

    device_ip_addr,
    bytes_sent,
    bytes_received,
    traffic_time AS effective_from,

    'TRAFFIC - DATA LAKE'::text AS record_source,
    current_timestamp AS load_date,

    date_part_year

FROM asamoilov.ods_traffic;
