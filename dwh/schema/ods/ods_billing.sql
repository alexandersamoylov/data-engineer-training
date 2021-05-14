-- ods_payment

-- DROP TABLE IF EXISTS asamoilov.ods_billing CASCADE;
 
CREATE TABLE asamoilov.ods_billing(
    user_id bigint,
    billing_period date,
    service text,
    tariff text,
    billing_sum decimal(10,2),
    created_at timestamp without time zone,
    date_part_year smallint
) DISTRIBUTED BY (user_id);

-- DROP VIEW IF EXISTS asamoilov.ods_billing_hash;

CREATE VIEW asamoilov.ods_billing_hash AS
SELECT

    user_id AS user_key,
    billing_period AS billing_period_key,
    service AS service_key,
    tariff AS tariff_key,

    cast(md5(nullif(upper(trim(cast(user_id AS varchar))), '')) AS text) AS user_pk,
    cast(md5(nullif(upper(trim(cast(billing_period AS varchar))), '')) AS text) AS billing_period_pk,
    cast(md5(nullif(upper(trim(cast(service AS varchar))), '')) AS text) AS service_pk,
    cast(md5(nullif(upper(trim(cast(tariff AS varchar))), '')) AS text) AS tariff_pk,

    cast(md5(concat_ws('||',
        coalesce(nullif(upper(trim(cast(user_id AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(billing_period AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(service AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(tariff AS varchar))), ''), '^^')
    )) AS TEXT) AS billing_pk,

    cast(md5(concat_ws('||',
        coalesce(nullif(upper(trim(cast(billing_sum AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(created_at AS varchar))), ''), '^^')
    )) AS TEXT) AS billing_hashdiff,

    billing_sum,
    created_at AS effective_from,

    'BILLING - DATA LAKE'::text AS record_source,
    current_timestamp AS load_date,

    date_part_year

FROM asamoilov.ods_billing;
