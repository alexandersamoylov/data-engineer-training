-- ods_payment

-- DROP TABLE IF EXISTS asamoilov.ods_payment CASCADE;
 
CREATE TABLE asamoilov.ods_payment(
    user_id bigint,
    pay_doc_type text,
    pay_doc_num bigint,
    account text,
    phone text,
    billing_period date,
    pay_date timestamp without time zone,
    sum decimal(10,2),
    date_part_year smallint
) DISTRIBUTED BY (user_id);

-- DROP VIEW IF EXISTS asamoilov.ods_payment_hash;

CREATE VIEW asamoilov.ods_payment_hash AS
SELECT

    user_id AS user_key,
    account AS account_key,
    billing_period AS billing_period_key,

    cast(md5(nullif(upper(trim(cast(user_id AS varchar))), '')) AS text) AS user_pk,
    cast(md5(nullif(upper(trim(cast(account AS varchar))), '')) AS text) AS account_pk,
    cast(md5(nullif(upper(trim(cast(billing_period AS varchar))), '')) AS text) AS billing_period_pk,

    cast(md5(concat_ws('||',
        coalesce(nullif(upper(trim(cast(user_id AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(account AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(billing_period AS varchar))), ''), '^^')
    )) AS TEXT) AS payment_pk,

    cast(md5(concat_ws('||',
        coalesce(nullif(upper(trim(cast(phone AS varchar))), ''), '^^')
    )) AS TEXT) AS user_hashdiff,

    phone,

    cast(md5(concat_ws('||',
        coalesce(nullif(upper(trim(cast(pay_doc_type AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(pay_doc_num AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(sum AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(pay_date AS varchar))), ''), '^^')
    )) AS TEXT) AS payment_hashdiff,

    pay_doc_type,
    pay_doc_num,
    sum,

    pay_date AS effective_from,

    'PAYMENT - DATA LAKE'::TEXT AS record_source,
    current_timestamp AS load_date,

    date_part_year

FROM asamoilov.ods_payment;
