-- ods_payment

DROP VIEW IF EXISTS asamoilov.ods_payment_v;

CREATE VIEW asamoilov.ods_payment_v AS
SELECT

    user_id AS user_key,
    account AS account_key,
    billing_period AS billing_period_key,

    cast((md5(nullif(upper(trim(cast(user_id AS varchar))), ''))) AS TEXT) AS user_pk,
    cast((md5(nullif(upper(trim(cast(account AS varchar))), ''))) AS TEXT) AS account_pk,
    cast((md5(nullif(upper(trim(cast(billing_period AS varchar))), ''))) AS TEXT) AS billing_period_pk,

    cast(md5(nullif(concat_ws('||',
        coalesce(nullif(upper(trim(cast(user_id AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(account AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(billing_period AS varchar))), ''), '^^')
    ), '^^||^^||^^')) AS TEXT) AS payment_pk,

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
