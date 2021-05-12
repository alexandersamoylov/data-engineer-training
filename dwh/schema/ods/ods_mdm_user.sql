-- ods_billing

-- DROP TABLE IF EXISTS asamoilov.ods_mdm_user CASCADE;

CREATE TABLE asamoilov.ods_mdm_user(
    user_id bigint not null,
    legal_type text,
    district text,
    registered_at timestamp without time zone,
    billing_mode text,
    is_vip boolean not null
) DISTRIBUTED BY (user_id);

-- DROP VIEW IF EXISTS asamoilov.ods_mdm_user_hash;

CREATE VIEW asamoilov.ods_mdm_user_hash AS
SELECT

    user_id AS user_key,

    cast(md5(nullif(upper(trim(cast(user_id AS varchar))), '')) AS text) AS user_pk,

    cast(md5(concat_ws('||',
        coalesce(nullif(upper(trim(cast(legal_type AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(district AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(billing_mode AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(is_vip AS varchar))), ''), '^^'),
        coalesce(nullif(upper(trim(cast(registered_at AS varchar))), ''), '^^')
    )) AS TEXT) AS user_hashdiff,

    legal_type,
    district,
    billing_mode,
    is_vip,

    registered_at AS effective_from,

    'MDM.USER'::TEXT AS record_source,
    current_timestamp AS load_date

FROM asamoilov.ods_mdm_user;
