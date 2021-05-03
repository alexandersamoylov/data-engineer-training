-- DM_PAYMENT_REPORT_TMP

CREATE TABLE asamoilov.dm_payment_report_tmp AS
WITH source_payment AS (
    SELECT
        s0.user_pk,
        s0.billing_year_key,
        s0.sum
    FROM (
        SELECT
            u.user_pk,
            date_part('year', bp.billing_period_key) AS billing_year_key,
            pd.sum AS sum
        FROM asamoilov.dds_link_payment p
        JOIN asamoilov.dds_hub_user u ON p.user_pk = u.user_pk
        JOIN asamoilov.dds_hub_billing_period bp ON p.billing_period_pk = bp.billing_period_pk
        JOIN asamoilov.dds_sat_payment_details pd ON p.payment_pk = pd.payment_pk
    ) s0
    WHERE s0.billing_year_key = 2013
),
source_user_pk AS (
    SELECT DISTINCT user_pk FROM source_payment
),
source_user AS (
    SELECT
        s1.user_pk,
        s1.legal_type_key,
        s1.district_key,
        s1.registration_year_key,
        s1.is_vip
    FROM (
        SELECT
            umd.user_pk,
            umd.legal_type AS legal_type_key,
            umd.district AS district_key,
            date_part('year', umd.effective_from) AS registration_year_key,
            umd.is_vip AS is_vip,
            row_number() OVER (PARTITION BY umd.user_pk ORDER BY umd.effective_from DESC) AS row_number
        FROM source_user_pk supk
        LEFT JOIN asamoilov.dds_sat_user_mdm_details umd ON supk.user_pk = umd.user_pk
    ) s1
    WHERE row_number = 1
)
SELECT
    sp.billing_year_key,
    su.legal_type_key,
    su.district_key,
    su.registration_year_key,
    su.is_vip,
    sum(sp.sum)
FROM source_payment sp
LEFT JOIN source_user su ON sp.user_pk = su.user_pk
GROUP BY sp.billing_year_key, su.legal_type_key, su.district_key, su.registration_year_key, su.is_vip;


-- DM_PAYMENT_REPORT_DIM_BILLING_YEAR

INSERT INTO asamoilov.dm_payment_report_dim_billing_year(billing_year_key)
SELECT DISTINCT s.billing_year_key
FROM asamoilov.dm_payment_report_tmp s
LEFT JOIN asamoilov.dm_payment_report_dim_billing_year d ON s.billing_year_key = d.billing_year_key
WHERE d.billing_year_key IS NULL;


-- DM_PAYMENT_REPORT_DIM_LEGAL_TYPE

INSERT INTO asamoilov.dm_payment_report_dim_legal_type(legal_type_key)
SELECT DISTINCT s.legal_type_key
FROM asamoilov.dm_payment_report_tmp s
LEFT JOIN asamoilov.dm_payment_report_dim_legal_type d ON s.legal_type_key = d.legal_type_key
WHERE d.legal_type_key IS NULL;


-- DM_PAYMENT_REPORT_DIM_DISTRICT

INSERT INTO asamoilov.dm_payment_report_dim_district(district_key)
SELECT DISTINCT s.district_key
FROM asamoilov.dm_payment_report_tmp s
LEFT JOIN asamoilov.dm_payment_report_dim_district d ON s.district_key = d.district_key
WHERE d.district_key IS NULL;


-- DM_PAYMENT_REPORT_DIM_REGISTRATION_YEAR

INSERT INTO asamoilov.dm_payment_report_dim_registration_year(registration_year_key)
SELECT DISTINCT s.registration_year_key
FROM asamoilov.dm_payment_report_tmp s
LEFT JOIN asamoilov.dm_payment_report_dim_registration_year d ON s.registration_year_key = d.registration_year_key
WHERE d.registration_year_key IS NULL;


-- DM_PAYMENT_REPORT_FCT

DELETE FROM asamoilov.dm_payment_report_fct 
WHERE billing_year_id = (
        SELECT billing_year_id 
        FROM asamoilov.dm_payment_report_dim_billing_year
        WHERE billing_year_key = 2013
    );

INSERT INTO asamoilov.dm_payment_report_fct
SELECT
    dby.billing_year_id,
    dlt.legal_type_id,
    dd.district_id,
    dry.registration_year_id,
    s.is_vip,
    s.sum
FROM asamoilov.dm_payment_report_tmp s
JOIN asamoilov.dm_payment_report_dim_billing_year dby ON s.billing_year_key = dby.billing_year_key
JOIN asamoilov.dm_payment_report_dim_legal_type dlt ON s.legal_type_key = dlt.legal_type_key
JOIN asamoilov.dm_payment_report_dim_district dd ON s.district_key = dd.district_key
JOIN asamoilov.dm_payment_report_dim_registration_year dry ON s.registration_year_key = dry.registration_year_key;


-- DROP_DM_PAYMENT_REPORT_TMP

DROP TABLE IF EXISTS asamoilov.dm_payment_report_tmp;
