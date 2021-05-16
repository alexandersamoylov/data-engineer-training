-- DM_PAYMENT_REPORT_TMP

DROP TABLE IF EXISTS asamoilov.dm_payment_report_tmp;

CREATE TABLE asamoilov.dm_payment_report_tmp_{{ execution_date.year }} AS
WITH source_user AS (    
    SELECT
        su.user_pk,
        date_part('year', bp.billing_period_key) AS billing_year_key,
        su.legal_type_key,
        su.district_key,
        su.billing_mode_key,
        su.registration_year_key,
        su.is_vip
    FROM (
        SELECT
            u.user_pk,
            um.legal_type AS legal_type_key,
            um.district AS district_key,
            um.billing_mode AS billing_mode_key,
            date_part('year', um.effective_from) AS registration_year_key,
            um.is_vip AS is_vip,
            row_number() OVER (PARTITION BY um.user_pk ORDER BY um.effective_from DESC) AS row_number
        FROM asamoilov.dds_hub_user u
        LEFT JOIN asamoilov.dds_sat_user_mdm um ON u.user_pk = um.user_pk
    ) su, asamoilov.dds_hub_billing_period bp
    -- JOIN asamoilov.dds_link_billing b ON su.user_pk = b.user_pk
    -- JOIN asamoilov.dds_hub_billing_period bp ON b.billing_period_pk = bp.billing_period_pk
    WHERE su.row_number = 1
        -- AND date_part('year', bp.billing_period_key) = {{ execution_date.year }}
    GROUP BY su.user_pk,
        date_part('year', bp.billing_period_key),
        su.legal_type_key,
        su.district_key,
        su.billing_mode_key,
        su.registration_year_key,
        su.is_vip
),
source_payment AS (
    SELECT
        sp.user_pk,
        sp.billing_year_key,
        sum(sp.payment_sum) AS payment_sum
    FROM (
        SELECT
            p1.user_pk,
            date_part('year', bp1.billing_period_key) AS billing_year_key,
            pd1.payment_sum
        FROM asamoilov.dds_link_payment p1
        JOIN asamoilov.dds_hub_billing_period bp1 ON p1.billing_period_pk = bp1.billing_period_pk
        JOIN asamoilov.dds_sat_payment_details pd1 ON p1.payment_pk = pd1.payment_pk
        -- WHERE date_part('year', bp1.billing_period_key) = {{ execution_date.year }}
    ) sp
    GROUP BY sp.user_pk, sp.billing_year_key
),
source_billing AS (
    SELECT
        sb.user_pk,
        sb.billing_year_key,
        sum(sb.billing_sum) AS billing_sum
    FROM (
        SELECT b2.user_pk,
            date_part('year', bp2.billing_period_key) AS billing_year_key,
            bd2.billing_sum
        FROM asamoilov.dds_link_billing b2
        JOIN asamoilov.dds_hub_billing_period bp2 ON b2.billing_period_pk = bp2.billing_period_pk
        JOIN asamoilov.dds_sat_billing_details bd2 ON b2.billing_pk = bd2.billing_pk
        -- WHERE date_part('year', bp2.billing_period_key) = {{ execution_date.year }}
    ) sb
    GROUP BY sb.user_pk, sb.billing_year_key
),
source_issue AS (
    SELECT
        si.user_pk,
        si.billing_year_key,
        count(*) AS issue_cnt
    FROM (
        SELECT i3.user_pk,
            date_part('year', id3.start_time) AS billing_year_key,
            row_number() OVER (PARTITION BY id3.issue_pk, id3.issue_hashdiff ORDER BY id3.effective_from DESC) AS row_number
        FROM asamoilov.dds_link_issue i3
        JOIN asamoilov.dds_sat_issue_details id3 ON i3.issue_pk = id3.issue_pk
        -- WHERE date_part('year', id3.start_time) = {{ execution_date.year }}
    ) si
    WHERE si.row_number = 1
    GROUP BY si.user_pk, si.billing_year_key
),
source_traffic AS (
    SELECT
        st.user_pk,
        st.billing_year_key,
        sum(st.traffic_amount) AS traffic_amount
    FROM (
        SELECT t4.user_pk,
            date_part('year', td4.effective_from) AS billing_year_key,
            bytes_sent + bytes_received AS traffic_amount
        FROM asamoilov.dds_link_traffic t4
        JOIN asamoilov.dds_sat_traffic_details td4 ON t4.traffic_pk = td4.traffic_pk
        -- WHERE date_part('year', td4.effective_from) = {{ execution_date.year }}
    ) st
    GROUP BY st.user_pk, st.billing_year_key
)
SELECT
    su0.legal_type_key,
    su0.district_key,
    su0.billing_mode_key,
    su0.registration_year_key,
    su0.is_vip,
    sum(sp0.payment_sum) AS payment_sum,
    sum(sb0.billing_sum) AS billing_sum,
    sum(si0.issue_cnt) AS issue_cnt,
    sum(st0.traffic_amount) AS traffic_amount
FROM source_user su0
LEFT JOIN source_payment sp0 ON su0.user_pk = sp0.user_pk AND su0.billing_year_key = sp0.billing_year_key
LEFT JOIN source_billing sb0 ON su0.user_pk = sb0.user_pk AND su0.billing_year_key = sb0.billing_year_key
LEFT JOIN source_issue si0 ON su0.user_pk = si0.user_pk AND su0.billing_year_key = si0.billing_year_key
LEFT JOIN source_traffic st0 ON su0.user_pk = st0.user_pk AND su0.billing_year_key = st0.billing_year_key
GROUP BY su0.legal_type_key,
    su0.district_key,
    su0.billing_mode_key,
    su0.registration_year_key,
    su0.is_vip;


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


-- DM_PAYMENT_REPORT_DIM_BILLING_MODE

INSERT INTO asamoilov.dm_payment_report_dim_billing_mode(billing_mode_key)
SELECT DISTINCT s.billing_mode_key
FROM asamoilov.dm_payment_report_tmp_{{ execution_date.year }} s
LEFT JOIN asamoilov.dm_payment_report_dim_billing_mode d ON s.billing_mode_key = d.billing_mode_key
WHERE d.billing_mode_key IS NULL;


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
        -- WHERE billing_year_key = {{ execution_date.year }}
    );

INSERT INTO asamoilov.dm_payment_report_fct
SELECT
    dby.billing_year_id,
    dlt.legal_type_id,
    dd.district_id,
    dbm.billing_mode_id,
    dry.registration_year_id,
    s.is_vip,
    s.payment_sum,
    s.billing_sum,
    s.issue_cnt,
    s.traffic_amount
FROM asamoilov.dm_payment_report_tmp_{{ execution_date.year }} s
JOIN asamoilov.dm_payment_report_dim_billing_year dby ON s.billing_year_key = dby.billing_year_key
JOIN asamoilov.dm_payment_report_dim_legal_type dlt ON s.legal_type_key = dlt.legal_type_key
JOIN asamoilov.dm_payment_report_dim_district dd ON s.district_key = dd.district_key
JOIN asamoilov.dm_payment_report_dim_billing_mode dbm ON s.billing_mode_key = dbm.billing_mode_key
JOIN asamoilov.dm_payment_report_dim_registration_year dry ON s.registration_year_key = dry.registration_year_key;


-- DROP_DM_PAYMENT_REPORT_TMP

DROP TABLE IF EXISTS asamoilov.dm_payment_report_tmp;

