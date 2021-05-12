-- dds_hub_user

INSERT INTO asamoilov.dds_hub_user
WITH source_data AS (
    SELECT s.user_pk, s.user_key, s.load_date, s.record_source
    FROM (
        SELECT p.user_pk, p.user_key, p.load_date, p.record_source,
            row_number() OVER (PARTITION BY p.user_pk ORDER BY p.effective_from ASC) AS row_number
        FROM asamoilov.ods_payment_v p
        -- WHERE p.date_part_year = 2013
    ) s
    WHERE s.row_number = 1
)
SELECT sd.user_pk, sd.user_key, sd.load_date, sd.record_source
FROM source_data sd
LEFT JOIN asamoilov.dds_hub_user dhu ON sd.user_pk = dhu.user_pk
WHERE dhu.user_pk IS NULL;

-- dds_sat_user_details

INSERT INTO asamoilov.dds_sat_user_details
WITH source_data AS (
    SELECT p.user_pk, p.user_hashdiff, p.phone, p.effective_from, p.load_date, p.record_source,
        CASE WHEN lag(p.user_hashdiff, 1, 'none') OVER (PARTITION BY p.user_pk ORDER BY p.effective_from) = p.user_hashdiff
            THEN 'N' ELSE 'Y' END AS is_update
    FROM asamoilov.ods_payment_v p
    -- WHERE p.date_part_year = 2013
),
source_user_pk AS (
    SELECT DISTINCT sd.user_pk FROM source_data sd
),
update_records AS (
    SELECT s1.user_pk, s1.user_hashdiff, s1.phone, s1.effective_from, s1.load_date, s1.record_source, s1.effective_to
    FROM (
        SELECT s.user_pk, s.user_hashdiff, s.phone, s.effective_from, s.load_date, s.record_source,
            rank() OVER (PARTITION BY s.user_pk, s.user_hashdiff ORDER BY s.effective_from DESC) AS rank_1,
            lead(s.effective_from) OVER (PARTITION BY s.user_pk ORDER BY s.effective_from) AS effective_to
        FROM asamoilov.dds_sat_user_details s
        JOIN source_user_pk su ON s.user_pk = su.user_pk
    ) s1 WHERE s1.rank_1 = 1
)
SELECT sd1.user_pk, sd1.user_hashdiff, sd1.phone, sd1.effective_from, sd1.load_date, sd1.record_source
FROM source_data sd1
LEFT JOIN update_records ur1 ON sd1.user_pk = ur1.user_pk AND sd1.user_hashdiff = ur1.user_hashdiff
WHERE (ur1.user_hashdiff IS NULL AND (sd1.is_update = 'Y'))
    OR (sd1.effective_from > ur1.effective_to);

-- dds_hub_account

INSERT INTO asamoilov.dds_hub_account
WITH source_data AS (
    SELECT s.account_pk, s.account_key, s.load_date, s.record_source
    FROM (
        SELECT p.account_pk, p.account_key, p.load_date, p.record_source,
            row_number() OVER (PARTITION BY p.account_pk ORDER BY p.effective_from ASC) AS row_number
        FROM asamoilov.ods_payment_v p
        -- WHERE p.date_part_year = 2013
    ) s
    WHERE s.row_number = 1
)
SELECT sd.account_pk, sd.account_key, sd.load_date, sd.record_source
FROM source_data sd
LEFT JOIN asamoilov.dds_hub_account dha ON sd.account_pk = dha.account_pk
WHERE dha.account_pk IS NULL;

-- dds_hub_billing_period

INSERT INTO asamoilov.dds_hub_billing_period
WITH source_data AS (
    SELECT s.billing_period_pk, s.billing_period_key, s.load_date, s.record_source
    FROM (
        SELECT p.billing_period_pk, p.billing_period_key, p.load_date, p.record_source,
            row_number() OVER (PARTITION BY p.billing_period_pk ORDER BY p.effective_from ASC) AS row_number
        FROM asamoilov.ods_payment_v p
        -- WHERE p.date_part_year = 2013
    ) s
    WHERE s.row_number = 1
)
SELECT sd.billing_period_pk, sd.billing_period_key, sd.load_date, sd.record_source
FROM source_data sd
LEFT JOIN asamoilov.dds_hub_billing_period dhbp ON sd.billing_period_pk = dhbp.billing_period_pk
WHERE dhbp.billing_period_pk IS NULL;

-- dds_link_payment

INSERT INTO asamoilov.dds_link_payment
WITH source_data AS (
    SELECT s.payment_pk, s.user_pk, s.account_pk, s.billing_period_pk, s.load_date, s.record_source
    FROM (
        SELECT p.payment_pk, p.user_pk, p.account_pk, p.billing_period_pk, p.load_date, p.record_source,
            row_number() OVER (PARTITION BY p.payment_pk ORDER BY p.effective_from ASC) AS row_number
        FROM asamoilov.ods_payment_v p
        -- WHERE p.date_part_year = 2013
    ) s
    WHERE s.row_number = 1
)
SELECT sd.payment_pk, sd.user_pk, sd.account_pk, sd.billing_period_pk, sd.load_date, sd.record_source
FROM source_data sd
LEFT JOIN asamoilov.dds_link_payment dlp ON sd.payment_pk = dlp.payment_pk
WHERE dlp.payment_pk IS NULL;

-- dds_sat_payment_details

INSERT INTO asamoilov.dds_sat_payment_details
WITH source_data AS (
    SELECT s.payment_pk, s.payment_hashdiff,
        s.pay_doc_type, s.pay_doc_num, s.sum, s.effective_from, s.load_date, s.record_source
    FROM (
        SELECT p.payment_pk, p.payment_hashdiff,
        p.pay_doc_type, p.pay_doc_num, p.sum, p.effective_from, p.load_date, p.record_source,
            row_number() OVER (PARTITION BY p.payment_pk, p.payment_hashdiff ORDER BY p.effective_from ASC) AS row_number
        FROM asamoilov.ods_payment_v p
        -- WHERE p.date_part_year = 2013
    ) s
    WHERE s.row_number = 1
)
SELECT sd.payment_pk, sd.payment_hashdiff,
    sd.pay_doc_type, sd.pay_doc_num, sd.sum, sd.effective_from, sd.load_date, sd.record_source
FROM source_data sd
LEFT JOIN asamoilov.dds_sat_payment_details dspd ON sd.payment_pk = dspd.payment_pk
    AND sd.payment_hashdiff = dspd.payment_hashdiff
WHERE dspd.payment_hashdiff IS NULL;

