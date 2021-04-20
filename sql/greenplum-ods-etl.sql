-- ods_billing

TRUNCATE asamoilov.ods_billing;

INSERT INTO asamoilov.ods_billing
SELECT user_id, billing_period, service, tariff, sum, created_at,
    date_part('year', created_at) AS date_part_year
FROM asamoilov.stg_billing;

-- ods_issue

TRUNCATE asamoilov.ods_issue;

INSERT INTO asamoilov.ods_issue
SELECT user_id, start_time, end_time, title, description, service,
    date_part('year', start_time) AS date_part_year
FROM asamoilov.stg_issue;

-- ods_payment

TRUNCATE asamoilov.ods_payment;

INSERT INTO asamoilov.ods_payment
SELECT user_id, pay_doc_type, pay_doc_num, account, phone, billing_period, pay_date, sum,
    date_part('year', pay_date) AS date_part_year
FROM asamoilov.stg_payment;

-- ods_traffic

TRUNCATE asamoilov.ods_traffic;

INSERT INTO asamoilov.ods_traffic
SELECT user_id, traffic_time, device_id, device_ip_addr, bytes_sent, bytes_received,
    date_part('year', traffic_time) AS date_part_year
FROM asamoilov.stg_traffic;
