-- ods_billing

DROP TABLE IF EXISTS asamoilov.ods_billing CASCADE;

CREATE TABLE asamoilov.ods_billing(
    user_id BIGINT,
    billing_period DATE,
    service TEXT,
    tariff TEXT,
    sum DECIMAL(10,2),
    created_at TIMESTAMP WITHOUT TIME ZONE,
    date_part_year SMALLINT
) DISTRIBUTED BY (user_id);

-- ods_issue

DROP TABLE IF EXISTS asamoilov.ods_issue CASCADE;

CREATE TABLE asamoilov.ods_issue(
    user_id BIGINT,
    start_time TIMESTAMP WITHOUT TIME ZONE,
    end_time TIMESTAMP WITHOUT TIME ZONE,
    title TEXT,
    description TEXT,
    service TEXT,
    date_part_year SMALLINT
) DISTRIBUTED BY (user_id);

-- ods_payment

DROP TABLE IF EXISTS asamoilov.ods_payment CASCADE;
 
CREATE TABLE asamoilov.ods_payment(
    user_id BIGINT,
    pay_doc_type TEXT,
    pay_doc_num BIGINT,
    account TEXT,
    phone TEXT,
    billing_period DATE,
    pay_date TIMESTAMP WITHOUT TIME ZONE,
    sum DECIMAL(10,2),
    date_part_year SMALLINT
) DISTRIBUTED BY (user_id);

-- ods_traffic

DROP TABLE IF EXISTS asamoilov.ods_traffic;

CREATE TABLE asamoilov.ods_traffic(
    user_id BIGINT,
    traffic_time TIMESTAMP WITHOUT TIME ZONE,
    device_id TEXT,
    device_ip_addr TEXT,
    bytes_sent BIGINT,
    bytes_received BIGINT,
    date_part_year SMALLINT
) DISTRIBUTED BY (user_id);
