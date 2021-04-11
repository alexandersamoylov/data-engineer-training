# Hive 

## STG

### stg_billing

```
DROP TABLE asamoilov.stg_billing;
CREATE EXTERNAL TABLE asamoilov.stg_billing(
    user_id BIGINT,
    billing_period STRING,
    service STRING,
    tariff STRING,
    sum STRING,
    created_at STRING
) STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-asamoilov/data_lake/stg/billing';

SELECT * FROM asamoilov.stg_billing LIMIT 10;
```

### stg_issue

```
DROP TABLE asamoilov.stg_issue;
CREATE EXTERNAL TABLE asamoilov.stg_issue(
    user_id STRING,
    start_time STRING,
    end_time STRING,
    title STRING,
    description STRING,
    service STRING
) STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-asamoilov/data_lake/stg/issue';

SELECT * FROM asamoilov.stg_issue LIMIT 10;
```

### stg_payment

```
DROP TABLE asamoilov.stg_payment;
CREATE EXTERNAL TABLE asamoilov.stg_payment(
    user_id BIGINT,
    pay_doc_type STRING,
    pay_doc_num BIGINT,
    account STRING,
    phone STRING,
    billing_period STRING,
    pay_date STRING,
    sum STRING
) STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-asamoilov/data_lake/stg/payment';

SELECT * FROM asamoilov.stg_payment LIMIT 10;
```

### stg_traffic

```
DROP TABLE asamoilov.stg_traffic;
CREATE EXTERNAL TABLE asamoilov.stg_traffic(
    user_id BIGINT,
    `timestamp` STRING,
    device_id STRING,
    device_ip_addr STRING,
    bytes_sent BIGINT,
    bytes_received BIGINT
) STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-asamoilov/data_lake/stg/traffic';

SELECT * FROM asamoilov.stg_traffic LIMIT 10;
```

## ODS

### ods_billing

```
DROP TABLE asamoilov.ods_billing;
CREATE EXTERNAL TABLE asamoilov.ods_billing(
    user_id BIGINT,
    billing_period DATE,
    service STRING,
    tariff STRING,
    sum DECIMAL(10,2),
    created_at TIMESTAMP
) PARTITIONED BY (year SMALLINT)
STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-asamoilov/data_lake/ods/billing';

INSERT OVERWRITE TABLE asamoilov.ods_billing PARTITION (year=2020)
SELECT user_id,
    CAST(from_unixtime(unix_timestamp(billing_period, 'yyyy-MM')) AS DATE) AS billing_period,
    service, 
    tariff, 
    CAST(sum AS DECIMAL(10,2)) AS sum,
    CAST(created_at AS TIMESTAMP) AS created_at
FROM asamoilov.stg_billing WHERE year(created_at) = 2020;

SELECT * FROM asamoilov.ods_billing WHERE year = 2020 LIMIT 10;
```

### ods_issue

```
DROP TABLE asamoilov.ods_issue;
CREATE EXTERNAL TABLE asamoilov.ods_issue(
    user_id BIGINT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    title STRING,
    description STRING,
    service STRING
) PARTITIONED BY (year SMALLINT)
STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-asamoilov/data_lake/ods/issue';

INSERT OVERWRITE TABLE asamoilov.ods_issue PARTITION (year=2020)
SELECT CAST(user_id AS BIGINT) as user_id,
    CAST(start_time AS TIMESTAMP) AS start_time,
    CAST(end_time AS TIMESTAMP) AS end_time,
    title,
    description,
    service
FROM asamoilov.stg_issue WHERE year(start_time) = 2020;

SELECT * FROM asamoilov.ods_issue WHERE year = 2020 LIMIT 10;
```

### ods_payment

```
DROP TABLE asamoilov.ods_payment;
CREATE EXTERNAL TABLE asamoilov.ods_payment(
    user_id BIGINT,
    pay_doc_type STRING,
    pay_doc_num BIGINT,
    account STRING,
    phone STRING,
    billing_period DATE,
    pay_date TIMESTAMP,
    sum DECIMAL(10,2)
) PARTITIONED BY (year SMALLINT)
STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-asamoilov/data_lake/ods/payment';

INSERT OVERWRITE TABLE asamoilov.ods_payment PARTITION (year=2020)
SELECT user_id,
    pay_doc_type,
    pay_doc_num,
    account,
    phone,
    CAST(from_unixtime(unix_timestamp(billing_period, 'yyyy-MM')) AS DATE) AS billing_period,
    CAST(pay_date AS TIMESTAMP) AS pay_date,
    CAST(sum AS DECIMAL(10,2)) AS sum
FROM asamoilov.stg_payment WHERE year(pay_date) = 2020;

SELECT * FROM asamoilov.ods_payment WHERE year = 2020 LIMIT 10;
```

### ods_traffic

```
DROP TABLE asamoilov.ods_traffic;
CREATE EXTERNAL TABLE asamoilov.ods_traffic(
    user_id BIGINT,
    traffic_time TIMESTAMP,
    device_id STRING,
    device_ip_addr STRING,
    bytes_sent BIGINT,
    bytes_received BIGINT
) PARTITIONED BY (year SMALLINT)
STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-asamoilov/data_lake/ods/traffic';

INSERT OVERWRITE TABLE asamoilov.ods_traffic PARTITION (year=2020)
SELECT user_id,
    from_unixtime(CAST(`timestamp`/1000 as BIGINT)) AS traffic_time,
    device_id,
    device_ip_addr,
    bytes_sent,
    bytes_received
FROM asamoilov.stg_traffic 
WHERE year(from_unixtime(CAST(`timestamp`/1000 as BIGINT))) = 2020;

SELECT * FROM asamoilov.ods_traffic WHERE year = 2020 LIMIT 10;
```

## DM

### dm_traffic

```
DROP TABLE asamoilov.dm_traffic;
CREATE EXTERNAL TABLE asamoilov.dm_traffic(
    user_id BIGINT,
    bytes_received_min BIGINT,
    bytes_received_max BIGINT,
    bytes_received_avg double
) PARTITIONED BY (year SMALLINT)
STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-asamoilov/data_lake/dm/traffic';

INSERT OVERWRITE TABLE asamoilov.dm_traffic PARTITION (year=2020)
SELECT user_id,
    min(bytes_received) AS bytes_received_min,
    max(bytes_received) AS bytes_received_max,
    avg(bytes_received) AS bytes_received_avg
FROM asamoilov.ods_traffic
WHERE year = 2020
GROUP BY user_id;

SELECT * FROM asamoilov.dm_traffic WHERE year = 2020;
```

