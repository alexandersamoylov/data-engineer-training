-- dm_payment_report_dim_billing_year

-- DROP TABLE asamoilov.dm_payment_report_dim_billing_year;

CREATE TABLE asamoilov.dm_payment_report_dim_billing_year(
    billing_year_id serial PRIMARY KEY,
    billing_year_key smallint
);


-- dm_payment_report_dim_legal_type

-- DROP TABLE asamoilov.dm_payment_report_dim_legal_type;

CREATE TABLE asamoilov.dm_payment_report_dim_legal_type(
    legal_type_id serial PRIMARY KEY,
    legal_type_key text
);


-- dm_payment_report_dim_district

-- DROP TABLE asamoilov.dm_payment_report_dim_district;

CREATE TABLE asamoilov.dm_payment_report_dim_district(
    district_id serial PRIMARY KEY,
    district_key text
);


-- dm_payment_report_dim_billing_mode

-- DROP TABLE asamoilov.dm_payment_report_dim_billing_mode;

CREATE TABLE asamoilov.dm_payment_report_dim_billing_mode(
    billing_mode_id serial PRIMARY KEY,
    billing_mode_key text
);


-- dm_payment_report_dim_registration_year

-- DROP TABLE asamoilov.dm_payment_report_dim_registration_year;

CREATE TABLE asamoilov.dm_payment_report_dim_registration_year(
    registration_year_id serial PRIMARY KEY,
    registration_year_key smallint
);


-- dm_payment_report_fct

-- DROP TABLE asamoilov.dm_payment_report_fct;

CREATE TABLE asamoilov.dm_payment_report_fct(
    billing_year_id int,
    legal_type_id int,
    district_id int,
    billing_mode_id int,
    registration_year_id int,
    is_vip boolean,
    payment_sum decimal(10,2),
    billing_sum decimal(10,2),
    issue_cnt int,
    traffic_amount bigint,
    CONSTRAINT fk_billing_year FOREIGN KEY(billing_year_id)
        REFERENCES dm_payment_report_dim_billing_year(billing_year_id),
    CONSTRAINT fk_legal_type FOREIGN KEY(legal_type_id)
        REFERENCES dm_payment_report_dim_legal_type(legal_type_id),
    CONSTRAINT fk_district FOREIGN KEY(district_id) 
        REFERENCES dm_payment_report_dim_district(district_id),
    CONSTRAINT fk_billing_mode FOREIGN KEY(billing_mode_id) 
        REFERENCES dm_payment_report_dim_billing_mode(billing_mode_id),
    CONSTRAINT fk_registration_year FOREIGN KEY(registration_year_id)
        REFERENCES dm_payment_report_dim_registration_year(registration_year_id)
);

