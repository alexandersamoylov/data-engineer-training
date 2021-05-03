-- dm_payment_report_dim_billing_year

-- DROP TABLE asamoilov.dm_payment_report_dim_billing_year;

CREATE TABLE asamoilov.dm_payment_report_dim_billing_year(
    billing_year_id SERIAL PRIMARY KEY,
    billing_year_key SMALLINT
);


-- dm_payment_report_dim_legal_type

-- DROP TABLE asamoilov.dm_payment_report_dim_legal_type;

CREATE TABLE asamoilov.dm_payment_report_dim_legal_type(
    legal_type_id SERIAL PRIMARY KEY,
    legal_type_key TEXT
);


-- dm_payment_report_dim_district

-- DROP TABLE asamoilov.dm_payment_report_dim_district;

CREATE TABLE asamoilov.dm_payment_report_dim_district(
    district_id SERIAL PRIMARY KEY,
    district_key TEXT
);


-- dm_payment_report_dim_registration_year

-- DROP TABLE asamoilov.dm_payment_report_dim_registration_year;

CREATE TABLE asamoilov.dm_payment_report_dim_registration_year(
    registration_year_id SERIAL PRIMARY KEY,
    registration_year_key SMALLINT
);


-- dm_payment_report_fct

-- DROP TABLE asamoilov.dm_payment_report_fct;

CREATE TABLE asamoilov.dm_payment_report_fct(
    billing_year_id INT,
    legal_type_id INT,
    district_id INT,
    registration_year_id INT,
    is_vip BOOLEAN,
    sum DECIMAL(10,2),
    CONSTRAINT fk_billing_year FOREIGN KEY(billing_year_id)
        REFERENCES dm_payment_report_dim_billing_year(billing_year_id),
    CONSTRAINT fk_legal_type FOREIGN KEY(legal_type_id)
        REFERENCES dm_payment_report_dim_legal_type(legal_type_id),
    CONSTRAINT fk_district FOREIGN KEY(district_id) 
        REFERENCES dm_payment_report_dim_district(district_id),
    CONSTRAINT fk_registration_year FOREIGN KEY(registration_year_id)
        REFERENCES dm_payment_report_dim_registration_year(registration_year_id)
);

