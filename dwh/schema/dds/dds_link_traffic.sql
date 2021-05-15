-- dds_link_traffic

-- DROP TABLE IF EXISTS asamoilov.dds_link_traffic;

CREATE TABLE asamoilov.dds_link_traffic(
    traffic_pk text CONSTRAINT dds_link_traffic_pk PRIMARY KEY,
    user_pk text,
    device_pk text,
    load_date timestamp with time zone,
    record_source text
);

