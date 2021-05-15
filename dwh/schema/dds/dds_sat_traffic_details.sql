-- dds_sat_traffic_details

-- DROP TABLE IF EXISTS asamoilov.dds_sat_traffic_details;

CREATE TABLE asamoilov.dds_sat_traffic_details(
    traffic_pk text,
    traffic_hashdiff text,
    device_ip_addr text,
    bytes_sent bigint,
    bytes_received bigint,
    effective_from timestamp without time zone,
    load_date timestamp with time zone,
    record_source text
);

