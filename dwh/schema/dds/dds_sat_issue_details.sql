-- dds_sat_issue_details

-- DROP TABLE IF EXISTS asamoilov.dds_sat_issue_details;

CREATE TABLE asamoilov.dds_sat_issue_details(
    issue_pk text,
    issue_hashdiff text,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    title text,
    description text,
    effective_from timestamp without time zone,
    load_date timestamp with time zone,
    record_source text
);

