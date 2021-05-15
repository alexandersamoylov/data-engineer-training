-- dds_link_issue

-- DROP TABLE IF EXISTS asamoilov.dds_link_issue;

CREATE TABLE asamoilov.dds_link_issue(
    issue_pk text CONSTRAINT dds_link_issue_pk PRIMARY KEY,
    user_pk text,
    service_pk text,
    load_date timestamp with time zone,
    record_source text
);

