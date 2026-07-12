-- =============================================================================
-- 01_create_raw_schema.sql
-- =============================================================================

CREATE WAREHOUSE IF NOT EXISTS ED_THROUGHPUT_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Compute for the ED throughput analytics pipeline';

CREATE DATABASE IF NOT EXISTS ED_THROUGHPUT_DB
    COMMENT = 'CMS Hospital IQR Emergency Department throughput analytics';

CREATE SCHEMA IF NOT EXISTS ED_THROUGHPUT_DB.RAW
    COMMENT = 'Landing schema for standardized CMS extracts, loaded from PySpark output';

USE WAREHOUSE ED_THROUGHPUT_WH;
USE DATABASE ED_THROUGHPUT_DB;

-- -----------------------------------------------------------------------------
-- RAW schema: mirrors the standardized (cleaned, type-cast, deduplicated)
-- extracts produced by pipelines/standardize_ed_data.py. No dimensional
-- modeling, surrogate keys, or business logic belong on these tables.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE RAW.HOSPITAL_STANDARDIZED (
    facility_id         VARCHAR(10)     NOT NULL,
    facility_name       VARCHAR(200)    NOT NULL,
    address             VARCHAR(200),
    city                VARCHAR(100),
    state               VARCHAR(2),
    zip_code            VARCHAR(10),
    county              VARCHAR(100),
    telephone_number    VARCHAR(20)
);

CREATE OR REPLACE TABLE RAW.ED_THROUGHPUT_STANDARDIZED (
    facility_id         VARCHAR(10)     NOT NULL,
    measure_id          VARCHAR(50)     NOT NULL,
    measure_name        VARCHAR(500),
    start_date          DATE,
    end_date            DATE,
    score_numeric       FLOAT,
    score_raw           VARCHAR(50),
    score_available     BOOLEAN,
    sample_size         VARCHAR(50),
    footnote            VARCHAR(10)
);
