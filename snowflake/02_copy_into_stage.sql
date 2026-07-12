-- =============================================================================
-- 02_copy_into_stage.sql
-- Loads the standardized CSVs (data/curated/) into Snowflake's RAW schema.
--
-- Written for the LOCAL development phase: files are pushed to an internal
-- named stage via PUT (or Snowsight's drag-and-drop loader) and loaded with
-- COPY INTO. When the project migrates to AWS (Phase 5), only the stage
-- definition changes to an external stage backed by a storage integration --
-- COPY INTO and every downstream dbt model stay identical.
--
-- This script loads RAW tables only. dbt builds everything from here
-- (`dbt run` in the dbt/ directory) -- there is no manual load step for
-- the STAR schema.
-- =============================================================================

USE WAREHOUSE ED_THROUGHPUT_WH;
USE DATABASE ED_THROUGHPUT_DB;

-- -----------------------------------------------------------------------------
-- File format shared by every load.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FILE FORMAT ED_THROUGHPUT_DB.RAW.CSV_STANDARD
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL')
    EMPTY_FIELD_AS_NULL = TRUE;

-- -----------------------------------------------------------------------------
-- Internal named stage (local dev). In Phase 5 this becomes:
--   CREATE STAGE ED_THROUGHPUT_DB.RAW.CURATED_S3_STAGE
--     URL = 's3://<PLACEHOLDER_BUCKET>/curated/'
--     STORAGE_INTEGRATION = <PLACEHOLDER_STORAGE_INTEGRATION>
--     FILE_FORMAT = ED_THROUGHPUT_DB.RAW.CSV_STANDARD;
-- -----------------------------------------------------------------------------

CREATE OR REPLACE STAGE ED_THROUGHPUT_DB.RAW.CURATED_LOCAL_STAGE
    FILE_FORMAT = ED_THROUGHPUT_DB.RAW.CSV_STANDARD;

-- Upload standardized CSVs into the stage (run from SnowSQL CLI, or use
-- Snowsight's "Load Data" drag-and-drop instead of PUT during local dev):
--   PUT file://data/curated/hospital_standardized.csv       @ED_THROUGHPUT_DB.RAW.CURATED_LOCAL_STAGE;
--   PUT file://data/curated/ed_throughput_standardized.csv  @ED_THROUGHPUT_DB.RAW.CURATED_LOCAL_STAGE;

-- -----------------------------------------------------------------------------
-- COPY INTO the RAW tables.
-- -----------------------------------------------------------------------------

COPY INTO ED_THROUGHPUT_DB.RAW.HOSPITAL_STANDARDIZED
FROM @ED_THROUGHPUT_DB.RAW.CURATED_LOCAL_STAGE/hospital_standardized.csv
FILE_FORMAT = (FORMAT_NAME = ED_THROUGHPUT_DB.RAW.CSV_STANDARD)
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO ED_THROUGHPUT_DB.RAW.ED_THROUGHPUT_STANDARDIZED
FROM @ED_THROUGHPUT_DB.RAW.CURATED_LOCAL_STAGE/ed_throughput_standardized.csv
FILE_FORMAT = (FORMAT_NAME = ED_THROUGHPUT_DB.RAW.CSV_STANDARD)
ON_ERROR = 'ABORT_STATEMENT';

-- -----------------------------------------------------------------------------
-- Quick load sanity check.
-- -----------------------------------------------------------------------------

SELECT 'HOSPITAL_STANDARDIZED' AS table_name, COUNT(*) AS row_count
FROM ED_THROUGHPUT_DB.RAW.HOSPITAL_STANDARDIZED
UNION ALL
SELECT 'ED_THROUGHPUT_STANDARDIZED', COUNT(*)
FROM ED_THROUGHPUT_DB.RAW.ED_THROUGHPUT_STANDARDIZED;

-- Next step: cd dbt/ && dbt run && dbt test
-- dbt builds the STAR schema (dimensions, fact, and mart) from these two
-- RAW tables -- see dbt/models/staging/ and dbt/models/marts/.
