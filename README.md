# Healthcare Operational Capacity & Throughput Pipeline

An end-to-end analytics engineering pipeline that turns CMS Hospital Inpatient
Quality Reporting (IQR) data into an executive dashboard for identifying
Emergency Department (ED) bottlenecks.

## Business problem

Emergency Department congestion and ambulance diversion are driven by
operational bottlenecks in patient flow that are hard to see hospital by
hospital. This project ingests CMS's national ED throughput measures,
models them into a star schema, and surfaces them in a Power BI control
tower so administrative stakeholders can compare facilities, rank
bottlenecks, and target staffing and process interventions.

## Architecture

```
CMS Hospital IQR CSV
        |
        v
Local landing zone (data/raw/)
        |
        v
PySpark ingestion -> cms_hospital_reference.csv, cms_ed_throughput_extract.csv
        |
        v
PySpark standardization -> hospital_standardized.csv, ed_throughput_standardized.csv
        |
        v
Snowflake RAW schema (landing tables only)
        |
        v
dbt staging (stg_hospital, stg_ed_throughput)
        |
        v
dbt marts (dim_hospital, dim_measure, dim_date, fct_ed_throughput)
        |
        v
dbt mart_ed_control_tower
        |
        v
Power BI Executive Control Tower dashboard
```

See `architecture/pipeline_overview.txt` for the full diagram, including the
planned AWS S3 migration path.

## Tool boundaries

Each layer of the model is owned by exactly one tool. This was a deliberate
architecture decision (see the "Design decisions" section below), not an
afterthought:

| Tool       | Owns                                                                 | Does NOT own                              |
| ---------- | --------------------------------------------------------------------- | -------------------------------------------- |
| PySpark    | Ingestion, type standardization, cleaning, deduplication               | Dimensional modeling, surrogate keys, business logic |
| Snowflake  | Storage (RAW landing tables) and compute for dbt                        | Anything computed -- it's a target, not a transformer |
| dbt        | Star schema (dims/facts), surrogate keys, business logic, testing        | Reading source files, cleaning raw text        |
| Power BI   | Presentation, connects only to `mart_ed_control_tower`                    | Any calculation that should be a tested dbt model |

## Technology stack

| Layer                 | Tool             | Purpose                                                      |
| ---------------------- | ---------------- | ------------------------------------------------------------ |
| Data source            | CMS Hospital IQR | Public healthcare operational data                            |
| ETL                    | PySpark          | Ingest, standardize types, clean, and deduplicate CMS data      |
| Local storage          | CSV files        | Landing zone and curated layer during local development        |
| Data warehouse         | Snowflake        | RAW landing tables, plus compute for dbt's transformations       |
| Analytics engineering  | dbt              | Star schema, business logic, testing, analytics mart            |
| BI and reporting       | Power BI         | Executive Control Tower dashboard                              |
| Version control        | Git and GitHub   | Source control and portfolio repository                        |
| Cloud (future)         | AWS S3           | Production-style landing and curated storage layer, config-driven |

## Folder structure

```
healthcare-ed-throughput-pipeline/
    README.md
    requirements.txt
    .gitignore
    architecture/          # architecture diagrams and design notes
    data/
        raw/               # landing zone: source CSV + landing extracts
        curated/           # standardized CSVs ready to load into Snowflake RAW
    pipelines/             # PySpark ingestion + standardization scripts, config.py
    snowflake/             # RAW schema DDL and COPY INTO scripts
    dbt/                   # staging + mart models (owns the star schema), tests, docs
    dashboards/            # Power BI build notes
    exports/               # exported dashboard screenshots / .pbix
```

## Data model

The dimensional model is built entirely in dbt, from the "Emergency
Department" condition slice of the CMS Timely and Effective Care -
Hospital dataset, after PySpark has standardized it.

**dim_hospital** - one row per CMS facility (facility_id, name, address,
city, state, zip, county, phone). Built in `dbt/models/marts/dim_hospital.sql`.

**dim_measure** - one row per ED throughput measure, with a
`measure_category` derived in dbt (Volume, Time-Based (Minutes), Rate,
Process Compliance):

| Measure ID | Description                                                        |
| ---------- | ------------------------------------------------------------------- |
| EDV        | Emergency department volume                                          |
| OP_18a     | Median time all patients spent in the ED before leaving               |
| OP_18b     | Median time patients spent in the ED, excluding transfers              |
| OP_18c     | Median time psychiatric/mental health patients spent in the ED         |
| OP_18d     | Median time patients spent in the ED before being transferred          |
| OP_22      | Percent of patients who left before being seen                        |
| OP_23      | Head CT results                                                       |

**dim_date** - one row per reporting period, including a surrogate
`date_key` (`year*10000 + month*100 + day`) generated in dbt.

**fct_ed_throughput** - one row per facility x measure x reporting
period, joined to `dim_date` for its surrogate key. Built in dbt.

**mart_ed_control_tower** - the wide, hospital-grain analytics mart that
powers the dashboard: one row per facility with every key measure pivoted
into its own column, plus national and in-state bottleneck rankings
(computed in dbt via `rank() over (...)`).

## Pipeline validation

Every script and model in this repository was actually run against the
real CMS dataset (138,173 source rows; 32,620 Emergency Department rows
across 4,660 facilities) while building it:

- `ingest_raw_cms_data.py` filtered to the Emergency Department condition
  and produced the two landing extracts, with row-count and null-key
  validation passing.
- `standardize_ed_data.py` standardized types, cleaned, and deduplicated
  both extracts, with data-quality validation (nulls, duplicate keys)
  passing. It produces no dimensions, facts, or surrogate keys.
- The dbt project (7 models, 22 tests) was validated locally against
  those standardized CSVs, loaded into a RAW schema, and all 22 tests
  passed: uniqueness and not-null checks on every dimension key, and
  relationship tests tying the fact table back to `dim_hospital`,
  `dim_measure`, and `dim_date`.
- The resulting `mart_ed_control_tower` output was spot-checked and
  matches expectations: e.g. the highest facility-level median ED time
  nationally is ~505 minutes (Manati Medical Center, PR), with the
  highest state averages concentrated in DC, Puerto Rico, and Maryland.

Sample standardized output is included under `data/curated/` so the
model can be inspected without re-running Spark.

## How to run

### 1. Local PySpark pipeline

```bash
pip install -r requirements.txt

# place the raw CMS extract at data/raw/Timely_and_Effective_Care-Hospital_SOURCE.csv
python pipelines/ingest_raw_cms_data.py
python pipelines/standardize_ed_data.py
```

This produces `hospital_standardized.csv` and `ed_throughput_standardized.csv`
in `data/curated/` -- cleaned and type-cast, not yet dimensionally modeled.

To point the pipeline at S3 instead of local disk, set one environment
variable before running either script:

```bash
export CMS_STORAGE_MODE=s3
export CMS_S3_BUCKET=healthcare-ed-throughput
```

See `pipelines/config.py` for details. (Running against S3 also requires
the hadoop-aws/aws-java-sdk-bundle Spark packages and AWS credentials --
not needed for local development.)

### 2. Snowflake (RAW schema only)

```sql
-- in Snowsight, run in order:
snowflake/01_create_raw_schema.sql
snowflake/02_copy_into_stage.sql
```

During local development, upload the standardized CSVs via Snowsight's
drag-and-drop loader (or `PUT`) into the named stage before running the
`COPY INTO` statements. This step loads RAW tables only -- the STAR
schema (dimensions, fact, mart) does not exist yet at this point.

### 3. dbt (builds the entire star schema)

```bash
cd dbt
cp profiles.yml.example ~/.dbt/profiles.yml   # fill in your Snowflake credentials
dbt debug
dbt run
dbt test
dbt docs generate && dbt docs serve
```

`dbt run` creates every STAR-schema object -- `dim_hospital`,
`dim_measure`, `dim_date`, `fct_ed_throughput`, and
`mart_ed_control_tower` -- from the two RAW tables. Nothing upstream of
dbt creates or knows about these tables.

### 4. Power BI

Connect Power BI only to `mart_ed_control_tower` (never directly to the
RAW or staging tables) and build the Executive Control Tower dashboard
described in `dashboards/`.

## Executive Control Tower dashboard

KPIs and visuals built from `mart_ed_control_tower`:

- Median ED arrival-to-departure time (national average and by facility)
- Average ambulance transfer delay
- Psychiatric/mental health ED delay
- Percent of patients who left before being seen
- Hospital bottleneck ranking, nationally and within state
- State-level comparison of ED throughput
- ED volume category mix across all reporting facilities

Across the 4,660 facilities in this dataset, the median ED
arrival-to-departure time nationally is approximately 163 minutes, with
the highest facility-level medians concentrated in Washington DC,
Maryland, and Puerto Rico.

## Design decisions

This project deliberately keeps each tool to a single responsibility,
matching how modern analytics engineering teams structure ELT pipelines:

- **PySpark does not build the star schema.** An earlier version of this
  repo had a `build_star_schema.py` script that constructed
  `dim_hospital`/`dim_measure`/`dim_date`/`fct_ed_throughput` directly in
  Spark. That put dimensional modeling in two places at once (Spark and
  dbt), which is harder to test and harder to reason about. It was
  replaced with `standardize_ed_data.py`, which only cleans, types, and
  deduplicates -- dbt now owns 100% of the dimensional modeling.
- **PySpark is used here to demonstrate distributed-processing tooling
  for a portfolio, not because 138K rows requires it.** Its scope is
  intentionally kept to ingestion/cleaning so that reasoning holds up:
  the tool's presence is justified by what it does, not stretched to
  justify its presence.
- **Snowflake's RAW schema only ever holds standardized, not dimensional,
  data.** No DDL for `dim_*`/`fct_*` tables exists in `snowflake/` --
  those are created by `dbt run`.
- **All business logic** (bottleneck rankings, measure categorization,
  national/state averages) **lives in dbt**, not in Power BI measures or
  Spark transformations, so it's version-controlled and tested once.
- **Power BI connects to a single semantic surface** (`mart_ed_control_tower`),
  so two reports can never compute the same KPI two different ways.

## Future improvements

- Migrate the landing and curated layers to AWS S3 (Phase 5); only the
  Snowflake stage definition changes, from an internal named stage to an
  external stage backed by a storage integration. `pipelines/config.py`
  already makes this a one-variable switch.
- Add incremental/merge loading in dbt once CMS publishes updated
  reporting periods, rather than full-refresh.
- Extend the model to the Sepsis Care and Electronic Clinical Quality
  Measure conditions in the same source file for a broader operational
  view.
- Add a CI job (GitHub Actions) that runs `dbt build` on every pull
  request against a Snowflake dev database.
