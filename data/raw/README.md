Place the raw CMS source extract here before running the pipeline:

    Timely_and_Effective_Care-Hospital_SOURCE.csv

Download it from the CMS Provider Data Catalog ("Timely and Effective Care -
Hospital" dataset). Running `pipelines/ingest_raw_cms_data.py` will populate
this directory with the standardized landing extracts
(`cms_hospital_reference.csv`, `cms_ed_throughput_extract.csv`).
