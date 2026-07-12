from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import config

LOGGER = logging.getLogger(__name__)

ED_CONDITION_NAME = "Emergency Department"

HOSPITAL_REFERENCE_COLUMNS = [
    "Facility ID",
    "Facility Name",
    "Address",
    "City/Town",
    "State",
    "ZIP Code",
    "County/Parish",
    "Telephone Number",
]

ED_MEASURE_COLUMNS = [
    "Facility ID",
    "Condition",
    "Measure ID",
    "Measure Name",
    "Score",
    "Sample",
    "Footnote",
    "Start Date",
    "End Date",
]


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        stream=sys.stdout,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest raw CMS IQR hospital data.")
    parser.add_argument(
        "--input-path",
        type=str,
        default=f"{config.get_dir('raw')}/Timely_and_Effective_Care-Hospital_SOURCE.csv",
        help="Path (local or s3a://) to the raw CMS source CSV file.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=config.get_dir("raw"),
        help="Directory (local or s3a://) in which standardized extracts are written.",
    )
    return parser.parse_args()


def get_spark_session(app_name: str = "cms_ingest_raw") -> SparkSession:
    """Create (or fetch) a local SparkSession for the ingestion job."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_raw_extract(spark: SparkSession, input_path: str) -> DataFrame:
    """Read the raw CMS CSV extract into a Spark DataFrame."""
    if config.is_local() and not Path(input_path).exists():
        raise FileNotFoundError(f"Raw source file not found: {input_path}")

    LOGGER.info("Reading raw CMS extract from %s", input_path)
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(input_path)
    )
    LOGGER.info("Raw extract loaded: %d rows, %d columns", df.count(), len(df.columns))
    return df


def filter_emergency_department(df: DataFrame) -> DataFrame:
    """Filter the raw extract down to Emergency Department measures only."""
    ed_df = df.filter(F.col("Condition") == ED_CONDITION_NAME)
    LOGGER.info("Filtered to Condition == '%s': %d rows", ED_CONDITION_NAME, ed_df.count())
    return ed_df


def build_hospital_reference(ed_df: DataFrame) -> DataFrame:
    """Derive a deduplicated hospital reference (facility attribute) extract."""
    hospital_ref = ed_df.select(*HOSPITAL_REFERENCE_COLUMNS).dropDuplicates(["Facility ID"])
    LOGGER.info("Hospital reference extract built: %d unique facilities", hospital_ref.count())
    return hospital_ref


def build_ed_measure_extract(ed_df: DataFrame) -> DataFrame:
    """Derive the ED throughput measure extract."""
    ed_measures = ed_df.select(*ED_MEASURE_COLUMNS)
    LOGGER.info("ED measure extract built: %d rows", ed_measures.count())
    return ed_measures


def validate_extract(df: DataFrame, key_columns: list[str], name: str) -> None:
    """Run lightweight validation checks against a standardized extract."""
    row_count = df.count()
    if row_count == 0:
        raise ValueError(f"Validation failed: '{name}' extract is empty.")

    for key in key_columns:
        null_count = df.filter(F.col(key).isNull()).count()
        if null_count > 0:
            raise ValueError(
                f"Validation failed: '{name}' extract has {null_count} "
                f"null values in key column '{key}'."
            )

    LOGGER.info("Validation passed for '%s' extract (%d rows).", name, row_count)


def write_extract(df: DataFrame, output_path: str, single_file: bool = True) -> None:
    write = (df.coalesce(1) if single_file else df).write.mode("overwrite").option(
        "header", True
    )

    if not config.is_local():
        write.csv(output_path)
        LOGGER.info("Wrote extract to %s", output_path)
        return

    local_output_path = Path(output_path)
    tmp_dir = local_output_path.with_suffix(".tmp_dir")
    write.csv(str(tmp_dir))

    part_file = next(tmp_dir.glob("part-*.csv"))
    local_output_path.parent.mkdir(parents=True, exist_ok=True)
    part_file.replace(local_output_path)

    for leftover in tmp_dir.glob("*"):
        leftover.unlink()
    tmp_dir.rmdir()
    LOGGER.info("Wrote extract to %s", local_output_path)


def run(input_path: str, output_dir: str) -> None:
    """Execute the full ingestion pipeline end to end."""
    spark = get_spark_session()
    try:
        raw_df = read_raw_extract(spark, input_path)
        ed_df = filter_emergency_department(raw_df)

        hospital_reference = build_hospital_reference(ed_df)
        ed_measure_extract = build_ed_measure_extract(ed_df)

        validate_extract(hospital_reference, ["Facility ID"], "cms_hospital_reference")
        validate_extract(ed_measure_extract, ["Facility ID", "Measure ID"], "cms_ed_throughput_extract")

        write_extract(hospital_reference, f"{output_dir}/cms_hospital_reference.csv")
        write_extract(ed_measure_extract, f"{output_dir}/cms_ed_throughput_extract.csv")

        LOGGER.info("Ingestion pipeline completed successfully.")
    finally:
        spark.stop()


def main() -> None:
    configure_logging()
    args = parse_args()
    run(input_path=args.input_path, output_dir=args.output_dir)


if __name__ == "__main__":
    main()
