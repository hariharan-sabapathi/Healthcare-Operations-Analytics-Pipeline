from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import config

LOGGER = logging.getLogger(__name__)

# Measures that report a numeric time-based score (minutes). All other ED
# measures (e.g. EDV = volume category, OP_22 = % left before seen) keep
# Score as a categorical/percentage string.
NUMERIC_MINUTE_MEASURES = {"OP_18a", "OP_18b", "OP_18c", "OP_18d"}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        stream=sys.stdout,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Standardize CMS ED landing extracts for Snowflake RAW."
    )
    parser.add_argument("--input-dir", type=str, default=config.get_dir("raw"))
    parser.add_argument("--output-dir", type=str, default=config.get_dir("curated"))
    return parser.parse_args()


def get_spark_session(app_name: str = "cms_standardize_ed_data") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_landing_extracts(spark: SparkSession, input_dir: str) -> tuple[DataFrame, DataFrame]:
    hospital_path = f"{input_dir}/cms_hospital_reference.csv"
    measure_path = f"{input_dir}/cms_ed_throughput_extract.csv"

    if config.is_local():
        for path in (hospital_path, measure_path):
            if not Path(path).exists():
                raise FileNotFoundError(
                    f"Expected landing extract not found: {path}. "
                    "Run ingest_raw_cms_data.py first."
                )

    LOGGER.info("Reading landing extracts from %s", input_dir)
    hospital_df = spark.read.option("header", True).csv(hospital_path)
    measure_df = spark.read.option("header", True).csv(measure_path)
    return hospital_df, measure_df


def standardize_hospital_reference(hospital_df: DataFrame) -> DataFrame:
    standardized = (
        hospital_df.dropDuplicates(["Facility ID"])
        .withColumn("Facility Name", F.trim(F.col("Facility Name")))
        .withColumn("State", F.upper(F.trim(F.col("State"))))
        .withColumn("ZIP Code", F.lpad(F.trim(F.col("ZIP Code")), 5, "0"))
        .na.fill({"County/Parish": "UNKNOWN", "Telephone Number": "UNKNOWN"})
        .select(
            F.col("Facility ID").alias("facility_id"),
            F.col("Facility Name").alias("facility_name"),
            F.col("Address").alias("address"),
            F.col("City/Town").alias("city"),
            F.col("State").alias("state"),
            F.col("ZIP Code").alias("zip_code"),
            F.col("County/Parish").alias("county"),
            F.col("Telephone Number").alias("telephone_number"),
        )
    )
    LOGGER.info("Standardized hospital reference: %d rows", standardized.count())
    return standardized


def standardize_ed_measures(measure_df: DataFrame) -> DataFrame:
    standardized = (
        measure_df.dropDuplicates(["Facility ID", "Measure ID", "Start Date", "End Date"])
        .withColumn(
            "score_numeric",
            F.when(
                F.col("Measure ID").isin(list(NUMERIC_MINUTE_MEASURES))
                & (~F.col("Score").isin("Not Available", "N/A", ""))
                & F.col("Score").isNotNull(),
                F.col("Score").cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "score_available",
            ~F.col("Score").isin("Not Available", "N/A", "") & F.col("Score").isNotNull(),
        )
        .withColumn("start_date", F.to_date("Start Date", "MM/dd/yyyy"))
        .withColumn("end_date", F.to_date("End Date", "MM/dd/yyyy"))
        .select(
            F.col("Facility ID").alias("facility_id"),
            F.col("Measure ID").alias("measure_id"),
            F.col("Measure Name").alias("measure_name"),
            F.col("start_date"),
            F.col("end_date"),
            F.col("score_numeric"),
            F.col("Score").alias("score_raw"),
            F.col("score_available"),
            F.col("Sample").alias("sample_size"),
            F.col("Footnote").alias("footnote"),
        )
    )
    LOGGER.info("Standardized ED measure extract: %d rows", standardized.count())
    return standardized


def validate_extract(df: DataFrame, key_columns: list[str], name: str) -> None:
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

    duplicate_count = (
        df.groupBy(*key_columns).count().filter(F.col("count") > 1).count()
    )
    if duplicate_count > 0:
        raise ValueError(
            f"Validation failed: '{name}' extract has {duplicate_count} "
            f"duplicate key combinations across {key_columns}."
        )

    LOGGER.info("Validation passed for '%s' extract (%d rows).", name, row_count)


def write_extract(df: DataFrame, output_path: str, single_file: bool = True) -> None:
    """Write a Spark DataFrame to CSV, local-filesystem-aware (see config.py)."""
    write = (df.coalesce(1) if single_file else df).write.mode("overwrite").option(
        "header", True
    )

    if not config.is_local():
        write.csv(output_path)
        LOGGER.info("Wrote standardized extract to %s", output_path)
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
    LOGGER.info("Wrote standardized extract to %s", local_output_path)


def run(input_dir: str, output_dir: str) -> None:
    spark = get_spark_session()
    try:
        hospital_df, measure_df = read_landing_extracts(spark, input_dir)

        hospital_standardized = standardize_hospital_reference(hospital_df)
        ed_measures_standardized = standardize_ed_measures(measure_df)

        validate_extract(hospital_standardized, ["facility_id"], "hospital_standardized")
        validate_extract(
            ed_measures_standardized,
            ["facility_id", "measure_id", "start_date", "end_date"],
            "ed_throughput_standardized",
        )

        write_extract(hospital_standardized, f"{output_dir}/hospital_standardized.csv")
        write_extract(ed_measures_standardized, f"{output_dir}/ed_throughput_standardized.csv")

        LOGGER.info("Standardization pipeline completed successfully.")
    finally:
        spark.stop()


def main() -> None:
    configure_logging()
    args = parse_args()
    run(input_dir=args.input_dir, output_dir=args.output_dir)


if __name__ == "__main__":
    main()
