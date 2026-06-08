import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, lit, to_timestamp, to_date

DATA_LAKE_BASE = "/opt/airflow/data"

def convert_source(spark, source, date_str):
    input_path = f"{DATA_LAKE_BASE}/raw/{source}/{date_str}/*.json"
    output_path = f"{DATA_LAKE_BASE}/parquet/{source}/ingestion_date={date_str}"

    df = spark.read.option("multiLine", "true").json(input_path)

    df = (
        df
        .withColumn("source", lit(source))
        .withColumn("ingestion_date", to_date(lit(date_str)))
        .withColumn("input_file", input_file_name())
    )

    df.write.mode("overwrite").parquet(output_path)
    print(f"[Spark] Wrote parquet: {output_path}")

def main():
    if len(sys.argv) < 2:
        raise ValueError("Usage: raw_to_parquet.py YYYY-MM-DD")

    date_str = sys.argv[1]

    spark = (
        SparkSession.builder
        .appName("raw-json-to-parquet")
        .getOrCreate()
    )

    for source in ["weather", "restaurants", "air_quality"]:
        convert_source(spark, source, date_str)

    spark.stop()

if __name__ == "__main__":
    main()