import os
import glob
import shutil
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, date_format


def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def write_single_csv(df, output_file):
    temp_dir = output_file + "_tmp"

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    if os.path.exists(output_file):
        os.remove(output_file)

    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

    part_files = glob.glob(os.path.join(temp_dir, "part-*.csv"))
    if not part_files:
        raise FileNotFoundError(f"No CSV part file found in {temp_dir}")

    shutil.move(part_files[0], output_file)
    shutil.rmtree(temp_dir)


def main():
    input_path = "/app/output/collision_result"
    output_file = "/app/output/collision_result_readable.csv"

    if not os.path.exists(input_path):
        raise FileNotFoundError(
            f"Final result folder not found: {input_path}. Run rank_collision_candidates.py first."
        )

    spark = (
        SparkSession.builder
        .appName("Export Readable Collision Result")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    log(f"Reading final result from: {input_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    if df.count() == 0:
        raise ValueError("Final collision result is empty.")

    readable = df.select(
        date_format(col("collision_timestamp"), "yyyy-MM-dd HH:mm:ss").alias("collision_time"),
        spark_round(col("collision_latitude"), 6).alias("collision_latitude"),
        spark_round(col("collision_longitude"), 6).alias("collision_longitude"),

        col("vessel_name_1"),
        col("mmsi_1"),
        date_format(col("timestamp_1"), "yyyy-MM-dd HH:mm:ss").alias("vessel_1_ais_time"),
        spark_round(col("latitude_1"), 6).alias("vessel_1_latitude"),
        spark_round(col("longitude_1"), 6).alias("vessel_1_longitude"),
        spark_round(col("sog_1"), 2).alias("vessel_1_speed_knots"),

        col("vessel_name_2"),
        col("mmsi_2"),
        date_format(col("timestamp_2"), "yyyy-MM-dd HH:mm:ss").alias("vessel_2_ais_time"),
        spark_round(col("latitude_2"), 6).alias("vessel_2_latitude"),
        spark_round(col("longitude_2"), 6).alias("vessel_2_longitude"),
        spark_round(col("sog_2"), 2).alias("vessel_2_speed_knots"),

        spark_round(col("distance_m"), 2).alias("distance_between_vessels_m"),
        col("time_diff_seconds"),
        spark_round(col("score"), 2).alias("collision_score"),
    )

    log(f"Writing readable CSV to: {output_file}")
    write_single_csv(readable, output_file)

    log("Readable final result created successfully.")

    spark.stop()


if __name__ == "__main__":
    main()
