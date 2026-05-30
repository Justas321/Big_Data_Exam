import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    radians,
    sin,
    cos,
    asin,
    sqrt,
    lit,
    unix_timestamp,
    floor,
)


def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def main():
    log("Starting AIS collision detection preprocessing...")

    spark = (
        SparkSession.builder
        .appName("AIS Collision Detection - Preprocessing")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .getOrCreate()
    )

    data_path = "/app/data"
    raw_data_path = "/app/data/aisdk-2021-12"
    output_path = "/app/output/preprocessed_ais_december"

    center_lat = 55.225000
    center_lon = 14.245000
    radius_km = 50 * 1.852
    earth_radius_km = 6371.0
    time_bucket_seconds = 10

    log(f"Checking data folder: {data_path}")

    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data folder not found: {data_path}")

    files = os.listdir(data_path)

    if not files:
        raise FileNotFoundError("No files found in /app/data")

    log("Files found:")
    for file in files[:10]:
        log(f"- {file}")

    log("Loading raw CSV files with PySpark...")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(raw_data_path)
    )

    log("Renaming and converting columns...")

    df = (
        df
        .withColumnRenamed("# Timestamp", "timestamp_raw")
        .withColumnRenamed("Latitude", "latitude")
        .withColumnRenamed("Longitude", "longitude")
        .withColumnRenamed("SOG", "sog")
        .withColumnRenamed("MMSI", "mmsi")
        .withColumnRenamed("Name", "vessel_name")
    )

    df = (
        df
        .withColumn("timestamp", to_timestamp(col("timestamp_raw"), "dd/MM/yyyy HH:mm:ss"))
        .withColumn("latitude", col("latitude").cast("double"))
        .withColumn("longitude", col("longitude").cast("double"))
        .withColumn("sog", col("sog").cast("double"))
    )

    log("Applying date, coordinate, and speed filters...")

    df = df.filter(
        (col("timestamp") >= "2021-12-01 00:00:00") &
        (col("timestamp") <= "2021-12-31 23:59:59")
    )

    df = df.filter(
        col("timestamp").isNotNull() &
        col("mmsi").isNotNull() &
        col("latitude").between(-90, 90) &
        col("longitude").between(-180, 180)
    )

    df = df.filter(
        col("sog").isNotNull() &
        (col("sog") >= 0.5) &
        (col("sog") <= 60)
    )

    log("Applying 50 nautical mile geographic filter...")

    df = df.withColumn(
        "distance_from_center_km",
        2 * lit(earth_radius_km) * asin(
            sqrt(
                sin((radians(col("latitude")) - radians(lit(center_lat))) / 2) ** 2 +
                cos(radians(lit(center_lat))) *
                cos(radians(col("latitude"))) *
                sin((radians(col("longitude")) - radians(lit(center_lon))) / 2) ** 2
            )
        )
    )

    df = df.filter(col("distance_from_center_km") <= radius_km)

    log("Creating time buckets...")

    df = df.withColumn(
        "time_bucket",
        floor(unix_timestamp(col("timestamp")) / time_bucket_seconds)
    )

    df = df.select(
        "timestamp",
        "time_bucket",
        "mmsi",
        "vessel_name",
        "latitude",
        "longitude",
        "sog",
        "distance_from_center_km",
    )

    log(f"Saving preprocessed data to Parquet: {output_path}")

    df = df.repartition(8, "time_bucket")

    df.write.mode("overwrite").parquet(output_path)

    log("Preprocessing completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()