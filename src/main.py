import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, radians, sin, cos, asin, sqrt, lit

def main():
    print("Starting AIS collision detection preprocessing...")

    spark = (
        SparkSession.builder
        .appName("AIS Collision Detection")
        .getOrCreate()
    )

    data_path = "/app/data"

    CENTER_LAT = 55.225000
    CENTER_LON = 14.245000
    RADIUS_KM = 50 * 1.852
    EARTH_RADIUS_KM = 6371.0

    print(f"Checking files in: {data_path}")

    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data folder not found: {data_path}")

    files = os.listdir(data_path)

    if not files:
        raise FileNotFoundError("No files found in /app/data")

    print("Files found:")
    for file in files[:10]:
        print(f"- {file}")

    print("Loading CSV files with PySpark...")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv("/app/data/aisdk-2021-12")
    )

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
        (col("sog") >= 0) &
        (col("sog") <= 60)
    )

    df = df.filter(col("sog") >= 0.5)

    df = df.withColumn(
        "distance_from_center_km",
        2 * lit(EARTH_RADIUS_KM) * asin(
            sqrt(
                sin((radians(col("latitude")) - radians(lit(CENTER_LAT))) / 2) ** 2 +
                cos(radians(lit(CENTER_LAT))) *
                cos(radians(col("latitude"))) *
                sin((radians(col("longitude")) - radians(lit(CENTER_LON))) / 2) ** 2
            )
        )
)

    df = df.filter(col("distance_from_center_km") <= RADIUS_KM)
    print("Applied 50 nautical mile geographic filter.")

    print("Data loaded and preprocessed successfully.")

    print("Schema:")
    df.printSchema()

    print("First 10 rows:")
    df.select(
        "timestamp",
        "mmsi",
        "vessel_name",
        "latitude",
        "longitude",
        "sog",
        "distance_from_center_km",
        "Navigational status"
    ).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()