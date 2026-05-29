import os
from pyspark.sql import SparkSession


def main():
    print("Starting PySpark AIS data loading test...")

    spark = (
        SparkSession.builder
        .appName("AIS Collision Detection")
        .getOrCreate()
    )

    data_path = "/app/data"

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
    .csv("/app/data/aisdk-2021-12/aisdk-2021-12-01.csv")
)

    print("Data loaded successfully.")

    print("Schema:")
    df.printSchema()

    print("First 5 rows:")
    df.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()