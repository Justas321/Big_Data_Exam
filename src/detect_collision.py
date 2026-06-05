import os
from datetime import datetime, timezone
import shutil
from pyspark import StorageLevel

from pyspark.shell import spark
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lit,
    lag,
    unix_timestamp,
    radians,
    sin,
    cos,
    asin,
    sqrt,
    when,
    floor,
    abs as spark_abs,
    sequence,
    explode,
    from_unixtime,
    to_date,
    min as spark_min,
    max as spark_max
)

# Collision detection pipeline.
# Reads preprocessed AIS data, filters invalid points, identifies potential close
# encounters between vessel pairs, and writes candidate and final collision output.

def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)

def haversine_m(lat1, lon1, lat2, lon2):
    """
    Returns geodesic distance in meters between two latitude/longitude points.
    Uses the Haversine formula so the computation can be executed in Spark.
    """
    """
    Returns distance in meters between two latitude/longitude points.
    Uses the Haversine formula.
    """

    earth_radius_m = 6_371_000.0

    dlat = radians(lat2) - radians(lat1)
    dlon = radians(lon2) - radians(lon1)

    sin_dlat = sin(dlat / 2)
    sin_dlon = sin(dlon / 2)

    a = (
        sin_dlat * sin_dlat
        + cos(radians(lat1))
        * cos(radians(lat2))
        * sin_dlon
        * sin_dlon
    )

    return 2 * lit(earth_radius_m) * asin(sqrt(a))

def main():
    log("Starting collision detection...")

    # Initialize the Spark session and tuning parameters for local execution.
    spark = (
    SparkSession.builder
    .appName("AIS Collision Detection")
    .master("local[4]")
    .config("spark.sql.shuffle.partitions", "64")
    .config("spark.default.parallelism", "64")
    .config("spark.driver.memory", "3g")
    .config("spark.executor.memory", "3g")
    .config("spark.driver.maxResultSize", "512m")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)

    spark.sparkContext.setLogLevel("ERROR")

    input_path = "/app/output/preprocessed_ais_december"
    output_dir = "/app/output"

    candidates_path = os.path.join(output_dir, "collision_candidates")
    result_path = os.path.join(output_dir, "collision_result")

    daily_candidates_path = os.path.join(output_dir, "collision_candidates_daily_tmp")

    for path in [daily_candidates_path, candidates_path, result_path]:
        if os.path.exists(path):
            shutil.rmtree(path)

    # Main detection parameters.
    # These are intentionally explicit because they are part of the methodology.
    min_moving_sog_knots = 0.5
    max_reasonable_sog_knots = 60.0

    # Used to remove impossible GPS jumps between consecutive AIS points.
    # A vessel suddenly "moving" faster than this is treated as noisy AIS data.
    max_implied_speed_knots = 80.0

    # If the gap is very large, implied speed becomes less reliable.
    # We do not remove points only because the previous point is too old.
    max_gap_for_jump_check_seconds = 3600

    # Vessels are compared only if their AIS messages are close in time.
    max_time_difference_seconds = 30

    # Collision/proximity threshold.
    # AIS positions are noisy and timestamps are not perfectly synchronized,
    # so we allow a wider window to avoid dropping real contact events.
    collision_distance_threshold_m = 100.0

    # Spatial grid size used to reduce pairwise comparisons. Each bin is roughly
    # a few hundred meters in latitude/longitude so we can join nearby points.
    grid_size_degrees = 0.005

    # Time bin width for collision candidate joins.
    time_bin_seconds = 30

    # Process the day in chunks of time bins to limit memory footprint.
    # For example, 360 bins × 30 seconds = 3 hours of data per chunk.
    time_bin_chunk_size = 360

    log(f"Reading preprocessed AIS data from: {input_path}")

    raw_df = spark.read.parquet(input_path).withColumn("date", to_date(col("timestamp")))

    log("Processing collision candidates day by day to reduce memory use...")

    dates = [f"2021-12-{day:02d}" for day in range(1, 32)]

    for current_date in dates:
        log(f"Processing date: {current_date}")

        log(f"Applying filters for date: {current_date}")

        day_df = (
            raw_df
            .filter(col("date") == lit(current_date))
            .filter(col("timestamp").isNotNull())
            .filter(col("mmsi").isNotNull())
            .filter(col("latitude").between(-90, 90))
            .filter(col("longitude").between(-180, 180))
            .filter(col("sog").isNotNull())
            .filter(col("sog") >= min_moving_sog_knots)
            .filter(col("sog") <= max_reasonable_sog_knots)
            .dropDuplicates(["mmsi", "timestamp", "latitude", "longitude"])
        )

        log(f"Detecting and filtering GPS jumps/noise for date: {current_date}")

        vessel_window = Window.partitionBy("mmsi").orderBy("timestamp")

        day_df = (
            day_df
            .withColumn("prev_timestamp", lag("timestamp").over(vessel_window))
            .withColumn("prev_latitude", lag("latitude").over(vessel_window))
            .withColumn("prev_longitude", lag("longitude").over(vessel_window))
        )

        day_df = day_df.withColumn(
            "time_gap_seconds",
            unix_timestamp(col("timestamp")) - unix_timestamp(col("prev_timestamp"))
        )

        day_df = day_df.withColumn(
            "segment_distance_m",
            when(
                col("prev_latitude").isNotNull() & col("prev_longitude").isNotNull(),
                haversine_m(
                    col("prev_latitude"),
                    col("prev_longitude"),
                    col("latitude"),
                    col("longitude"),
                )
            )
        )

        day_df = day_df.withColumn(
            "implied_speed_knots",
            when(
                col("time_gap_seconds") > 0,
                (col("segment_distance_m") / 1000.0) / (col("time_gap_seconds") / 3600.0) / 1.852
            )
        )

        # Filter out AIS points that imply impossible vessel speed jumps.
        # Keep the first point for each vessel and any points where the implied
        # speed is reasonable or the previous point is too old to compare.
        day_df_base = (
            day_df
            .filter(
                col("prev_timestamp").isNull()
                | (
                    (col("time_gap_seconds") > 0)
                    & (
                        (col("time_gap_seconds") > max_gap_for_jump_check_seconds)
                        | (col("implied_speed_knots") <= max_implied_speed_knots)
                    )
                )
            )
            .withColumn("time_bin", floor(unix_timestamp(col("timestamp")) / lit(time_bin_seconds)))
            .withColumn("lat_bin", floor(col("latitude") / lit(grid_size_degrees)))
            .withColumn("lon_bin", floor(col("longitude") / lit(grid_size_degrees)))
            .select(
                "date",
                "timestamp",
                "time_bin",
                "mmsi",
                "vessel_name",
                "latitude",
                "longitude",
                "sog",
                "lat_bin",
                "lon_bin",
            )
        )

        bucket_bounds = day_df_base.agg(
        spark_min("time_bin").alias("min_bucket"),
        spark_max("time_bin").alias("max_bucket")
        ).collect()[0]

        min_bucket = bucket_bounds["min_bucket"]
        max_bucket = bucket_bounds["max_bucket"]

        if min_bucket is None or max_bucket is None:
            log(f"Date {current_date}: no AIS rows after filtering")
            continue

        for chunk_start in range(int(min_bucket), int(max_bucket) + 1, time_bin_chunk_size):
            chunk_end = chunk_start + time_bin_chunk_size - 1

            chunk_start_time = datetime.fromtimestamp(
                chunk_start * time_bin_seconds,
                tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S UTC")

            chunk_end_time = datetime.fromtimestamp(
                ((chunk_end + 1) * time_bin_seconds) - 1,
                tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S UTC")

            log(
                f"Processing date {current_date}, "
                f"time {chunk_start_time} to {chunk_end_time} "
                f"(bins {chunk_start}-{chunk_end})"
            )

            # Expand the spatial bins around each point so we can compare nearby
            # vessel positions without computing a full cartesian product.
            chunk_df = (
                day_df_base
                .filter(
                    (col("time_bin") >= lit(chunk_start - 1)) &
                    (col("time_bin") <= lit(chunk_end + 1))
                )
                .repartition(32, "time_bin", "lat_bin", "lon_bin")
            )

            a_df = chunk_df.filter(
                (col("time_bin") >= lit(chunk_start)) &
                (col("time_bin") <= lit(chunk_end))
            )

            b_expanded = (
                chunk_df
                .withColumn("match_lat_bin", explode(sequence(col("lat_bin") - 1, col("lat_bin") + 1)))
                .withColumn("match_lon_bin", explode(sequence(col("lon_bin") - 1, col("lon_bin") + 1)))
            )

            a = a_df.alias("a")
            b = b_expanded.alias("b")

            joined = a.join(
                b,
                (
                    (spark_abs(col("a.time_bin") - col("b.time_bin")) <= lit(1))
                    & (col("a.lat_bin") == col("b.match_lat_bin"))
                    & (col("a.lon_bin") == col("b.match_lon_bin"))
                    & (col("a.mmsi") < col("b.mmsi"))
                ),
                "inner",
            )

            chunk_candidates = (
                joined
                .withColumn(
                    "time_diff_seconds",
                    spark_abs(unix_timestamp(col("a.timestamp")) - unix_timestamp(col("b.timestamp")))
                )
                .filter(col("time_diff_seconds") <= max_time_difference_seconds)
                .filter(spark_abs(col("a.latitude") - col("b.latitude")) <= lit(0.002))
                .filter(spark_abs(col("a.longitude") - col("b.longitude")) <= lit(0.004))
                .withColumn(
                    "distance_m",
                    haversine_m(
                        col("a.latitude"),
                        col("a.longitude"),
                        col("b.latitude"),
                        col("b.longitude"),
                    )
                )
                .filter(col("distance_m") <= collision_distance_threshold_m)
                .withColumn(
                    "collision_timestamp",
                    from_unixtime(
                        (
                            unix_timestamp(col("a.timestamp"))
                            + unix_timestamp(col("b.timestamp"))
                        ) / 2
                    ).cast("timestamp")
                )
                .withColumn(
                    "collision_latitude",
                    (col("a.latitude") + col("b.latitude")) / 2
                )
                .withColumn(
                    "collision_longitude",
                    (col("a.longitude") + col("b.longitude")) / 2
                )
                .select(
                    col("a.date").alias("date"),
                    col("a.mmsi").alias("mmsi_1"),
                    col("a.vessel_name").alias("vessel_name_1"),
                    col("a.timestamp").alias("timestamp_1"),
                    col("a.latitude").alias("latitude_1"),
                    col("a.longitude").alias("longitude_1"),
                    col("a.sog").alias("sog_1"),

                    col("b.mmsi").alias("mmsi_2"),
                    col("b.vessel_name").alias("vessel_name_2"),
                    col("b.timestamp").alias("timestamp_2"),
                    col("b.latitude").alias("latitude_2"),
                    col("b.longitude").alias("longitude_2"),
                    col("b.sog").alias("sog_2"),

                    "time_diff_seconds",
                    "distance_m",
                    "collision_timestamp",
                    "collision_latitude",
                    "collision_longitude",
                )
            )

            (
                chunk_candidates
                .write
                .mode("append")
                .parquet(daily_candidates_path)
            )

        log(f"Date {current_date}: candidate chunk writes completed")

    log("Candidate generation completed.")
    log(f"All raw candidates saved to: {daily_candidates_path}")
    log("Final validated ranking will be performed by rank_collision_candidates.py")

    spark.stop()