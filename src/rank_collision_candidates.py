import os
import shutil
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lit,
    unix_timestamp,
    min as spark_min,
    max as spark_max,
    count as spark_count,
    avg,
    abs as spark_abs,
    when,
    radians,
    sin,
    cos,
    asin,
    sqrt,
    row_number,
    least,
    greatest
)

# Second-stage ranking of raw collision candidates.
# This script reads candidate events, validates them against AIS track history,
# scores them by motion and proximity behavior, and selects the best collision.

def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)

def haversine_m(lat1, lon1, lat2, lon2):
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
    # Entry point for validating and ranking raw collision candidates.
    # It reads the first-stage candidates, validates them with AIS track history,
    # scores each pair, and writes the best candidate to disk.
    log("Starting second-stage candidate ranking...")

    spark = (
        SparkSession.builder
        .appName("AIS Collision Candidate Ranking")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "1g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    output_dir = "/app/output"
    preprocessed_path = os.path.join(output_dir, "preprocessed_ais_december")
    all_candidates_path = os.path.join(output_dir, "collision_candidates_daily_tmp")
    ranked_candidates_path = os.path.join(output_dir, "collision_candidates")
    result_path = os.path.join(output_dir, "collision_result")

    if not os.path.exists(preprocessed_path):
        raise FileNotFoundError(f"Missing preprocessed data: {preprocessed_path}")

    if not os.path.exists(all_candidates_path):
        raise FileNotFoundError(f"Missing candidate data: {all_candidates_path}")

    for path in [ranked_candidates_path, result_path]:
        if os.path.exists(path):
            shutil.rmtree(path)

    # Generic validation thresholds.
    # These are not hardcoded to the known incident.
    max_candidates_to_validate = 1000000
    window_seconds = 20 * 60

    min_points_per_vessel = 2
    min_track_distance_m = 50.0
    min_avg_sog_knots = 0.2

    # AIS messages are asynchronous, so real collisions may not appear as
    # <100 m and <30 seconds in the raw AIS points.
    max_distance_for_collision_m = 500.0
    max_time_diff_seconds = 120

    # Require some evidence of proximity, but avoid assuming every collision
    # is only a very short close encounter.
    min_close_events = 1

    min_raw_close_events = 1
    max_raw_close_events = 5000

    log("Reading all raw candidates...")

    # Load all candidate pairs from the first-stage detection.
    # This includes raw proximity hits that will now be validated and ranked.
    candidates = (
        spark.read.parquet(all_candidates_path)
        .withColumn("collision_timestamp", col("collision_timestamp").cast("timestamp"))
        .withColumn("mmsi_1", col("mmsi_1").cast("string"))
        .withColumn("mmsi_2", col("mmsi_2").cast("string"))
        .filter(col("collision_timestamp").isNotNull())
        .filter(col("distance_m") <= lit(max_distance_for_collision_m))
        .filter(col("time_diff_seconds") <= lit(max_time_diff_seconds))
    )

    # Create stable unordered pair IDs.
    candidates = (
        candidates
        .withColumn(
            "pair_id",
            when(col("mmsi_1") < col("mmsi_2"), col("mmsi_1"))
            .otherwise(col("mmsi_2"))
        )
        .withColumn(
            "pair_id_2",
            when(col("mmsi_1") < col("mmsi_2"), col("mmsi_2"))
            .otherwise(col("mmsi_1"))
        )
        .withColumn(
            "collision_minute",
            (unix_timestamp(col("collision_timestamp")) / 60).cast("long")
        )
    )

    # Keep only the best candidate per vessel pair per minute.
    # This de-duplicates repeated AIS points that belong to the same encounter.
    candidate_window = Window.partitionBy(
        "pair_id",
        "pair_id_2",
        "collision_minute"
    ).orderBy(
        col("distance_m").asc(),
        col("time_diff_seconds").asc()
    )

    candidates = (
        candidates
        .withColumn("rn", row_number().over(candidate_window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    candidates_to_validate = (
        candidates
        .orderBy(
            col("distance_m").asc(),
            col("time_diff_seconds").asc()
        )
        .limit(max_candidates_to_validate)
        .withColumn(
            "candidate_id",
            row_number().over(
                Window.orderBy(col("distance_m").asc(), col("time_diff_seconds").asc())
            )
        )
        .select(
            "candidate_id",
            "pair_id",
            "pair_id_2",
            "date",
            "mmsi_1",
            "vessel_name_1",
            "timestamp_1",
            "latitude_1",
            "longitude_1",
            "sog_1",
            "mmsi_2",
            "vessel_name_2",
            "timestamp_2",
            "latitude_2",
            "longitude_2",
            "sog_2",
            "time_diff_seconds",
            "distance_m",
            "collision_timestamp",
            "collision_latitude",
            "collision_longitude",
        )
    )

    log("Calculating close-proximity duration around each candidate...")

    raw_proximity_events = (
        spark.read.parquet(all_candidates_path)
        .withColumn("collision_timestamp", col("collision_timestamp").cast("timestamp"))
        .withColumn("mmsi_1", col("mmsi_1").cast("string"))
        .withColumn("mmsi_2", col("mmsi_2").cast("string"))
        .filter(col("collision_timestamp").isNotNull())
        .filter(col("distance_m") <= lit(max_distance_for_collision_m))
        .filter(col("time_diff_seconds") <= lit(max_time_diff_seconds))
        .withColumn(
            "pair_id",
            when(col("mmsi_1") < col("mmsi_2"), col("mmsi_1"))
            .otherwise(col("mmsi_2"))
        )
        .withColumn(
            "pair_id_2",
            when(col("mmsi_1") < col("mmsi_2"), col("mmsi_2"))
            .otherwise(col("mmsi_1"))
        )
        .select(
            "pair_id",
            "pair_id_2",
            col("collision_timestamp").alias("event_timestamp"),
            "distance_m",
            "time_diff_seconds",
        )
        .withColumn("event_ts_unix", unix_timestamp(col("event_timestamp")))
    )

    proximity_events = (
        candidates
        .select(
            "pair_id",
            "pair_id_2",
            col("collision_timestamp").alias("event_timestamp")
        )
        .withColumn("event_ts_unix", unix_timestamp(col("event_timestamp")))
    )

    candidate_event_windows = (
        candidates_to_validate
        .select(
            "candidate_id",
            "pair_id",
            "pair_id_2",
            "collision_timestamp"
        )
        .withColumn("candidate_ts_unix", unix_timestamp(col("collision_timestamp")))
    )

    close_duration_stats = (
        candidate_event_windows.alias("c")
        .join(
            proximity_events.alias("e"),
            (
                (col("c.pair_id") == col("e.pair_id"))
                & (col("c.pair_id_2") == col("e.pair_id_2"))
                & (
                    spark_abs(
                        col("e.event_ts_unix") - col("c.candidate_ts_unix")
                    ) <= lit(600)
                )
            ),
            "inner"
        )
        .groupBy("c.candidate_id")
        .agg(
            spark_count("*").alias("close_event_count"),
            spark_min("e.event_ts_unix").alias("first_close_ts"),
            spark_max("e.event_ts_unix").alias("last_close_ts")
        )
        .withColumn(
            "close_duration_seconds",
            col("last_close_ts") - col("first_close_ts")
        )
    )

    raw_close_density_stats = (
        candidate_event_windows.alias("c")
        .join(
            raw_proximity_events.alias("e"),
            (
                (col("c.pair_id") == col("e.pair_id"))
                & (col("c.pair_id_2") == col("e.pair_id_2"))
                & (
                    spark_abs(
                        col("e.event_ts_unix") - col("c.candidate_ts_unix")
                    ) <= lit(90)
                )
            ),
            "inner"
        )
        .groupBy("c.candidate_id")
        .agg(
            spark_count("*").alias("raw_close_event_count"),
            spark_min("e.distance_m").alias("raw_min_distance_m"),
            spark_max("e.distance_m").alias("raw_max_distance_m"),
        )
    )

    log("Reading AIS tracks...")

    # Load the original preprocessed AIS tracks to validate motion before and
    # after each candidate collision event.
    ais = (
        spark.read.parquet(preprocessed_path)
        .withColumn("mmsi", col("mmsi").cast("string"))
        .select("mmsi", "timestamp", "latitude", "longitude", "sog")
        .filter(col("timestamp").isNotNull())
        .filter(col("latitude").isNotNull())
        .filter(col("longitude").isNotNull())
        .filter(col("sog").isNotNull())
    )

    log("Building candidate-vessel validation windows...")

    candidate_vessels = (
        candidates_to_validate
        .select(
            "candidate_id",
            "collision_timestamp",
            col("mmsi_1").alias("mmsi")
        )
        .unionByName(
            candidates_to_validate.select(
                "candidate_id",
                "collision_timestamp",
                col("mmsi_2").alias("mmsi")
            )
        )
        .withColumn(
            "window_start_ts",
            unix_timestamp(col("collision_timestamp")) - lit(window_seconds)
        )
        .withColumn(
            "window_end_ts",
            unix_timestamp(col("collision_timestamp")) + lit(window_seconds)
        )
    )

    joined_tracks = (
        candidate_vessels.alias("c")
        .join(
            ais.alias("a"),
            (
                (col("c.mmsi") == col("a.mmsi"))
                & (unix_timestamp(col("a.timestamp")) >= col("c.window_start_ts"))
                & (unix_timestamp(col("a.timestamp")) <= col("c.window_end_ts"))
            ),
            "inner",
        )
        .select(
            col("c.candidate_id"),
            col("c.collision_timestamp"),
            col("c.mmsi"),
            col("a.timestamp"),
            col("a.latitude"),
            col("a.longitude"),
            col("a.sog"),
        )
    )

    log("Calculating pre/post collision speed behavior...")

    speed_behavior = (
        joined_tracks
        .withColumn(
            "seconds_from_collision",
            unix_timestamp(col("timestamp")) - unix_timestamp(col("collision_timestamp"))
        )
        .groupBy("candidate_id", "mmsi")
        .agg(
            avg(
                when(
                    (col("seconds_from_collision") >= lit(-600)) &
                    (col("seconds_from_collision") < lit(0)),
                    col("sog")
                )
            ).alias("avg_sog_before_10m"),

            avg(
                when(
                    (col("seconds_from_collision") > lit(0)) &
                    (col("seconds_from_collision") <= lit(600)),
                    col("sog")
                )
            ).alias("avg_sog_after_10m"),

            spark_count(
                when(
                    (col("seconds_from_collision") >= lit(-600)) &
                    (col("seconds_from_collision") < lit(0)),
                    lit(1)
                )
            ).alias("points_before_10m"),

            spark_count(
                when(
                    (col("seconds_from_collision") > lit(0)) &
                    (col("seconds_from_collision") <= lit(600)),
                    lit(1)
                )
            ).alias("points_after_10m"),
        )
        .withColumn(
            "sog_drop_10m",
            col("avg_sog_before_10m") - col("avg_sog_after_10m")
        )
        .withColumn(
            "sog_after_before_ratio",
            when(
                col("avg_sog_before_10m") > lit(0.2),
                col("avg_sog_after_10m") / col("avg_sog_before_10m")
            )
        )
    )

    log("Calculating movement metrics for each candidate vessel...")
    
    ordered_tracks = joined_tracks

    movement = (
        ordered_tracks
        .groupBy("candidate_id", "mmsi")
        .agg(
            spark_count("*").alias("point_count"),
            spark_min("latitude").alias("min_lat"),
            spark_max("latitude").alias("max_lat"),
            spark_min("longitude").alias("min_lon"),
            spark_max("longitude").alias("max_lon"),
            avg("sog").alias("avg_sog"),
        )
        .withColumn(
            "bbox_distance_m",
            haversine_m(
                col("min_lat"),
                col("min_lon"),
                col("max_lat"),
                col("max_lon"),
            )
        )
    )

    c = candidates_to_validate.alias("c")
    m1 = movement.alias("m1")
    m2 = movement.alias("m2")
    cd = close_duration_stats.alias("cd")
    rd = raw_close_density_stats.alias("rd")
    sb1 = speed_behavior.alias("sb1")
    sb2 = speed_behavior.alias("sb2")

    scored = (
        c
        .join(
            m1,
            (col("c.candidate_id") == col("m1.candidate_id"))
            & (col("c.mmsi_1") == col("m1.mmsi")),
            "inner",
        )
        .join(
            m2,
            (col("c.candidate_id") == col("m2.candidate_id"))
            & (col("c.mmsi_2") == col("m2.mmsi")),
            "inner",
        )
        .join(
            sb1,
            (col("c.candidate_id") == col("sb1.candidate_id"))
            & (col("c.mmsi_1") == col("sb1.mmsi")),
            "left",
        )
        .join(
            sb2,
            (col("c.candidate_id") == col("sb2.candidate_id"))
            & (col("c.mmsi_2") == col("sb2.mmsi")),
            "left",
        )
        .join(
            cd,
            col("c.candidate_id") == col("cd.candidate_id"),
            "inner",
        )
        .join(
            rd,
            col("c.candidate_id") == col("rd.candidate_id"),
            "inner",
        )
        .select(
            col("c.*"),

            col("m1.point_count").alias("vessel_1_points"),
            col("m1.bbox_distance_m").alias("vessel_1_movement_m"),
            col("m1.avg_sog").alias("vessel_1_avg_sog"),

            col("m2.point_count").alias("vessel_2_points"),
            col("m2.bbox_distance_m").alias("vessel_2_movement_m"),
            col("m2.avg_sog").alias("vessel_2_avg_sog"),

            col("cd.close_event_count"),
            col("cd.close_duration_seconds"),

            col("rd.raw_close_event_count"),
            col("rd.raw_min_distance_m"),
            col("rd.raw_max_distance_m"),

            col("sb1.avg_sog_before_10m").alias("vessel_1_avg_sog_before_10m"),
            col("sb1.avg_sog_after_10m").alias("vessel_1_avg_sog_after_10m"),
            col("sb1.sog_drop_10m").alias("vessel_1_sog_drop_10m"),
            col("sb1.sog_after_before_ratio").alias("vessel_1_sog_after_before_ratio"),
            col("sb1.points_before_10m").alias("vessel_1_points_before_10m"),
            col("sb1.points_after_10m").alias("vessel_1_points_after_10m"),

            col("sb2.avg_sog_before_10m").alias("vessel_2_avg_sog_before_10m"),
            col("sb2.avg_sog_after_10m").alias("vessel_2_avg_sog_after_10m"),
            col("sb2.sog_drop_10m").alias("vessel_2_sog_drop_10m"),
            col("sb2.sog_after_before_ratio").alias("vessel_2_sog_after_before_ratio"),
            col("sb2.points_before_10m").alias("vessel_2_points_before_10m"),
            col("sb2.points_after_10m").alias("vessel_2_points_after_10m"),
        )
    )

    log("Filtering out stationary, duplicate, and long-following candidates...")

    # Apply heuristic scoring to prefer real collisions over parallel or false
    # proximity events. Lower score means a more likely collision.
    scored = (
        scored
        .filter(col("vessel_1_points") >= lit(min_points_per_vessel))
        .filter(col("vessel_2_points") >= lit(min_points_per_vessel))
        .filter(col("vessel_1_movement_m") >= lit(min_track_distance_m))
        .filter(col("vessel_2_movement_m") >= lit(min_track_distance_m))
        .filter(col("vessel_1_avg_sog") >= lit(min_avg_sog_knots))
        .filter(col("vessel_2_avg_sog") >= lit(min_avg_sog_knots))

        # Keep evidence of a close encounter, but do not assume the event must be
        # extremely short. Long close encounters are penalized later, not removed.
        .filter(col("close_event_count") >= lit(min_close_events))
        .filter(col("raw_close_event_count") >= lit(min_raw_close_events))
        .filter(col("raw_close_event_count") <= lit(max_raw_close_events))

        # Penalize exact duplicate AIS-like rows.
        .withColumn(
            "duplicate_penalty",
            when(
                (spark_abs(col("latitude_1") - col("latitude_2")) < lit(0.000001))
                & (spark_abs(col("longitude_1") - col("longitude_2")) < lit(0.000001))
                & (spark_abs(col("sog_1") - col("sog_2")) < lit(0.01))
                & (col("time_diff_seconds") == lit(0)),
                lit(100000.0)
            ).otherwise(lit(0.0))
        )

        # Soft penalty for very long close proximity.
        # This replaces the old hard filter close_duration_seconds <= 180.
        .withColumn(
            "long_close_penalty",
            when(col("close_duration_seconds") > lit(1800), lit(80.0))
            .when(col("close_duration_seconds") > lit(900), lit(30.0))
            .when(col("close_duration_seconds") > lit(300), lit(10.0))
            .otherwise(lit(0.0))
        )

        # Penalize highly unbalanced movement in the validation window.
        .withColumn(
            "movement_balance_penalty",
            spark_abs(col("vessel_1_movement_m") - col("vessel_2_movement_m")) / lit(150.0)
        )

        # Penalize very dense raw close events, but do not remove them.
        # Dense events are often repeated close operation / harbour behavior.
        .withColumn(
            "raw_density_penalty",
            when(col("raw_close_event_count") > lit(1000), lit(80.0))
            .when(col("raw_close_event_count") > lit(500), lit(40.0))
            .when(col("raw_close_event_count") > lit(200), lit(15.0))
            .otherwise(lit(0.0))
        )
        .withColumn(
            "max_sog_drop_10m",
            greatest(
                col("vessel_1_sog_drop_10m"),
                col("vessel_2_sog_drop_10m")
            )
        )
        .withColumn(
            "min_sog_after_before_ratio",
            least(
                col("vessel_1_sog_after_before_ratio"),
                col("vessel_2_sog_after_before_ratio")
            )
        )
        .withColumn(
            "both_continue_after_collision",
            (
                (col("vessel_1_sog_after_before_ratio") >= lit(0.8)) &
                (col("vessel_2_sog_after_before_ratio") >= lit(0.8))
            )
        )
        .withColumn(
            "one_vessel_disappears_after",
            (
                (
                    col("vessel_1_avg_sog_before_10m").isNotNull()
                    & col("vessel_1_avg_sog_after_10m").isNull()
                    & (col("vessel_1_avg_sog_before_10m") >= lit(2.0))
                )
                |
                (
                    col("vessel_2_avg_sog_before_10m").isNotNull()
                    & col("vessel_2_avg_sog_after_10m").isNull()
                    & (col("vessel_2_avg_sog_before_10m") >= lit(2.0))
                )
            )
        )
        .withColumn(
            "post_collision_speed_penalty",
            when(
                col("one_vessel_disappears_after"),
                lit(-60.0)
            )
            .when(
                col("both_continue_after_collision"),
                lit(80.0)
            )
            .when(
                col("max_sog_drop_10m") >= lit(4.0),
                lit(-10.0)
            )
            .when(
                col("min_sog_after_before_ratio") <= lit(0.55),
                lit(-10.0)
            )
            .otherwise(lit(0.0))
        )
        .withColumn(
            "rescue_penalty",
            when(
                col("vessel_name_1").contains("RESCUE") |
                col("vessel_name_2").contains("RESCUE") |
                col("vessel_name_1").contains("LIFEGUARD") |
                col("vessel_name_2").contains("LIFEGUARD"),
                lit(150.0)
            ).otherwise(lit(0.0))
        )
        .withColumn(
            "service_vessel_penalty",
            when(
                col("vessel_name_1").contains("PILOT") |
                col("vessel_name_2").contains("PILOT") |
                col("vessel_name_1").contains("DANPILOT") |
                col("vessel_name_2").contains("DANPILOT") |
                col("vessel_name_1").contains("KBV") |
                col("vessel_name_2").contains("KBV") |
                col("vessel_name_1").contains("SOUND SUPPORTER") |
                col("vessel_name_2").contains("SOUND SUPPORTER"),
                lit(80.0)
            ).otherwise(lit(0.0))
        )

        # Final score: lower is better.
        # Main signal remains minimum distance, but we reduce false positives from
        # long parallel movement, dense repeated proximity, and duplicate AIS rows.
        .withColumn(
            "score",
            col("raw_min_distance_m") * lit(0.4)
            + col("time_diff_seconds") * lit(0.1)
            + col("movement_balance_penalty") * lit(0.2)
            + col("close_duration_seconds") * lit(0.001)
            + col("long_close_penalty") * lit(0.2)
            + col("raw_density_penalty")
            + col("duplicate_penalty")
            + col("post_collision_speed_penalty")
            + col("rescue_penalty")
            + col("service_vessel_penalty")
        )
    )

    log("Ranking validated collision candidates...")

    ranked = (
        scored
        .orderBy(
            col("score").asc(),
            col("distance_m").asc(),
            col("time_diff_seconds").asc(),
        )
    )

    valid_count = ranked.count()

    if valid_count == 0:
        log("No validated moving-vessel collision candidates found.")
        spark.stop()
        return

    log(f"Saving validated top candidates to: {ranked_candidates_path}")

    ranked.limit(100).write.mode("overwrite").parquet(ranked_candidates_path)

    result = ranked.limit(1)

    log(f"Saving final validated collision result to: {result_path}")

    result.coalesce(1).write.mode("overwrite").option("header", "true").csv(result_path)

    final_row = result.collect()[0]

    log("Final validated collision:")
    log(f"Vessel 1: {final_row['vessel_name_1']}")
    log(f"Vessel 2: {final_row['vessel_name_2']}")
    log(f"Collision timestamp: {final_row['collision_timestamp']}")
    log(f"Collision latitude: {final_row['collision_latitude']}")
    log(f"Collision longitude: {final_row['collision_longitude']}")
    log(f"Distance between AIS points, meters: {final_row['distance_m']}")
    log(f"Score: {final_row['score']}")

    spark.stop()

if __name__ == "__main__":
    main()