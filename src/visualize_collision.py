import os
import shutil
from datetime import datetime, timedelta

import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Visualization of the final collision candidate.
# Reads the validated collision result and plots both vessel tracks around the
# collision time to help verify the event.

def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)

def add_direction_arrows(ax, lons, lats, step=3):
    """
    Add small arrows along the trajectory to show movement direction.
    """
    if len(lons) < 2:
        return

    for i in range(0, len(lons) - 1, step):
        ax.annotate(
            "",
            xy=(lons[i + 1], lats[i + 1]),
            xytext=(lons[i], lats[i]),
            arrowprops=dict(arrowstyle="->", lw=1.2)
        )

def main():
    # Entry point for collision plot generation.
    # Reads the verified collision result and plots AIS trajectories for both vessels
    # around the collision timestamp.
    log("Starting collision visualization...")

    spark = (
        SparkSession.builder
        .appName("AIS Collision Visualization")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    preprocessed_path = "/app/output/preprocessed_ais_december"
    collision_result_path = "/app/output/collision_result"
    output_png = "/app/output/collision_trajectory.png"
    output_csv_dir = "/app/output/collision_trajectory_data"

    if not os.path.exists(preprocessed_path):
        raise FileNotFoundError(f"Preprocessed AIS data not found: {preprocessed_path}")

    if not os.path.exists(collision_result_path):
        raise FileNotFoundError(f"Collision result not found: {collision_result_path}")

    log("Reading collision result...")
    result_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(collision_result_path)
        .withColumn("collision_timestamp", col("collision_timestamp").cast("timestamp"))
    )

    result_row = result_df.first()

    if result_row is None:
        raise ValueError("Collision result file is empty.")

    mmsi_1 = str(result_row["mmsi_1"])
    mmsi_2 = str(result_row["mmsi_2"])

    vessel_name_1 = result_row["vessel_name_1"] or f"MMSI {mmsi_1}"
    vessel_name_2 = result_row["vessel_name_2"] or f"MMSI {mmsi_2}"

    collision_ts = result_row["collision_timestamp"]
    collision_lat = float(result_row["collision_latitude"])
    collision_lon = float(result_row["collision_longitude"])

    if collision_ts is None:
        raise ValueError("collision_timestamp is missing in collision result.")

    window_start = collision_ts - timedelta(minutes=10)
    window_end = collision_ts + timedelta(minutes=10)

    log(
        f"Visualizing vessels {mmsi_1} and {mmsi_2} "
        f"from {window_start} to {window_end}"
    )

    log("Reading preprocessed AIS trajectories...")
    traj_df = (
        spark.read.parquet(preprocessed_path)
        .filter(col("mmsi").cast("string").isin([mmsi_1, mmsi_2]))
        .filter((col("timestamp") >= lit(window_start)) & (col("timestamp") <= lit(window_end)))
        .select("timestamp", "mmsi", "vessel_name", "latitude", "longitude", "sog")
        .orderBy("mmsi", "timestamp")
    )

    if os.path.exists(output_csv_dir):
        shutil.rmtree(output_csv_dir)

    traj_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_csv_dir)

    rows = traj_df.collect()

    if not rows:
        raise ValueError("No trajectory data found in the +/-10 minute window.")

    vessel_points = {
        mmsi_1: {"name": vessel_name_1, "lat": [], "lon": [], "ts": []},
        mmsi_2: {"name": vessel_name_2, "lat": [], "lon": [], "ts": []},
    }

    for row in rows:
        row_mmsi = str(row["mmsi"])
        if row_mmsi not in vessel_points:
            continue

        vessel_points[row_mmsi]["lat"].append(float(row["latitude"]))
        vessel_points[row_mmsi]["lon"].append(float(row["longitude"]))
        vessel_points[row_mmsi]["ts"].append(row["timestamp"])

    fig, ax = plt.subplots(figsize=(11, 8))

    line_styles = {
        mmsi_1: {"marker": "o"},
        mmsi_2: {"marker": "o"},
    }

    for mmsi, vessel in vessel_points.items():
        if not vessel["lat"]:
            continue

        lons = vessel["lon"]
        lats = vessel["lat"]
        ts = vessel["ts"]

        ax.plot(
            lons,
            lats,
            linewidth=2.5,
            marker=line_styles[mmsi]["marker"],
            markersize=4,
            label=f"{vessel['name']} ({mmsi})"
        )

        # Start point
        ax.scatter(lons[0], lats[0], marker="s", s=90, label=f"Start {mmsi}")
        ax.annotate(
            f"Start\n{ts[0].strftime('%H:%M:%S')}",
            (lons[0], lats[0]),
            xytext=(6, 6),
            textcoords="offset points",
            fontsize=8
        )

        # End point
        ax.scatter(lons[-1], lats[-1], marker="^", s=90, label=f"End {mmsi}")
        ax.annotate(
            f"End\n{ts[-1].strftime('%H:%M:%S')}",
            (lons[-1], lats[-1]),
            xytext=(6, -14),
            textcoords="offset points",
            fontsize=8
        )

        # Direction arrows
        add_direction_arrows(ax, lons, lats, step=max(1, len(lons) // 6))

        # Add a few time labels along the route
        if len(ts) > 4:
            sample_indices = [0, len(ts)//3, 2*len(ts)//3, len(ts)-1]
            for idx in sample_indices:
                ax.annotate(
                    ts[idx].strftime("%H:%M:%S"),
                    (lons[idx], lats[idx]),
                    xytext=(4, 4),
                    textcoords="offset points",
                    fontsize=7
                )

    # Collision point
    ax.scatter(
        collision_lon,
        collision_lat,
        marker="x",
        s=180,
        linewidths=3,
        color="red",
        label="Collision point"
    )
    ax.annotate(
        f"Collision\n{collision_ts.strftime('%H:%M:%S UTC')}",
        (collision_lon, collision_lat),
        xytext=(8, 8),
        textcoords="offset points",
        fontsize=9,
        fontweight="bold"
    )

    # Make bounds a little nicer
    all_lons = []
    all_lats = []
    for vessel in vessel_points.values():
        all_lons.extend(vessel["lon"])
        all_lats.extend(vessel["lat"])

    if all_lons and all_lats:
        lon_margin = max((max(all_lons) - min(all_lons)) * 0.15, 0.01)
        lat_margin = max((max(all_lats) - min(all_lats)) * 0.15, 0.01)

        ax.set_xlim(min(all_lons) - lon_margin, max(all_lons) + lon_margin)
        ax.set_ylim(min(all_lats) - lat_margin, max(all_lats) + lat_margin)

    ax.set_title(
        "Vessel trajectories 10 minutes before and after collision\n"
        f"Collision time: {collision_ts.strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.legend(fontsize=8)
    plt.tight_layout()

    plt.savefig(output_png, dpi=250)
    plt.close()

    log(f"Visualization saved to: {output_png}")
    log(f"Trajectory data saved to: {output_csv_dir}")

    spark.stop()