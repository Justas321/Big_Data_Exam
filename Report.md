# Big Data Exam – AIS Collision Detection

## Project Overview

This project identifies a likely vessel collision or closest physical proximity event using Danish AIS data from December 2021. The solution processes large-scale temporal and spatial vessel data with PySpark and is fully containerized using Docker.

The analysis focuses on vessels operating within a 50-nautical-mile radius of the center coordinate:

- Latitude: `55.225000`
- Longitude: `14.245000`

The final output identifies the two vessels involved in the detected collision event and reports their MMSI numbers, vessel names, collision timestamp, and collision coordinates.

## Technical Stack

- Language: Python 3.10
- Big data framework: PySpark
- Visualization: Matplotlib
- Containerization: Docker and Docker Compose

## Repository Structure

```text
.
├── data/
│   └── aisdk-2021-12/
├── output/
├── src/
│   ├── main.py
│   ├── preprocess_ais.py
│   ├── detect_collision.py
│   ├── rank_collision_candidates.py
│   ├── visualize_collision.py
│   └── export_readable_result.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## How to Build and Run

Build and execute the Docker container with:

```bash
docker compose up --build
```

The pipeline is controlled from `src/main.py`. A full run should execute the following stages:

```python
preprocess_main()
detect_main()
rank_main()
visualize_main()
export_result_main()
```

## Output Files

After the container finishes, the main outputs are saved in the local `output/` folder:

```text
output/collision_result/
output/collision_result_readable.csv
output/collision_trajectory.png
output/collision_trajectory_data/
output/collision_candidates/
```

The most important final result file is:

```text
output/collision_result_readable.csv
```

This CSV contains the required final collision details:

- MMSI number of vessel 1
- name of vessel 1
- MMSI number of vessel 2
- name of vessel 2
- exact collision timestamp
- collision latitude
- collision longitude

The visualization output is:

```text
output/collision_trajectory.png
```

This plot shows the trajectories of both identified vessels from 10 minutes before to 10 minutes after the detected collision time.

## Methodology

### 1. Data Loading and Preprocessing

The raw AIS CSV files are loaded using PySpark. The preprocessing stage standardizes the important AIS fields, including timestamp, MMSI, vessel name, latitude, longitude, and speed over ground.

The data is restricted to the required time period:

```text
2021-12-01 00:00:00 to 2021-12-31 23:59:59
```

Rows with missing timestamps, missing MMSI values, invalid coordinates, or unrealistic speed values are removed. The preprocessing step also filters the data geographically, keeping only vessels within 50 nautical miles of the required center coordinate at latitude `55.225000` and longitude `14.245000`.

The cleaned dataset is saved as Parquet because this format is more efficient for repeated Spark processing than raw CSV.

### 2. Defining and Excluding Data Noise

AIS data can contain duplicated messages, missing values, asynchronous timestamps, and GPS anomalies. Several rules are used to reduce the effect of noise.

Invalid coordinate values are removed by requiring latitude to be between `-90` and `90`, and longitude to be between `-180` and `180`. Stationary or nearly stationary vessels are reduced by filtering out very low speed records. Extremely high speeds are also removed because they are not realistic for normal vessel movement.

The collision detection step also checks for GPS jumps. For each vessel, consecutive AIS points are compared. If the implied speed between two consecutive positions is unrealistically high, the point is treated as a likely GPS anomaly and excluded. This prevents sudden false position jumps from being identified as collision events.

Duplicate records are removed using vessel identifier, timestamp, latitude, and longitude. This reduces repeated AIS messages that could otherwise create artificial close encounters.

### 3. Collision Candidate Detection

The detection stage searches for vessel pairs that are close in both time and space. To avoid an inefficient full Cartesian comparison of all vessel points, the data is divided into time bins and spatial grid cells.

Each AIS point is assigned:

- a time bin
- a latitude bin
- a longitude bin

Only vessels in nearby time bins and neighboring spatial cells are compared. This keeps the number of pairwise distance calculations much lower while still allowing close vessels to be detected.

For candidate pairs, the Haversine formula is used to calculate the distance between vessel coordinates. Candidate events are kept only when the vessels are within the collision distance threshold and their AIS timestamps are close enough in time.

### 4. Candidate Ranking and Collision Verification

The first detection stage may still produce false positives, such as vessels moving near each other in a harbor, service vessels operating close to other ships, or vessels following parallel routes. Therefore, a second ranking stage validates the candidates.

For each candidate, the surrounding AIS history is examined in a time window around the possible collision. The ranking stage checks whether both vessels have enough movement and enough AIS points to support a real event. It also calculates proximity duration, density of close events, movement balance, and speed behavior before and after the possible collision.

Candidates are penalized when they appear to be long-duration close-following events, repeated dense harbor proximity events, duplicate AIS records, rescue or pilot/service vessel activity, or cases where both vessels continue normally after the event. The best candidate is selected using a scoring method where lower scores indicate a more likely collision.

This approach verifies the final collision by checking not only the closest point, but also the vessel behavior around the event.

### 5. Visualization

After the best collision candidate is selected, the visualization script extracts AIS points for both vessels from exactly 10 minutes before to 10 minutes after the collision timestamp.

The trajectory plot includes:

- the path of each vessel
- start and end points
- direction arrows
- time labels along the tracks
- the detected collision point

The plot is saved automatically as `output/collision_trajectory.png`.

## Computational Strategy

The main computational challenge is avoiding unnecessary distance calculations between all possible vessel pairs. A full Cartesian product would be inefficient and unsuitable for large AIS data.

The solution improves efficiency by using:

- Parquet storage after preprocessing
- date-by-date processing
- time-bin chunking
- spatial grid binning
- neighboring-cell comparison only
- filtering before expensive distance calculations
- Spark partitioning and repartitioning

This strategy reduces memory usage and keeps the number of Haversine distance calculations manageable.

## Findings

The pipeline produces one final validated collision result. The final result is exported to a readable CSV file containing the MMSI numbers, vessel names, timestamp, and coordinates of the detected event.

The generated trajectory visualization is used to confirm that the two selected vessels were close in space and time and to inspect their movement patterns around the event. The ranking logic further supports the result by filtering out stationary vessels, unrealistic GPS jumps, duplicate records, and long-duration non-collision proximity cases.
