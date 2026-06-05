# AIS Collision Detection – Docker Submission

This repository contains a Dockerized Python/PySpark solution for detecting a vessel collision event from Danish AIS data.

## Project Structure

The project includes the required Docker configuration files:

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

The raw AIS CSV files should be placed inside:

```text
data/aisdk-2021-12/
```

The generated results are written to:

```text
output/
```

## Docker Hub Image

The compiled Docker image for this project is available on Docker Hub:

```text
https://hub.docker.com/r/justas321/ais-collision-detection
```

This Docker Hub link is provided as the compiled container image submission.

To pull the image, run:

```bash
docker pull justas321/ais-collision-detection:latest
```

## Running the Project

From the root folder of the repository, run:

```bash
docker compose up
```

If using the older Docker Compose command, run:

```bash
docker-compose up
```

To rebuild the image locally and run the project, use:

```bash
docker compose up --build
```

## Expected Output

After the container finishes running, the results will be saved in the local `output/` folder.

The main final result file is:

```text
output/collision_result_readable.csv
```

This file contains:

* MMSI number of vessel 1
* name of vessel 1
* MMSI number of vessel 2
* name of vessel 2
* collision timestamp
* collision latitude
* collision longitude

The trajectory visualization is saved as:

```text
output/collision_trajectory.png
```
