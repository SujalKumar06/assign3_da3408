# AI Operations (AIOps): Assignment 3

Spark vs. Ray: the same data preprocessing pipeline implemented in Apache Spark
and in Ray Data, run on a two-worker Docker cluster for each framework, and
benchmarked on the NYC TLC Yellow Taxi trip data (38 monthly files, 2022-01 to
2025-02, 126,188,813 rows, ~2 GB).

A video of the whole process is
[here](https://drive.google.com/file/d/1Xj7BCZRdBJQ_FbPGiC_ctcyST71PzNcC/view?usp=sharing).

Structure of the repository:

```
spark_clean.py       Spark (PySpark DataFrame) pipeline
ray_clean.py         Ray Data pipeline
verify_parity.py     checks that the Spark and Ray outputs are identical
pipeline/            schema, cleaning rules and the UDF shared by both pipelines
docker/              cluster image and the Spark / Ray compose files
benchmark/           full benchmark run and results summary
data/raw/            trip data (Git LFS) and the taxi zone lookup table
results/             run records, logs, resource monitoring and the summary table
evidence/            screenshots: cluster UIs, job UIs, top, parity check
benchmark.log        console output of the full benchmark run
```

## Environment

```bash
conda activate aiops
pip install -r requirements.txt
```

Docker with Compose v2, Java 21 (for running Spark outside Docker) and
`git-lfs` must be installed. Run every command from the repository root.

The trip data is stored with Git LFS:

```bash
git lfs install
git lfs pull       # 38 files in data/raw/trips/
```

The source is the
[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
(Yellow Taxi Trip Records and the Taxi Zone Lookup Table).

---

## Running end to end

The cluster files are sized for a 16-core, 32 GB machine (two workers with
6 CPUs and 8 GB each). On a smaller machine, lower `cpus`, `mem_limit`,
`SPARK_WORKER_CORES`/`SPARK_WORKER_MEMORY` and the Ray `--num-cpus` and
`--object-store-memory` values in `docker/` by the same amount for both
clusters, and the `local[12]` / `12g` of the parity check at the end of
`benchmark/run_benchmark.sh`.

1. Get the code and the data

   ```bash
   git lfs install
   git clone https://github.com/SujalKumar06/assign3_da3408 assignment_3
   cd assignment_3
   git lfs pull
   ls data/raw/trips | wc -l        # 38
   ```

2. Set up the environment

   ```bash
   conda create -n aiops python=3.13 -y
   conda activate aiops
   pip install -r requirements.txt
   ```

3. Optional quick check on the laptop (3 dev files, a few minutes)

   ```bash
   python spark_clean.py
   python ray_clean.py
   python verify_parity.py          # ends with "PARITY PASSED"
   ```

4. Build the cluster image

   ```bash
   docker compose -f docker/spark-compose.yml build
   ```

5. Run the full benchmark

   ```bash
   bash benchmark/run_benchmark.sh 2>&1 | tee benchmark.log
   ```

   This runs, in order: Spark cluster up, Spark `--stage-counts` run, Spark
   timed run, Spark cluster down, the same four steps on the Ray cluster, the
   full-data parity check and the summary. While a cluster is up, its UIs are
   at `localhost:9090` / `localhost:4040` (Spark) and `localhost:8265` (Ray).

6. Read the results

   ```bash
   cat results/summary.md
   sudo chown -R $USER:$USER data results     # files written by the containers are owned by root
   ```

---

## The pipeline

```
spark_clean.py       Spark implementation
ray_clean.py         Ray Data implementation
pipeline/config.py   canonical schema, column renames, cleaning thresholds, output schema
pipeline/udf.py      avg_speed_mph, the custom Python UDF used by both pipelines
```

Neither script hard-codes a rule; both read everything from
`pipeline/config.py` and call the same UDF, so the logic is identical.

| Step | What happens |
|---|---|
| Ingest | reads every parquet file and unifies the three raw schema versions (renames, casts, drops `cbd_congestion_fee`) |
| Cleanse | drops rows with nulls, applies the validity rules (pickup window, duration, distance, fare, passengers, rate code, location IDs), removes exact duplicates, formats timestamps (date, hour, ISO weekday, duration) |
| Transform | joins the zone lookup table twice (pickup and dropoff), computes `avg_speed_mph` with the Python UDF, keeps 0.5 to 80 mph |
| Export | writes parquet to `data/output/spark/` or `data/output/ray/` |

Options shared by both scripts:

```
--input                glob of trip files (default: 3 dev files, one per raw schema version)
--join                 shuffle (default) or broadcast
--shuffle-partitions   partitions for dedup and the shuffle join
--stage-counts         untimed run that prints the rows left after every stage
```

Run on the laptop (local mode, dev files):

```bash
python spark_clean.py
python ray_clean.py
python verify_parity.py
```

`verify_parity.py` reads both outputs and checks the schemas, the row counts,
`exceptAll` in both directions (must be 0 rows) and the per-stage row counts of
the latest `--stage-counts` runs.

---

## Clusters

```
docker/Dockerfile          one image for both clusters (Python 3.13, Java 21, requirements.txt)
docker/spark-compose.yml   Spark master + 2 workers
docker/ray-compose.yml     Ray head + 2 workers
```

Both clusters give each worker 6 CPUs and 8 GB of memory, and mount the
repository at `/app` on every node so all nodes share the code, data and output
path. Only one cluster runs at a time.

| Cluster | Containers | UI |
|---|---|---|
| Spark | `a3_spark_master`, `a3_spark_worker_a`, `a3_spark_worker_b` | master `localhost:9090`, workers `9091`/`9092`, jobs `localhost:4040` |
| Ray | `a3_ray_head`, `a3_ray_worker_a`, `a3_ray_worker_b` | dashboard `localhost:8265` |

```bash
docker compose -f docker/spark-compose.yml up -d --build
docker compose -f docker/spark-compose.yml down

docker compose -f docker/ray-compose.yml up -d
docker compose -f docker/ray-compose.yml down
```

The containers run as root, so files they write under `data/` and `results/`
are owned by root. Reclaim them with `sudo chown -R $USER:$USER data results`.

---

## Benchmark

```
benchmark/run_benchmark.sh   runs both pipelines on both clusters on all 38 files
benchmark/summarize.py       builds results/summary.md from the run records
```

```bash
bash benchmark/run_benchmark.sh
```

The script brings up the Spark cluster, runs a `--stage-counts` run and the
timed run, tears it down, then does the same on the Ray cluster. During every
run it samples `docker stats` and `top` once per second. It finishes with
`verify_parity.py` on the full outputs and `summarize.py`.

Outputs in `results/`:

```
runs/            one JSON per run (mode, wall time, output rows, per-stage counts and times)
logs/            full console log of every run
monitor/         per-second docker stats (CSV) and top snapshots for every run
summary.md       timing, per-stage, resource and parity tables
parity.json      full-data parity check result
*_explain_*.txt  Spark physical plan and Ray execution stats
```

Screenshots of the clusters and runs are in `evidence/`.

### Result (one run, all 38 files)

| | Spark | Ray |
|---|---:|---:|
| Wall time | 175.3 s | 345.0 s |
| Output rows | 111,016,594 | 111,016,594 |
| Parity | passed | passed |

Per-stage times, the UDF stage and peak CPU/memory are in `results/summary.md`.

---

## Implementation notes

- Spark runs with `spark.sql.constraintPropagation.enabled=false`; with it on,
  the optimizer ran out of driver memory while planning the chained filters.
- Ray uses the task-based `HASH_SHUFFLE_V2` strategy and 32 MB blocks
  (`--block-mb`); the default actor-based shuffle reserved more memory than the
  workers had and stalled.
- Ray applies the null drop and the validity filters in one `map_batches` step:
  a second map on a block that the first one emptied produces a block with no
  columns, which breaks Ray's hash shuffle.
- Before dedup both pipelines turn `-0.0` into `0.0` in the double columns, so
  Spark and Ray treat them as the same value.
