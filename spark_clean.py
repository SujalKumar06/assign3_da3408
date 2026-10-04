"""Spark implementation of the preprocessing pipeline."""

import argparse
import contextlib
import glob
import io
import json
import logging
import time
from datetime import datetime
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline import config
from pipeline.udf import avg_speed_mph

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("spark_clean")

SPARK_TYPES = {
    "int32": T.IntegerType(),
    "int64": T.LongType(),
    "double": T.DoubleType(),
    "string": T.StringType(),
    "timestamp": T.TimestampNTZType(),
    "date": T.DateType(),
}


def build_session(master: str, join: str, shuffle_partitions: int) -> SparkSession:
    builder = (
        SparkSession.builder.appName(f"a3-spark-clean-{join}")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", "2g")
        .config("spark.local.dir", str(config.ROOT / "data" / "tmp" / "spark"))
        .config("spark.sql.constraintPropagation.enabled", "false")
    )
    if join == "shuffle":
        builder = builder.config("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_trips(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Read each file on its own and unify it, since the raw schemas differ."""
    frames = []
    for path in paths:
        df = spark.read.parquet(path)
        df = df.drop(*[c for c in config.DROPPED_COLUMNS if c in df.columns])
        df = df.select([F.col(c).alias(config.RENAMES[c]) for c in df.columns])
        df = df.select([F.col(n).cast(SPARK_TYPES[t]).alias(n) for n, t in config.TRIP_SCHEMA])
        frames.append(df)
    return reduce(DataFrame.unionByName, frames)


def read_zones(spark: SparkSession) -> DataFrame:
    schema = T.StructType([T.StructField(c, T.StringType()) for c in config.ZONE_COLUMNS])
    zones = spark.read.csv(config.ZONES_CSV, header=True, schema=schema)
    return zones.withColumn("LocationID", F.col("LocationID").cast(T.IntegerType()))


def epoch_us(col: str):
    return F.unix_micros(F.col(col).cast(T.TimestampType()))


def drop_nulls(df: DataFrame) -> DataFrame:
    return df.na.drop(subset=config.DROP_NULLS_IN)


def validity_filters(df: DataFrame) -> DataFrame:
    ts = lambda s: F.lit(s).cast(T.TimestampNTZType())  # noqa: E731
    duration_us = F.col("dropoff_us") - F.col("pickup_us")
    lo_loc, hi_loc = config.LOCATION_ID_RANGE
    return (
        df.withColumn("pickup_us", epoch_us("pickup_datetime"))
        .withColumn("dropoff_us", epoch_us("dropoff_datetime"))
        .filter(F.col("pickup_datetime") >= ts(config.PICKUP_START))
        .filter(F.col("pickup_datetime") < ts(config.PICKUP_END))
        .filter(duration_us > 0)
        .filter(duration_us <= config.MAX_DURATION_SECONDS * 1_000_000)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("trip_distance") <= config.TRIP_DISTANCE_MAX)
        .filter(F.col("fare_amount") > config.FARE_AMOUNT_MIN_EXCLUSIVE)
        .filter(F.col("total_amount") > config.TOTAL_AMOUNT_MIN_EXCLUSIVE)
        .filter(F.col("passenger_count").between(*config.PASSENGER_COUNT_RANGE))
        .filter(F.col("ratecode_id").between(*config.RATECODE_ID_RANGE))
        .filter(F.col("pu_location_id").between(lo_loc, hi_loc))
        .filter(F.col("do_location_id").between(lo_loc, hi_loc))
    )


def normalize_zeros(df: DataFrame) -> DataFrame:
    return df.withColumns({
        c: F.when(F.col(c) == 0.0, F.lit(0.0)).otherwise(F.col(c)) for c in config.ZERO_NORMALIZED_COLUMNS
    })


def dedup(df: DataFrame) -> DataFrame:
    return normalize_zeros(df).dropDuplicates(config.DEDUP_ON)


def format_timestamps(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_dow", F.weekday("pickup_datetime") + 1)
        .withColumn("trip_duration_min", (F.col("dropoff_us") - F.col("pickup_us")) / 60_000_000)
    )


def join_zones(df: DataFrame, zones: DataFrame, join: str) -> DataFrame:
    """Join the zone table twice: once for pickup, once for dropoff."""
    for side in ("pu", "do"):
        lookup = zones.select(
            F.col("LocationID").alias(f"{side}_location_id"),
            F.col("Borough").alias(f"{side}_borough"),
            F.col("Zone").alias(f"{side}_zone"),
            F.col("service_zone").alias(f"{side}_service_zone"),
        )
        if join == "broadcast":
            lookup = F.broadcast(lookup)
        df = df.join(lookup, on=f"{side}_location_id", how="inner")
    return df


def apply_udf(df: DataFrame) -> DataFrame:
    speed = F.udf(avg_speed_mph, T.DoubleType(), useArrow=False)
    return df.withColumn("avg_speed_mph", speed("trip_distance", "pickup_us", "dropoff_us"))


def speed_filter(df: DataFrame) -> DataFrame:
    return df.filter(F.col("avg_speed_mph").between(*config.AVG_SPEED_MPH_RANGE))


def to_output(df: DataFrame) -> DataFrame:
    return df.select([F.col(n).cast(SPARK_TYPES[t]).alias(n) for n, t in config.OUTPUT_SCHEMA])


def build_stages(spark: SparkSession, paths: list[str], join: str) -> dict[str, DataFrame]:
    """The pipeline as a chain of lazy DataFrames, one per stage in config.STAGES."""
    raw = read_trips(spark, paths)
    no_nulls = drop_nulls(raw)
    valid = validity_filters(no_nulls)
    unique = format_timestamps(dedup(valid))
    joined = join_zones(unique, read_zones(spark), join)
    fast = speed_filter(apply_udf(joined))
    stages = dict(zip(config.STAGES, [raw, no_nulls, valid, unique, joined, fast]))
    stages["output"] = to_output(fast)
    return stages


def stage_counts(stages: dict[str, DataFrame]) -> list[dict]:
    """Materialize each stage in turn so each step's time excludes the steps before it."""
    rows, previous, cached = [], None, None
    for name in config.STAGES:
        df = stages[name].persist(StorageLevel.MEMORY_AND_DISK)
        start = time.perf_counter()
        n = df.count()
        elapsed = time.perf_counter() - start
        if cached is not None:
            cached.unpersist()
        cached = df
        rows.append({
            "stage": name,
            "rows": n,
            "removed": None if previous is None else previous - n,
            "seconds": round(elapsed, 3),
        })
        log.info("%-17s rows=%12s removed=%10s  %.2fs", name, f"{n:,}",
                 "" if previous is None else f"{previous - n:,}", elapsed)
        previous = n
    cached.unpersist()
    return rows


def explain_text(df: DataFrame) -> str:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="extended")
    return buf.getvalue()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", help="glob of trip parquet files (default: the 3 dev files)")
    parser.add_argument("--output", default=str(config.OUTPUT_DIR / "spark"))
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--join", choices=["shuffle", "broadcast"], default="shuffle")
    parser.add_argument("--shuffle-partitions", type=int, default=16)
    parser.add_argument("--stage-counts", action="store_true")
    args = parser.parse_args()

    paths = sorted(glob.glob(args.input)) if args.input else config.DEV_FILES
    if not paths:
        raise SystemExit(f"No input files match {args.input}")
    log.info("%d input files, join=%s, master=%s", len(paths), args.join, args.master)

    results_dir = config.ROOT / "results"
    results_dir.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    spark = build_session(args.master, args.join, args.shuffle_partitions)
    try:
        stages = build_stages(spark, paths, args.join)
        (results_dir / f"spark_explain_{args.join}.txt").write_text(explain_text(stages["output"]))

        if args.stage_counts:
            record = {"mode": "stage_counts", "stages": stage_counts(stages)}
        else:
            start = time.perf_counter()
            stages["output"].write.mode("overwrite").parquet(args.output)
            wall = time.perf_counter() - start
            out_rows = spark.read.parquet(args.output).count()
            log.info("wrote %s rows to %s in %.2fs", f"{out_rows:,}", args.output, wall)
            record = {"mode": "timed", "wall_seconds": round(wall, 3), "output_rows": out_rows}

        record.update({
            "framework": "spark",
            "join": args.join,
            "master": args.master,
            "input_files": len(paths),
            "timestamp": stamp,
        })
        runs_dir = results_dir / "runs"
        runs_dir.mkdir(exist_ok=True)
        (runs_dir / f"spark_{record['mode']}_{stamp}.json").write_text(json.dumps(record, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
