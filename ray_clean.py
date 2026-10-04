"""Ray Data implementation of the preprocessing pipeline."""

import argparse
import glob
import json
import logging
import shutil
import time
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import ray
import ray.data
from ray.data.context import DataContext, ShuffleStrategy

from pipeline import config
from pipeline.udf import avg_speed_mph

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("ray_clean")

ARROW_TYPES = {
    "int32": pa.int32(),
    "int64": pa.int64(),
    "double": pa.float64(),
    "string": pa.string(),
    "timestamp": pa.timestamp("us"),
    "date": pa.date32(),
}
TRIP_ARROW = pa.schema([(n, ARROW_TYPES[t]) for n, t in config.TRIP_SCHEMA])
OUTPUT_ARROW = pa.schema([(n, ARROW_TYPES[t]) for n, t in config.OUTPUT_SCHEMA])


def unify(t: pa.Table) -> pa.Table:
    t = t.drop_columns([c for c in config.DROPPED_COLUMNS if c in t.column_names])
    t = t.rename_columns([config.RENAMES[c] for c in t.column_names])
    return t.select(config.TRIP_COLUMNS).cast(TRIP_ARROW)


def read_trips(paths: list[str]) -> ray.data.Dataset:
    """Read each file on its own and unify it, since the raw schemas differ."""
    parts = [ray.data.read_parquet(p).map_batches(unify, batch_format="pyarrow") for p in paths]
    return parts[0].union(*parts[1:]) if len(parts) > 1 else parts[0]


def read_zones() -> pa.Table:
    zones = pacsv.read_csv(
        config.ZONES_CSV,
        convert_options=pacsv.ConvertOptions(
            column_types={c: pa.string() for c in config.ZONE_COLUMNS},
            null_values=config.ZONE_NA_VALUES,
            strings_can_be_null=False,
        ),
    )
    return zones.set_column(0, "LocationID", zones["LocationID"].cast(pa.int32()))


def epoch_us(col: pa.ChunkedArray) -> pa.ChunkedArray:
    return col.cast(pa.int64())


def drop_nulls(t: pa.Table) -> pa.Table:
    mask = pc.is_valid(t[config.DROP_NULLS_IN[0]])
    for c in config.DROP_NULLS_IN[1:]:
        mask = pc.and_(mask, pc.is_valid(t[c]))
    return t.filter(mask)


def validity_filters(t: pa.Table) -> pa.Table:
    ts = lambda s: pa.scalar(datetime.fromisoformat(s), pa.timestamp("us"))  # noqa: E731
    between = lambda c, lo, hi: pc.and_(pc.greater_equal(t[c], lo), pc.less_equal(t[c], hi))  # noqa: E731
    duration_us = pc.subtract(epoch_us(t["dropoff_datetime"]), epoch_us(t["pickup_datetime"]))
    conditions = [
        pc.greater_equal(t["pickup_datetime"], ts(config.PICKUP_START)),
        pc.less(t["pickup_datetime"], ts(config.PICKUP_END)),
        pc.greater(duration_us, 0),
        pc.less_equal(duration_us, config.MAX_DURATION_SECONDS * 1_000_000),
        pc.greater(t["trip_distance"], 0),
        pc.less_equal(t["trip_distance"], config.TRIP_DISTANCE_MAX),
        pc.greater(t["fare_amount"], config.FARE_AMOUNT_MIN_EXCLUSIVE),
        pc.greater(t["total_amount"], config.TOTAL_AMOUNT_MIN_EXCLUSIVE),
        between("passenger_count", *config.PASSENGER_COUNT_RANGE),
        between("ratecode_id", *config.RATECODE_ID_RANGE),
        between("pu_location_id", *config.LOCATION_ID_RANGE),
        between("do_location_id", *config.LOCATION_ID_RANGE),
    ]
    mask = conditions[0]
    for cond in conditions[1:]:
        mask = pc.and_(mask, cond)
    return t.filter(mask)


def normalize_zeros(t: pa.Table) -> pa.Table:
    for c in config.ZERO_NORMALIZED_COLUMNS:
        col = t[c]
        t = t.set_column(t.column_names.index(c), c, pc.if_else(pc.equal(col, 0.0), 0.0, col))
    return t


def cleanse(t: pa.Table) -> pa.Table:
    # One map step: a map_batches after a block was emptied would emit a column-less block,
    # which breaks Ray's hash shuffle.
    return normalize_zeros(validity_filters(drop_nulls(t)))


def drop_duplicate_rows(t: pa.Table) -> pa.Table:
    return t.group_by(config.DEDUP_ON, use_threads=False).aggregate([]).select(t.column_names)


def dedup(ds: ray.data.Dataset, partitions: int) -> ray.data.Dataset:
    """Hash-partition on every column so identical rows share one block, then dedup each block."""
    return ds.repartition(partitions, keys=config.DEDUP_ON).map_batches(
        drop_duplicate_rows, batch_format="pyarrow", batch_size=None
    )


def format_timestamps(t: pa.Table) -> pa.Table:
    pickup = t["pickup_datetime"]
    pickup_us, dropoff_us = epoch_us(pickup), epoch_us(t["dropoff_datetime"])
    duration_min = pc.divide(pc.subtract(dropoff_us, pickup_us).cast(pa.float64()), 60_000_000.0)
    return (
        t.append_column("pickup_us", pickup_us)
        .append_column("dropoff_us", dropoff_us)
        .append_column("pickup_date", pickup.cast(pa.date32()))
        .append_column("pickup_hour", pc.hour(pickup).cast(pa.int32()))
        .append_column("pickup_dow", pc.day_of_week(pickup, count_from_zero=False, week_start=1).cast(pa.int32()))
        .append_column("trip_duration_min", duration_min)
    )


def zone_lookup(zones: pa.Table, side: str) -> pa.Table:
    return zones.rename_columns(
        [f"{side}_location_id", f"{side}_borough", f"{side}_zone", f"{side}_service_zone"]
    )


def broadcast_join(t: pa.Table, zones_ref, side: str) -> pa.Table:
    lookup = zone_lookup(ray.get(zones_ref), side)
    return t.join(lookup, keys=f"{side}_location_id", join_type="inner", use_threads=False)


def join_zones(ds: ray.data.Dataset, zones: pa.Table, join: str, partitions: int) -> ray.data.Dataset:
    """Join the zone table twice: once for pickup, once for dropoff."""
    zones_ref = ray.put(zones)
    for side in ("pu", "do"):
        if join == "broadcast":
            ds = ds.map_batches(
                broadcast_join, batch_format="pyarrow", fn_kwargs={"zones_ref": zones_ref, "side": side}
            )
        else:
            lookup = ray.data.from_arrow(zone_lookup(zones, side))
            ds = ds.join(lookup, join_type="inner", num_partitions=partitions, on=(f"{side}_location_id",))
    return ds


def apply_udf(t: pa.Table) -> pa.Table:
    speeds = [
        avg_speed_mph(d, p, q)
        for d, p, q in zip(
            t["trip_distance"].to_pylist(), t["pickup_us"].to_pylist(), t["dropoff_us"].to_pylist()
        )
    ]
    return t.append_column("avg_speed_mph", pa.array(speeds, pa.float64()))


def speed_filter(t: pa.Table) -> pa.Table:
    lo, hi = config.AVG_SPEED_MPH_RANGE
    speed = t["avg_speed_mph"]
    return t.filter(pc.and_(pc.greater_equal(speed, lo), pc.less_equal(speed, hi)))


def to_output(t: pa.Table) -> pa.Table:
    return t.select(config.OUTPUT_COLUMNS).cast(OUTPUT_ARROW)


def pipeline_steps(join: str, partitions: int) -> list:
    """(stage name, Dataset -> Dataset) for every stage after raw, in config.STAGES order."""
    zones = read_zones()
    batch = lambda fn: lambda ds: ds.map_batches(fn, batch_format="pyarrow")  # noqa: E731
    return [
        ("validity_filters", batch(cleanse)),
        ("dedup", lambda ds: dedup(ds, partitions).map_batches(format_timestamps, batch_format="pyarrow")),
        ("join", lambda ds: join_zones(ds, zones, join, partitions)),
        ("speed_filter", lambda ds: ds.map_batches(apply_udf, batch_format="pyarrow")
                                       .map_batches(speed_filter, batch_format="pyarrow")),
    ]


def stage_counts(raw: ray.data.Dataset, steps: list) -> list[dict]:
    """Materialize each stage in turn so each step's time excludes the steps before it."""
    rows, previous, ds = [], None, raw
    side_count = ("drop_nulls", lambda d: d.map_batches(drop_nulls, batch_format="pyarrow"))
    for name, step in [("raw", lambda d: d), side_count] + steps:
        start = time.perf_counter()
        out = step(ds).materialize()
        n = out.count()
        if name != "drop_nulls":
            ds = out
        elapsed = time.perf_counter() - start
        rows.append({
            "stage": name,
            "rows": n,
            "removed": None if previous is None else previous - n,
            "seconds": round(elapsed, 3),
        })
        log.info("%-17s rows=%12s removed=%10s  %.2fs", name, f"{n:,}",
                 "" if previous is None else f"{previous - n:,}", elapsed)
        previous = n
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", help="glob of trip parquet files (default: the 3 dev files)")
    parser.add_argument("--output", default=str(config.OUTPUT_DIR / "ray"))
    parser.add_argument("--address", help="Ray cluster address (default: start a local Ray)")
    parser.add_argument("--num-cpus", type=int, default=2)
    parser.add_argument("--join", choices=["shuffle", "broadcast"], default="shuffle")
    parser.add_argument("--shuffle-partitions", type=int, default=16)
    parser.add_argument("--block-mb", type=int, default=32, help="max block size; smaller blocks need less memory per task")
    parser.add_argument("--stage-counts", action="store_true")
    args = parser.parse_args()

    paths = sorted(glob.glob(args.input)) if args.input else config.DEV_FILES
    if not paths:
        raise SystemExit(f"No input files match {args.input}")
    log.info("%d input files, join=%s, address=%s", len(paths), args.join, args.address or "local")

    results_dir = config.ROOT / "results"
    results_dir.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    if args.address:
        ray.init(address=args.address, logging_level="WARNING")
    else:
        ray.init(num_cpus=args.num_cpus, include_dashboard=False, logging_level="WARNING")
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE_V2
    ctx.target_max_block_size = args.block_mb * 1024 * 1024
    ctx.enable_progress_bars = False
    try:
        raw = read_trips(paths)
        steps = pipeline_steps(args.join, args.shuffle_partitions)

        if args.stage_counts:
            record = {"mode": "stage_counts", "stages": stage_counts(raw, steps)}
        else:
            shutil.rmtree(args.output, ignore_errors=True)
            start = time.perf_counter()
            ds = raw
            for _, step in steps:
                ds = step(ds)
            ds = ds.map_batches(to_output, batch_format="pyarrow")
            ds.write_parquet(args.output)
            wall = time.perf_counter() - start
            (results_dir / f"ray_stats_{args.join}.txt").write_text(ds.stats())
            out_rows = ray.data.read_parquet(args.output).count()
            log.info("wrote %s rows to %s in %.2fs", f"{out_rows:,}", args.output, wall)
            record = {"mode": "timed", "wall_seconds": round(wall, 3), "output_rows": out_rows}

        record.update({
            "framework": "ray",
            "join": args.join,
            "block_mb": args.block_mb,
            "address": args.address or f"local[{args.num_cpus}]",
            "input_files": len(paths),
            "timestamp": stamp,
        })
        runs_dir = results_dir / "runs"
        runs_dir.mkdir(exist_ok=True)
        (runs_dir / f"ray_{record['mode']}_{stamp}.json").write_text(json.dumps(record, indent=2))
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
