"""Checks that the Spark and Ray outputs contain exactly the same rows."""

import argparse
import glob
import json
import logging
import sys
from datetime import datetime

from pyspark.sql import SparkSession

from pipeline import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("verify_parity")


def latest_stage_counts(framework: str) -> dict | None:
    files = sorted(glob.glob(str(config.ROOT / "results" / "runs" / f"{framework}_stage_counts_*.json")))
    return json.loads(open(files[-1]).read()) if files else None


def compare_stage_counts() -> dict:
    spark, ray = latest_stage_counts("spark"), latest_stage_counts("ray")
    if not spark or not ray:
        return {"checked": False, "reason": "missing --stage-counts run for one framework"}
    if spark["input_files"] != ray["input_files"]:
        return {"checked": False, "reason": "latest stage-count runs used different inputs"}
    rows = [
        {"stage": s["stage"], "spark": s["rows"], "ray": r["rows"], "match": s["rows"] == r["rows"]}
        for s, r in zip(spark["stages"], ray["stages"])
    ]
    for r in rows:
        log.info("stage %-17s spark=%12s ray=%12s %s", r["stage"], f"{r['spark']:,}", f"{r['ray']:,}",
                 "OK" if r["match"] else "MISMATCH")
    return {"checked": True, "match": all(r["match"] for r in rows), "stages": rows}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spark", default=str(config.OUTPUT_DIR / "spark"))
    parser.add_argument("--ray", default=str(config.OUTPUT_DIR / "ray"))
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--driver-memory", default="2g")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("a3-verify-parity")
        .master(args.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.local.dir", str(config.ROOT / "data" / "tmp" / "spark"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        a, b = spark.read.parquet(args.spark), spark.read.parquet(args.ray)
        schema_a = [(f.name, f.dataType.simpleString()) for f in a.schema]
        schema_b = [(f.name, f.dataType.simpleString()) for f in b.schema]
        rows_a, rows_b = a.count(), b.count()
        only_spark = a.exceptAll(b).count()
        only_ray = b.exceptAll(a).count()

        result = {
            "schema_match": schema_a == schema_b,
            "spark_rows": rows_a,
            "ray_rows": rows_b,
            "rows_only_in_spark": only_spark,
            "rows_only_in_ray": only_ray,
            "stage_counts": compare_stage_counts(),
            "timestamp": datetime.now().strftime("%Y%m%d-%H%M%S"),
        }
        result["parity"] = (
            result["schema_match"] and rows_a == rows_b and only_spark == 0 and only_ray == 0
        )

        if not result["schema_match"]:
            log.error("schema mismatch:\n spark=%s\n ray=  %s", schema_a, schema_b)
        log.info("rows spark=%s ray=%s | only in spark=%s only in ray=%s",
                 f"{rows_a:,}", f"{rows_b:,}", f"{only_spark:,}", f"{only_ray:,}")
        log.info("PARITY %s", "PASSED" if result["parity"] else "FAILED")
        (config.ROOT / "results" / "parity.json").write_text(json.dumps(result, indent=2))
    finally:
        spark.stop()
    sys.exit(0 if result["parity"] else 1)


if __name__ == "__main__":
    main()
