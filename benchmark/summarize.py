import csv
import glob
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline import config  # noqa: E402

RESULTS = config.ROOT / "results"
UNITS = {"B": 1, "kB": 1e3, "KiB": 2**10, "MB": 1e6, "MiB": 2**20, "GB": 1e9, "GiB": 2**30}


def runs(framework: str, mode: str) -> list[dict]:
    files = sorted(glob.glob(str(RESULTS / "runs" / f"{framework}_{mode}_*.json")))
    return [json.loads(Path(f).read_text()) for f in files]


def to_gib(text: str) -> float:
    value, unit = re.match(r"([\d.]+)\s*([A-Za-z]+)", text.strip()).groups()
    return float(value) * UNITS[unit] / 2**30


def monitor_peaks(path: str) -> dict | None:
    cpu, mem = defaultdict(float), defaultdict(float)
    with open(path) as f:
        for ts, _, cpu_pct, usage in csv.reader(f):
            cpu[ts] += float(cpu_pct.rstrip("%"))
            mem[ts] += to_gib(usage.split("/")[0])
    if not cpu:
        return None
    return {
        "peak_cpu": max(cpu.values()),
        "mean_cpu": sum(cpu.values()) / len(cpu),
        "peak_mem": max(mem.values()),
        "mean_mem": sum(mem.values()) / len(mem),
    }


def main() -> None:
    out = ["| Framework | Runs (s) | Mean (s) | Output rows |", "|---|---|---:|---:|"]
    for fw in ("spark", "ray"):
        timed = runs(fw, "timed")
        if timed:
            secs = [r["wall_seconds"] for r in timed]
            out.append(f"| {fw} | {', '.join(f'{s:.1f}' for s in secs)} | {sum(secs) / len(secs):.1f} "
                       f"| {timed[-1]['output_rows']:,} |")

    spark, ray = runs("spark", "stage_counts"), runs("ray", "stage_counts")
    if spark and ray:
        s, r = spark[-1]["stages"], ray[-1]["stages"]
        out += ["", "| Stage | Rows (Spark) | Rows (Ray) | Spark (s) | Ray (s) |", "|---|---:|---:|---:|---:|"]
        for a, b in zip(s, r):
            out.append(f"| {a['stage']} | {a['rows']:,} | {b['rows']:,} | {a['seconds']:.1f} | {b['seconds']:.1f} |")
        out += ["", "| Python UDF stage | Spark (s) | Ray (s) |", "|---|---:|---:|",
                f"| speed_filter (avg_speed_mph) | {s[-1]['seconds']:.1f} | {r[-1]['seconds']:.1f} |"]

    out += ["", "| Run | Peak CPU % | Mean CPU % | Peak memory (GiB) | Mean memory (GiB) |", "|---|---:|---:|---:|---:|"]
    for path in sorted(glob.glob(str(RESULTS / "monitor" / "*.csv"))):
        p = monitor_peaks(path)
        if p:
            out.append(f"| {Path(path).stem} | {p['peak_cpu']:.0f} | {p['mean_cpu']:.0f} "
                       f"| {p['peak_mem']:.2f} | {p['mean_mem']:.2f} |")

    parity = RESULTS / "parity.json"
    if parity.exists():
        p = json.loads(parity.read_text())
        out += ["", "| Parity check | Result |", "|---|---|",
                f"| Overall | {'PASSED' if p['parity'] else 'FAILED'} |",
                f"| Schema match | {p['schema_match']} |",
                f"| Rows (Spark / Ray) | {p['spark_rows']:,} / {p['ray_rows']:,} |",
                f"| Rows only in Spark / only in Ray | {p['rows_only_in_spark']:,} / {p['rows_only_in_ray']:,} |",
                f"| Stage counts match | {p['stage_counts'].get('match')} |"]

    text = "\n".join(out) + "\n"
    (RESULTS / "summary.md").write_text(text)
    print(text)


if __name__ == "__main__":
    main()
