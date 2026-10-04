"""Shared schema and cleaning rules for spark_clean.py and ray_clean.py."""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW_TRIPS_GLOB = str(ROOT / "data" / "raw" / "trips" / "*.parquet")
ZONES_CSV = str(ROOT / "data" / "raw" / "taxi_zone_lookup.csv")
OUTPUT_DIR = ROOT / "data" / "output"

RENAMES = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "ratecode_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
    "Airport_fee": "airport_fee",
}

DROPPED_COLUMNS = ["cbd_congestion_fee"]

TRIP_SCHEMA = [
    ("vendor_id", "int32"),
    ("pickup_datetime", "timestamp"),
    ("dropoff_datetime", "timestamp"),
    ("passenger_count", "int64"),
    ("trip_distance", "double"),
    ("ratecode_id", "int64"),
    ("store_and_fwd_flag", "string"),
    ("pu_location_id", "int32"),
    ("do_location_id", "int32"),
    ("payment_type", "int64"),
    ("fare_amount", "double"),
    ("extra", "double"),
    ("mta_tax", "double"),
    ("tip_amount", "double"),
    ("tolls_amount", "double"),
    ("improvement_surcharge", "double"),
    ("total_amount", "double"),
    ("congestion_surcharge", "double"),
    ("airport_fee", "double"),
]
TRIP_COLUMNS = [name for name, _ in TRIP_SCHEMA]

ZONE_COLUMNS = ["LocationID", "Borough", "Zone", "service_zone"]
# Keep zone 265's "N/A" as text; pandas would otherwise read it as null.
ZONE_NA_VALUES: list[str] = []

DROP_NULLS_IN = TRIP_COLUMNS

PICKUP_START = "2022-01-01 00:00:00"
PICKUP_END = "2025-03-01 00:00:00"  # exclusive

MAX_DURATION_SECONDS = 6 * 3600
TRIP_DISTANCE_MAX = 200.0
FARE_AMOUNT_MIN_EXCLUSIVE = 0.0
TOTAL_AMOUNT_MIN_EXCLUSIVE = 0.0
PASSENGER_COUNT_RANGE = (1, 6)
RATECODE_ID_RANGE = (1, 6)
LOCATION_ID_RANGE = (1, 263)

DEDUP_ON = TRIP_COLUMNS
# -0.0 becomes 0.0 in these before dedup, so both engines treat them as the same value.
ZERO_NORMALIZED_COLUMNS = [name for name, kind in TRIP_SCHEMA if kind == "double"]

AVG_SPEED_MPH_RANGE = (0.5, 80.0)

STAGES = ["raw", "drop_nulls", "validity_filters", "dedup", "join", "speed_filter"]

# pickup_dow is ISO: 1 = Monday ... 7 = Sunday.
OUTPUT_SCHEMA = TRIP_SCHEMA + [
    ("pickup_date", "date"),
    ("pickup_hour", "int32"),
    ("pickup_dow", "int32"),
    ("trip_duration_min", "double"),
    ("pu_borough", "string"),
    ("pu_zone", "string"),
    ("pu_service_zone", "string"),
    ("do_borough", "string"),
    ("do_zone", "string"),
    ("do_service_zone", "string"),
    ("avg_speed_mph", "double"),
]
OUTPUT_COLUMNS = [name for name, _ in OUTPUT_SCHEMA]

# One file per raw schema version.
DEV_FILES = [
    str(ROOT / "data" / "raw" / "trips" / f"yellow_tripdata_{m}.parquet")
    for m in ("2022-01", "2024-01", "2025-01")
]
