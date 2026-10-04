"""The custom Python UDF, shared unchanged by the Spark and Ray pipelines."""

MICROS_PER_HOUR = 3_600_000_000


def avg_speed_mph(trip_distance: float, pickup_us: int, dropoff_us: int) -> float | None:
    """Average speed of one trip in miles per hour, from epoch-microsecond timestamps."""
    hours = (dropoff_us - pickup_us) / MICROS_PER_HOUR
    if hours <= 0:
        return None
    return trip_distance / hours
