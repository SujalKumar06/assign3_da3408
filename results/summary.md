| Framework | Runs (s) | Mean (s) | Output rows |
|---|---|---:|---:|
| spark | 175.3 | 175.3 | 111,016,594 |
| ray | 345.0 | 345.0 | 111,016,594 |

| Stage | Rows (Spark) | Rows (Ray) | Spark (s) | Ray (s) |
|---|---:|---:|---:|---:|
| raw | 126,188,813 | 126,188,813 | 35.6 | 10.8 |
| drop_nulls | 118,072,836 | 118,072,836 | 29.3 | 22.8 |
| validity_filters | 111,083,668 | 111,083,668 | 35.9 | 26.9 |
| dedup | 111,083,664 | 111,083,664 | 74.4 | 73.5 |
| join | 111,083,664 | 111,083,664 | 90.7 | 175.8 |
| speed_filter | 111,016,594 | 111,016,594 | 108.0 | 132.4 |

| Python UDF stage | Spark (s) | Ray (s) |
|---|---:|---:|
| speed_filter (avg_speed_mph) | 108.0 | 132.4 |

| Run | Peak CPU % | Mean CPU % | Peak memory (GiB) | Mean memory (GiB) |
|---|---:|---:|---:|---:|
| ray_stage_counts | 1212 | 236 | 17.25 | 12.95 |
| ray_timed_1 | 1198 | 353 | 17.15 | 14.09 |
| spark_stage_counts | 1457 | 1104 | 12.64 | 9.71 |
| spark_timed_1 | 1294 | 1057 | 11.95 | 10.70 |

| Parity check | Result |
|---|---|
| Overall | PASSED |
| Schema match | True |
| Rows (Spark / Ray) | 111,016,594 / 111,016,594 |
| Rows only in Spark / only in Ray | 0 / 0 |
| Stage counts match | True |
