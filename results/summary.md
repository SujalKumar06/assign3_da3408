| Framework | Runs (s) | Mean (s) | Output rows |
|---|---|---:|---:|
| spark | 185.9 | 185.9 | 111,016,594 |
| ray | 347.1 | 347.1 | 111,016,594 |

| Stage | Rows (Spark) | Rows (Ray) | Spark (s) | Ray (s) |
|---|---:|---:|---:|---:|
| raw | 126,188,813 | 126,188,813 | 36.9 | 11.9 |
| drop_nulls | 118,072,836 | 118,072,836 | 30.8 | 26.5 |
| validity_filters | 111,083,668 | 111,083,668 | 36.8 | 25.1 |
| dedup | 111,083,664 | 111,083,664 | 74.9 | 80.0 |
| join | 111,083,664 | 111,083,664 | 92.4 | 187.1 |
| speed_filter | 111,016,594 | 111,016,594 | 113.9 | 132.9 |

| Python UDF stage | Spark (s) | Ray (s) |
|---|---:|---:|
| speed_filter (avg_speed_mph) | 113.9 | 132.9 |

| Run | Peak CPU % | Mean CPU % | Peak memory (GiB) | Mean memory (GiB) |
|---|---:|---:|---:|---:|
| ray_stage_counts | 1351 | 248 | 17.69 | 12.83 |
| ray_timed_1 | 1220 | 361 | 17.39 | 14.80 |
| spark_stage_counts | 1288 | 1104 | 12.72 | 9.70 |
| spark_timed_1 | 1300 | 1092 | 12.10 | 10.86 |

| Parity check | Result |
|---|---|
| Overall | PASSED |
| Schema match | True |
| Rows (Spark / Ray) | 111,016,594 / 111,016,594 |
| Rows only in Spark / only in Ray | 0 / 0 |
| Stage counts match | True |
