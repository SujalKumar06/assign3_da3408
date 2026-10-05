#!/usr/bin/env bash
REPS=${REPS:-1}
INPUT='/app/data/raw/trips/*.parquet'
mkdir -p results/monitor results/logs

monitor() {
    while true; do
        docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}" $(docker ps -q --filter name=a3_) \
            | sed "s/^/$(date +%s),/" >> "results/monitor/$1.csv"
        { date +%T; top -b -n 1 -o %CPU | head -20; } >> "results/monitor/$1.top.txt"
        sleep 1
    done
}

run() {
    echo ">>> $1 ($(date +%T))"
    monitor "$1" & mon=$!
    "${@:2}" > "results/logs/$1.log" 2>&1 || echo "    FAILED, see results/logs/$1.log"
    kill $mon
    grep -E "INFO (spark|ray)_clean:" "results/logs/$1.log"
}

spark() {
    docker exec a3_spark_master spark-submit --master spark://spark-master:7077 \
        --driver-memory 2g --executor-memory 4g \
        --conf spark.sql.files.maxPartitionBytes=32m /app/spark_clean.py \
        --master spark://spark-master:7077 --input "$INPUT" --shuffle-partitions 200 "$@"
}

ray_job() {
    docker exec a3_ray_head python /app/ray_clean.py --address auto --input "$INPUT" --shuffle-partitions 200 "$@"
}

docker compose -f docker/spark-compose.yml up -d
until [ "$(docker logs a3_spark_worker_a 2>&1 | grep -c registered)$(docker logs a3_spark_worker_b 2>&1 | grep -c registered)" = 11 ]; do sleep 2; done
run spark_stage_counts spark --stage-counts
for i in $(seq "$REPS"); do run spark_timed_$i spark; done
docker compose -f docker/spark-compose.yml down

docker compose -f docker/ray-compose.yml up -d
until docker exec a3_ray_head ray status 2>/dev/null | grep -q "/12.0 CPU"; do sleep 2; done
run ray_stage_counts ray_job --stage-counts
for i in $(seq "$REPS"); do run ray_timed_$i ray_job; done
docker compose -f docker/ray-compose.yml down

python verify_parity.py --master "local[12]" --driver-memory 12g 2>&1 | grep "verify_parity:"
python benchmark/summarize.py
