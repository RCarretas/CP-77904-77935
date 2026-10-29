"""Pipeline do Projeto 1 (NYC Yellow Taxi) — versão base, sem otimizações.

Exemplos:
  python apps/pipeline.py --input data/raw --zones data/raw/taxi_zone_lookup.csv --sample 0.01
  spark-submit apps/pipeline.py --input data/raw --zones data/raw/taxi_zone_lookup.csv --years 2020
"""
import argparse
import csv
import os
import subprocess
import time
from datetime import datetime

from pyspark.sql import SparkSession

import ingest
import m1_clean
import m2_temporal
import m3_zones
import o1_fare_anomaly
import o2_od_flow

OPERATIONS = ["m1", "m2", "m3", "o1", "o2"]


def parse_args():
    parser = argparse.ArgumentParser(description="NYC Taxi analytics pipeline (PySpark)")
    parser.add_argument("--input", nargs="+", required=True,
                        help="pasta(s) e/ou ficheiros yellow_tripdata_YYYY-MM.parquet")
    parser.add_argument("--zones", required=True, help="caminho do taxi_zone_lookup.csv")
    parser.add_argument("--output", default="output", help="pasta base de saída")
    parser.add_argument("--years", nargs="+", type=int, help="anos a processar (por defeito, todos)")
    parser.add_argument("--months", nargs="+", type=int, help="meses a processar (por defeito, todos)")
    parser.add_argument("--sample", type=float, default=1.0,
                        help="fração dos registos a usar, para testes rápidos (ex.: 0.01)")
    parser.add_argument("--ops", nargs="+", choices=OPERATIONS, default=OPERATIONS,
                        help="operações a correr (por defeito, todas)")
    parser.add_argument("--master", default=None,
                        help="ex.: local[*] ou spark://spark-master:7077 (com spark-submit, usar --master do spark-submit)")
    parser.add_argument("--conf", nargs="*", default=[], metavar="CHAVE=VALOR",
                        help="configurações Spark extra, ex.: spark.driver.memory=4g")
    parser.add_argument("--event-log", default=None,
                        help="pasta para o event log do Spark (para rever as métricas depois)")
    parser.add_argument("--run-log", default="runs.csv", help="CSV onde se regista cada execução")
    parser.add_argument("--wait", action="store_true",
                        help="no fim, espera por Enter antes de fechar (para ver o Spark UI em :4040)")
    return parser.parse_args()


def build_spark(args) -> SparkSession:
    builder = SparkSession.builder.appName("compar-p1-pipeline")
    if args.master:
        builder = builder.master(args.master)
    for item in args.conf:
        key, value = item.split("=", 1)
        builder = builder.config(key, value)
    if args.event_log:
        os.makedirs(args.event_log, exist_ok=True)
        builder = (builder.config("spark.eventLog.enabled", "true")
                   .config("spark.eventLog.dir", args.event_log))
    return builder.getOrCreate()


def git_commit():
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"],
                                       stderr=subprocess.DEVNULL, text=True).strip()
    except Exception:
        return ""


def append_run_log(path, rows):
    new_file = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        if new_file:
            writer.writeheader()
        writer.writerows(rows)


def main():
    args = parse_args()
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    out = os.path.join(args.output, run_id)

    spark = build_spark(args)
    files = ingest.list_input_files(args.input, args.years, args.months)
    print(f"[{run_id}] {len(files)} ficheiro(s) de entrada, sample={args.sample}, ops={args.ops}")

    trips = ingest.load_trips(spark, files, args.sample)
    zones = ingest.load_zones(spark, args.zones)
    clean_trips = m1_clean.clean(trips)

    # Cada operação escreve o seu resultado (uma ação), por isso o tempo de cada
    # uma inclui ler e limpar os dados de novo: nada é guardado em cache.
    steps = {
        "m1": lambda: m1_clean.run(trips, f"{out}/m1_cleaning_stats"),
        "m2": lambda: m2_temporal.run(clean_trips, zones, f"{out}/m2_temporal_demand"),
        "m3": lambda: m3_zones.run(clean_trips, zones, f"{out}/m3_zone_stats"),
        "o1": lambda: o1_fare_anomaly.run(clean_trips, zones, f"{out}/o1_fare_anomaly"),
        "o2": lambda: o2_od_flow.run(clean_trips, zones, f"{out}/o2_od_flow"),
    }

    timings = {}
    total_start = time.perf_counter()
    for op in args.ops:
        start = time.perf_counter()
        result = steps[op]()
        timings[op] = time.perf_counter() - start
        print(f"[{run_id}] {op}: {timings[op]:.1f} s")
        if op == "m1":
            result.show(truncate=False)
    timings["total"] = time.perf_counter() - total_start

    base = {
        "run_id": run_id,
        "app_id": spark.sparkContext.applicationId,
        "git_commit": git_commit(),
        "master": spark.sparkContext.master,
        "default_parallelism": spark.sparkContext.defaultParallelism,
        "files": len(files),
        "years": " ".join(map(str, args.years or [])),
        "months": " ".join(map(str, args.months or [])),
        "sample": args.sample,
        "conf": " ".join(args.conf),
    }
    append_run_log(args.run_log, [{**base, "operation": op, "seconds": round(s, 2)}
                                  for op, s in timings.items()])
    print(f"[{run_id}] total: {timings['total']:.1f} s — resultados em {out}")

    if args.wait:
        input("Spark UI em http://localhost:4040 — Enter para terminar...")
    spark.stop()


if __name__ == "__main__":
    main()
