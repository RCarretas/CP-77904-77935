"""
pipeline.py — NYC Yellow Taxi, Projeto 1 (Large-Scale Analytics with Spark)

BASELINE: implementação direta, sem tuning de performance (gerada com IA,
ver acknowledgements do relatório). De propósito:
  * sem cache/persist do DataFrame limpo  -> cada operação relê e relimpa os Parquet
  * sem select de colunas (column pruning manual)
  * sem broadcast() explícito nos joins com a tabela de zonas
  * spark.sql.shuffle.partitions e AQE com os valores por defeito
É este o ponto de partida do ciclo profile -> hypothesise -> optimise -> measure.

Operações: M1, M2, M3 (obrigatórias) + O1, O2 (opcionais).

Exemplo (dentro do contentor jupyter):
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
      /workspace/apps/pipeline.py --input /data --output /data/output \
      --years 2018 2019 2020 --run-name baseline
"""

import argparse
import json
import os
import sys
import time
from contextlib import contextmanager
from functools import reduce

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, IntegerType,
                               DoubleType, StringType, TimestampType)

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
TARGET_SCHEMA = StructType([
    StructField("tpep_pickup_datetime",  TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count",       IntegerType()),
    StructField("trip_distance",         DoubleType()),
    StructField("RatecodeID",            IntegerType()),
    StructField("PULocationID",          IntegerType()),
    StructField("DOLocationID",          IntegerType()),
    StructField("payment_type",          IntegerType()),
    StructField("fare_amount",           DoubleType()),
    StructField("tip_amount",            DoubleType()),
    StructField("total_amount",          DoubleType()),
])

ZONES_SCHEMA = StructType([
    StructField("LocationID",   IntegerType()),
    StructField("Borough",      StringType()),
    StructField("Zone",         StringType()),
    StructField("service_zone", StringType()),
])

NULL_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
             "trip_distance", "fare_amount", "PULocationID", "DOLocationID"]


# ---------------------------------------------------------------------------
# Argumentos
# ---------------------------------------------------------------------------
def parse_args(argv=None):
    p = argparse.ArgumentParser(description="NYC Taxi Spark pipeline (baseline)")
    p.add_argument("--master", default=None,
                   help="ex.: spark://spark-master:7077 ou local[*] "
                        "(por defeito usa o do spark-submit)")
    p.add_argument("--input", default="/data",
                   help="pasta com yellow_tripdata_YYYY-MM.parquet e taxi_zone_lookup.csv")
    p.add_argument("--output", default="/data/output", help="pasta de saída")
    p.add_argument("--years", type=int, nargs="+", default=[2018, 2019, 2020])
    p.add_argument("--months", type=int, nargs="+", default=list(range(1, 13)),
                   help="meses a ler (ficheiros que não existam são ignorados)")
    p.add_argument("--fraction", type=float, default=1.0,
                   help="fração dos registos (ex.: 0.1 para smoke-test / scale test)")
    p.add_argument("--ops", nargs="+", default=["M1", "M2", "M3", "O1", "O2"],
                   choices=["M1", "M2", "M3", "O1", "O2"])
    p.add_argument("--run-name", default="baseline",
                   help="etiqueta desta run (vai para o ficheiro de métricas)")
    p.add_argument("--metrics-file", default=None,
                   help="CSV onde acrescentar métricas (default: <output>/metrics.csv)")
    p.add_argument("--event-log-dir", default=None,
                   help="se dado, ativa spark.eventLog (para o History Server)")
    return p.parse_args(argv)


def build_spark(args):
    b = SparkSession.builder.appName(f"nyc-taxi-{args.run_name}")
    if args.master:
        b = b.master(args.master)
    if args.event_log_dir:
        b = (b.config("spark.eventLog.enabled", "true")
              .config("spark.eventLog.dir", args.event_log_dir))
    return b.getOrCreate()


# ---------------------------------------------------------------------------
# Profiling: tempo de parede sempre; métricas de stage via spark_profiler.py
# (shuffle read/write, nº de tasks, spill) se a REST API da UI estiver acessível.
# ---------------------------------------------------------------------------
class StepTimer:
    def __init__(self, spark):
        self.results = []
        self.profiler = None
        try:
            sys.path.append(os.path.dirname(os.path.abspath(__file__)))
            from spark_profiler import SparkProfiler
            self.profiler = SparkProfiler(spark)
        except Exception as e:  # o pipeline funciona sem profiler
            print(f"[aviso] SparkProfiler indisponível: {e!r}")

    @contextmanager
    def step(self, name):
        print(f"\n===== {name} =====")
        t0 = time.time()
        if self.profiler is not None:
            with self.profiler.profile(name):
                yield
        else:
            yield
        wall = time.time() - t0
        row = {"step": name, "wall_time_s": round(wall, 2)}
        if self.profiler is not None and name in self.profiler.runs:
            row.update(self.profiler.runs[name])
            row["wall_time_s"] = round(wall, 2)
        self.results.append(row)
        print(f"[{name}] {wall:.2f}s")


def save_metrics(rows, path, args, spark):
    import csv
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conf = {
        "master": spark.sparkContext.master,
        "default_parallelism": spark.sparkContext.defaultParallelism,
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "aqe": spark.conf.get("spark.sql.adaptive.enabled"),
    }
    fields = ["run_name", "step", "years", "fraction", "master",
              "default_parallelism", "shuffle_partitions", "aqe",
              "wall_time_s", "num_jobs", "num_stages", "num_tasks",
              "executor_run_time_ms", "executor_cpu_time_ms",
              "input_bytes", "output_bytes",
              "shuffle_read_bytes", "shuffle_write_bytes",
              "memory_spilled_bytes", "disk_spilled_bytes"]
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if new:
            w.writeheader()
        for r in rows:
            w.writerow({"run_name": args.run_name,
                        "years": " ".join(map(str, args.years)),
                        "fraction": args.fraction, **conf, **r})
    print(f"\nMétricas acrescentadas a {path}")


# ---------------------------------------------------------------------------
# M1 — Ingestão e limpeza
# ---------------------------------------------------------------------------
def to_target_schema(df, schema=TARGET_SCHEMA):
    """O schema muda entre ficheiros (ex.: passenger_count double vs long).
    Cast de cada coluna para o tipo alvo; colunas em falta ficam null."""
    existing = set(df.columns)
    return df.select([
        (F.col(f.name) if f.name in existing else F.lit(None))
        .cast(f.dataType).alias(f.name)
        for f in schema.fields
    ])


def list_files(input_dir, years, months):
    files = []
    for y in years:
        for m in months:
            p = os.path.join(input_dir, f"yellow_tripdata_{y}-{m:02d}.parquet")
            if os.path.exists(p):
                files.append((p, y))
    if not files:
        raise SystemExit(f"Nenhum ficheiro encontrado em {input_dir} "
                         f"para anos={years} meses={months}")
    return files


def load_raw_trips(spark, files, fraction):
    """Lê mês a mês (schemas diferentes entre ficheiros) e faz union."""
    dfs = [to_target_schema(spark.read.parquet(p)).withColumn("file_year", F.lit(y))
           for p, y in files]
    trips = reduce(lambda a, b: a.unionByName(b), dfs)
    if fraction < 1.0:
        trips = trips.sample(withReplacement=False, fraction=fraction, seed=42)
    return trips


def load_zones(spark, input_dir):
    return spark.read.csv(os.path.join(input_dir, "taxi_zone_lookup.csv"),
                          header=True, schema=ZONES_SCHEMA)


def _out_of_range(c):
    return (F.col(c) < 1) | (F.col(c) > 263)


# Critérios do enunciado + 2 extra (nulls e ano do pickup != ano do ficheiro,
# que existem nos dados: ex. viagens com data de 2009 em ficheiros de 2018).
# coalesce(..., False): comparações com null dão null -> não contam como falha
# nesse critério (os nulls já são apanhados por 'null_values').
# (é uma função porque F.col precisa de uma SparkSession ativa)
def bad_flags():
    invalid = {
        "null_values":           reduce(lambda a, b: a | b,
                                        [F.col(c).isNull() for c in NULL_COLS]),
        "fare_amount_le_0":      F.col("fare_amount") <= 0,
        "trip_distance_le_0":    F.col("trip_distance") <= 0,
        "passenger_count_0_gt6": (F.col("passenger_count") == 0) | (F.col("passenger_count") > 6),
        "dropoff_le_pickup":     F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"),
        "location_out_of_range": _out_of_range("PULocationID") | _out_of_range("DOLocationID"),
        "year_mismatch":         F.year("tpep_pickup_datetime") != F.col("file_year"),
    }
    bad = {n: F.coalesce(c, F.lit(False)) for n, c in invalid.items()}
    any_bad = reduce(lambda a, b: a | b, bad.values())
    return bad, any_bad


def cleaning_stats(raw):
    """Por ano: total, mantidos e nº de registos que falham cada critério
    (um registo pode falhar vários critérios, por isso as colunas sobrepõem-se)."""
    bad, any_bad = bad_flags()
    aggs = [F.count("*").alias("total"),
            F.sum((~any_bad).cast("int")).alias("kept")]
    aggs += [F.sum(c.cast("int")).alias(f"removed_{n}") for n, c in bad.items()]
    return raw.groupBy("file_year").agg(*aggs).orderBy("file_year")


def clean(raw):
    _, any_bad = bad_flags()
    return raw.filter(~any_bad).drop("file_year")


def run_m1(raw, out):
    stats = cleaning_stats(raw)
    stats.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(os.path.join(out, "m1_cleaning_stats"))
    stats.show(truncate=False)
    return stats


# ---------------------------------------------------------------------------
# M2 — Procura temporal
# ---------------------------------------------------------------------------
def run_m2(trips, zones, out):
    m2 = (trips
          .join(zones, trips.PULocationID == zones.LocationID, "left")
          .withColumn("pickup_borough", F.col("Borough"))
          .withColumn("day_of_week", F.dayofweek("tpep_pickup_datetime"))  # 1=Dom ... 7=Sáb
          .withColumn("hour_of_day", F.hour("tpep_pickup_datetime"))
          .withColumn("duration_min",
                      (F.unix_timestamp("tpep_dropoff_datetime")
                       - F.unix_timestamp("tpep_pickup_datetime")) / 60.0)
          .groupBy("pickup_borough", "day_of_week", "hour_of_day")
          .agg(F.count("*").alias("trip_count"),
               F.avg("fare_amount").alias("mean_fare"),
               F.avg("duration_min").alias("mean_duration_min"),
               F.avg("trip_distance").alias("mean_distance")))

    m2.write.mode("overwrite").partitionBy("pickup_borough") \
        .parquet(os.path.join(out, "m2_temporal_demand"))
    return m2


# ---------------------------------------------------------------------------
# M3 — Estatísticas por zona
# ---------------------------------------------------------------------------
def run_m3(trips, zones, out):
    zone_month = (trips
        .join(zones, trips.PULocationID == zones.LocationID, "inner")
        .withColumn("year", F.year("tpep_pickup_datetime"))
        .withColumn("month", F.month("tpep_pickup_datetime"))
        .groupBy("Borough", "PULocationID", "Zone", "year", "month")
        .agg(F.count("*").alias("trip_count"),
             F.avg("fare_amount").alias("mean_fare"),
             F.stddev("fare_amount").alias("std_fare"),
             F.avg("trip_distance").alias("mean_distance"),
             F.stddev("trip_distance").alias("std_distance"))
        # índice contínuo de mês -> rangeBetween respeita meses em falta
        .withColumn("month_idx", F.col("year") * 12 + F.col("month")))

    # 1) rank por volume dentro do borough, em cada mês
    w_rank = Window.partitionBy("Borough", "year", "month").orderBy(F.desc("trip_count"))

    # 2) quota rolante de 3 meses: viagens da zona (m-2..m) / viagens do borough (m-2..m)
    w_zone_3m = Window.partitionBy("PULocationID").orderBy("month_idx").rangeBetween(-2, 0)
    borough_month = (zone_month.groupBy("Borough", "month_idx")
                     .agg(F.sum("trip_count").alias("borough_count")))
    w_boro_3m = Window.partitionBy("Borough").orderBy("month_idx").rangeBetween(-2, 0)
    borough_roll = borough_month.withColumn(
        "borough_3m", F.sum("borough_count").over(w_boro_3m)).drop("borough_count")

    # 3) desvio face à média rolante da própria zona (3 meses anteriores)
    w_prev = Window.partitionBy("PULocationID").orderBy("month_idx").rangeBetween(-3, -1)

    m3 = (zone_month
        .withColumn("rank_in_borough", F.rank().over(w_rank))
        .withColumn("zone_3m", F.sum("trip_count").over(w_zone_3m))
        .join(borough_roll, ["Borough", "month_idx"])
        .withColumn("share_3m", F.col("zone_3m") / F.col("borough_3m"))
        .withColumn("roll_mean", F.avg("trip_count").over(w_prev))
        .withColumn("roll_std", F.stddev("trip_count").over(w_prev))
        .withColumn("volume_anomaly",
                    F.coalesce(F.abs(F.col("trip_count") - F.col("roll_mean"))
                               > 1.5 * F.col("roll_std"), F.lit(False))))

    m3.write.mode("overwrite").parquet(os.path.join(out, "m3_zone_stats"))
    return m3


# ---------------------------------------------------------------------------
# O1 — Deteção de anomalias de tarifa
# ---------------------------------------------------------------------------
def run_o1(trips, zones, out):
    # trip_distance está em milhas; o enunciado fala em "per-km",
    # convertemos para km (1 milha = 1.609344 km)
    base = (trips
        .withColumn("hour_of_day", F.hour("tpep_pickup_datetime"))
        .withColumn("is_weekend", F.dayofweek("tpep_pickup_datetime").isin(1, 7))
        .withColumn("fare_per_km", F.col("fare_amount") / (F.col("trip_distance") * 1.609344)))

    bucket_stats = (base.groupBy("PULocationID", "hour_of_day")
                    .agg(F.avg("fare_per_km").alias("fpk_mean"),
                         F.stddev("fare_per_km").alias("fpk_std")))

    flagged = (base
        .join(bucket_stats, ["PULocationID", "hour_of_day"])
        .withColumn("is_anomaly",
                    F.coalesce(F.col("fare_per_km") > F.col("fpk_mean") + 3 * F.col("fpk_std"),
                               F.lit(False))))

    o1 = (flagged
        .join(zones, flagged.PULocationID == zones.LocationID, "left")
        .withColumn("pickup_borough", F.col("Borough"))
        .groupBy("pickup_borough", "payment_type", "is_weekend")
        .agg(F.count("*").alias("trips"),
             F.sum(F.col("is_anomaly").cast("int")).alias("anomalies"),
             F.avg(F.col("is_anomaly").cast("int")).alias("anomaly_rate")))

    o1.write.mode("overwrite").parquet(os.path.join(out, "o1_fare_anomalies"))
    return o1


# ---------------------------------------------------------------------------
# O2 — Fluxos origem-destino
# ---------------------------------------------------------------------------
def run_o2(trips, zones, out):
    pu = zones.select(F.col("LocationID").alias("PULocationID"),
                      F.col("Borough").alias("pickup_borough"),
                      F.col("Zone").alias("pickup_zone"))
    do = zones.select(F.col("LocationID").alias("DOLocationID"),
                      F.col("Borough").alias("dropoff_borough"),
                      F.col("Zone").alias("dropoff_zone"))

    # contagens mensais por par (PU, DO) + join duplo à tabela de zonas
    od = (trips
        .withColumn("year", F.year("tpep_pickup_datetime"))
        .withColumn("month", F.month("tpep_pickup_datetime"))
        .groupBy("PULocationID", "DOLocationID", "year", "month")
        .agg(F.count("*").alias("trips"))
        .join(pu, "PULocationID", "left")
        .join(do, "DOLocationID", "left"))

    # quota da rota no total de saídas da zona de pickup, por mês
    w_out = Window.partitionBy("PULocationID", "year", "month")
    od = od.withColumn("share", F.col("trips") / F.sum("trips").over(w_out))
    od.write.mode("overwrite").parquet(os.path.join(out, "o2_od_monthly"))

    # top-20 rotas por borough de pickup (volume total no período)
    route_tot = (od.groupBy("pickup_borough", "PULocationID", "pickup_zone",
                            "DOLocationID", "dropoff_zone")
                 .agg(F.sum("trips").alias("total_trips")))
    w_top = Window.partitionBy("pickup_borough").orderBy(F.desc("total_trips"))
    top20 = (route_tot.withColumn("rank", F.row_number().over(w_top))
             .filter(F.col("rank") <= 20))
    top20.write.mode("overwrite").parquet(os.path.join(out, "o2_top20_routes"))

    # inversão de tendência: direção da quota no Q1 (mar - jan) vs Q3 (set - jul)
    # NOTA: precisa de dados de jan, mar, jul e set do mesmo ano.
    key = ["PULocationID", "DOLocationID", "year"]
    s = od.groupBy(*key).agg(
        F.max(F.when(F.col("month") == 1, F.col("share"))).alias("s_jan"),
        F.max(F.when(F.col("month") == 3, F.col("share"))).alias("s_mar"),
        F.max(F.when(F.col("month") == 7, F.col("share"))).alias("s_jul"),
        F.max(F.when(F.col("month") == 9, F.col("share"))).alias("s_sep"))
    reversals = (s
        .withColumn("q1_trend", F.signum(F.col("s_mar") - F.col("s_jan")))
        .withColumn("q3_trend", F.signum(F.col("s_sep") - F.col("s_jul")))
        .filter((F.col("q1_trend") * F.col("q3_trend")) < 0))
    reversals.write.mode("overwrite").parquet(os.path.join(out, "o2_q1_q3_reversals"))
    return od, top20, reversals


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main(argv=None):
    args = parse_args(argv)
    spark = build_spark(args)
    print("Spark", spark.version, "| master:", spark.sparkContext.master,
          "| UI:", spark.sparkContext.uiWebUrl)

    files = list_files(args.input, args.years, args.months)
    print(f"{len(files)} ficheiros:", [os.path.basename(p) for p, _ in files])

    timer = StepTimer(spark)
    zones = load_zones(spark, args.input)
    raw = load_raw_trips(spark, files, args.fraction)
    trips = clean(raw)   # sem cache: baseline

    t0 = time.time()
    if "M1" in args.ops:
        with timer.step("M1"):
            run_m1(raw, args.output)
    if "M2" in args.ops:
        with timer.step("M2"):
            run_m2(trips, zones, args.output)
    if "M3" in args.ops:
        with timer.step("M3"):
            run_m3(trips, zones, args.output)
    if "O1" in args.ops:
        with timer.step("O1"):
            run_o1(trips, zones, args.output)
    if "O2" in args.ops:
        with timer.step("O2"):
            run_o2(trips, zones, args.output)
    total = time.time() - t0
    timer.results.append({"step": "TOTAL", "wall_time_s": round(total, 2)})
    print(f"\nTOTAL: {total:.2f}s")

    save_metrics(timer.results,
                 args.metrics_file or os.path.join(args.output, "metrics.csv"),
                 args, spark)
    spark.stop()


if __name__ == "__main__":
    main()
