#!/usr/bin/env python
# coding: utf-8

# # Projeto 1 — NYC Taxi com Spark (baseline)
# 
# **Baseline:** implementação direta, sem tuning (gerada com IA, ver acknowledgements). De propósito:
# - sem `cache`/`persist` do DataFrame limpo → cada operação relê e relimpa os Parquet
# - sem `select` de colunas (column pruning manual)
# - sem `broadcast()` explícito nos joins com a tabela de zonas
# - `spark.sql.shuffle.partitions` e AQE com os valores por defeito
# 
# Operações: **M1, M2, M3** + **O1, O2**.
# 
# **Correr no Jupyter:** muda os valores por defeito na célula de parâmetros e faz *Run All*.
# 
# **Correr com spark-submit (cluster):** converte o notebook em script e passa os parâmetros:
# ```bash
# jupyter nbconvert --to script main.ipynb        # gera main.py
# spark-submit --master spark://spark-master:7077 main.py --years 2018 2019 2020 --fraction 0.1 --run-name baseline-10pct
# ```

# ## Parâmetros

# In[ ]:


import sys, argparse

p = argparse.ArgumentParser(description="NYC Taxi Spark pipeline")
p.add_argument("--master", default="spark://spark-master:7077",
               help="ex.: spark://spark-master:7077 ou local[*]")
p.add_argument("--input", default="/data",
               help="pasta com yellow_tripdata_YYYY-MM.parquet e taxi_zone_lookup.csv")
p.add_argument("--output", default="/data/output")
p.add_argument("--years", type=int, nargs="+", default=[2018, 2019, 2020])
p.add_argument("--months", type=int, nargs="+", default=list(range(1, 10)),
               help="ficheiros que não existam são ignorados")
p.add_argument("--fraction", type=float, default=1.0,
               help="fração dos registos (ex.: 0.1)")
p.add_argument("--ops", nargs="+", default=["M1", "M2", "M3", "O1", "O2"],
               choices=["M1", "M2", "M3", "O1", "O2"])
p.add_argument("--run-name", default="baseline",
               help="etiqueta desta run no metrics.csv")
p.add_argument("--event-log-dir", default=None,
               help="se dado, ativa spark.eventLog (History Server)")

# No Jupyter usa os defaults acima; como script (spark-submit) lê a linha de comando
IN_NOTEBOOK = "ipykernel" in sys.modules
args = p.parse_args([] if IN_NOTEBOOK else None)
args


# ## Setup

# In[ ]:


import os, time, csv
from functools import reduce

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, IntegerType,
                               DoubleType, StringType, TimestampType)

builder = (SparkSession.builder
           .master(args.master)
           .appName(f"nyc-taxi-{args.run_name}")
           .config("spark.driver.memory", "2g")
           .config("spark.executor.memory", "1536m")   # abaixo do --memory 2G do worker
           .config("spark.ui.showConsoleProgress", "false"))
if args.event_log_dir:
    builder = (builder.config("spark.eventLog.enabled", "true")
                      .config("spark.eventLog.dir", args.event_log_dir))
spark = builder.getOrCreate()

print("Spark", spark.version, "| master:", spark.sparkContext.master,
      "| UI:", spark.sparkContext.uiWebUrl,
      "| cores:", spark.sparkContext.defaultParallelism)


# In[ ]:


sys.path.append("/workspace/apps")
from spark_profiler import SparkProfiler
profiler = SparkProfiler(spark)


# ## M1 — Ingestão e limpeza
# O schema muda entre ficheiros (ex.: `passenger_count` double vs long), por isso lê-se mês a mês e faz-se cast para um schema fixo.

# In[ ]:


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


def to_target_schema(df, schema=TARGET_SCHEMA):
    """Cast de cada coluna para o tipo alvo; colunas em falta ficam null."""
    existing = set(df.columns)
    return df.select([
        (F.col(f.name) if f.name in existing else F.lit(None)).cast(f.dataType).alias(f.name)
        for f in schema.fields
    ])


files = [(os.path.join(args.input, f"yellow_tripdata_{y}-{m:02d}.parquet"), y)
         for y in args.years for m in args.months]
files = [(p_, y) for p_, y in files if os.path.exists(p_)]
assert files, "Nenhum ficheiro encontrado"
print(len(files), "ficheiros:", [os.path.basename(p_) for p_, _ in files])

zones = spark.read.csv(os.path.join(args.input, "taxi_zone_lookup.csv"),
                       header=True, schema=ZONES_SCHEMA)

raw = reduce(lambda a, b: a.unionByName(b),
             [to_target_schema(spark.read.parquet(p_)).withColumn("file_year", F.lit(y))
              for p_, y in files])
if args.fraction < 1.0:
    raw = raw.sample(withReplacement=False, fraction=args.fraction, seed=42)


# In[ ]:


tamanho = 0
for path, ano in files:
    tamanho += os.path.getsize(path)
print("tamanho:", tamanho)


# In[ ]:


def out_of_range(c):
    return (F.col(c) < 1) | (F.col(c) > 263)

# Critérios do enunciado + 2 extra (nulls; ano do pickup != ano do ficheiro,
# ex.: viagens com data de 2009 em ficheiros de 2018).
# coalesce(..., False): comparações com null dão null -> não contam nesse critério
# (os nulls já são apanhados por 'null_values').
INVALID = {
    "null_values":           reduce(lambda a, b: a | b, [F.col(c).isNull() for c in NULL_COLS]),
    "fare_amount_le_0":      F.col("fare_amount") <= 0,
    "trip_distance_le_0":    F.col("trip_distance") <= 0,
    "passenger_count_0_gt6": (F.col("passenger_count") == 0) | (F.col("passenger_count") > 6),
    "dropoff_le_pickup":     F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"),
    "location_out_of_range": out_of_range("PULocationID") | out_of_range("DOLocationID"),
    "year_mismatch":         F.year("tpep_pickup_datetime") != F.col("file_year"),
}
BAD = {n: F.coalesce(c, F.lit(False)) for n, c in INVALID.items()}
ANY_BAD = reduce(lambda a, b: a | b, BAD.values())

# DataFrame limpo — baseline:
trips = raw.filter(~ANY_BAD).drop("file_year")




# In[ ]:


with profiler.profile("M1"):
    # por ano: total, mantidos e nº que falha cada critério (podem sobrepor-se)
    aggs = [F.count("*").alias("total"), F.sum((~ANY_BAD).cast("int")).alias("kept")]
    aggs += [F.sum(c.cast("int")).alias(f"removed_{n}") for n, c in BAD.items()]
    m1 = raw.groupBy("file_year").agg(*aggs).orderBy("file_year")

    m1_pdf = m1.toPandas()
    m1_pdf.to_csv(os.path.join(args.output, "m1_cleaning_stats.csv"), index=False)

m1_pdf.set_index("file_year").T


# ## M2 — Procura temporal
# Por (pickup_borough, day_of_week, hour_of_day): nº de viagens, tarifa média, duração média (min), distância média. Escrito em Parquet particionado por borough.

# In[ ]:


with profiler.profile("M2"):
    m2 = (trips
          .join(zones, trips.PULocationID == zones.LocationID)
          .withColumn("pickup_borough", F.col("Borough"))
          .withColumn("day_of_week", F.dayofweek("tpep_pickup_datetime"))
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
        .parquet(os.path.join(args.output, "m2_temporal_demand"))
m2.explain(mode="formatted")


# In[ ]:





# ## M3 — Estatísticas por zona
# Por zona e mês: média e desvio-padrão de fare e distância. Windows: rank no borough, quota rolante de 3 meses no borough, e flag quando o volume se afasta mais de 1.5σ da média rolante da própria zona (3 meses anteriores).

# In[ ]:


with profiler.profile("M3"):
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

    # 2) quota rolante 3 meses = viagens da zona (m-2..m) / viagens do borough (m-2..m)
    w_zone_3m = Window.partitionBy("PULocationID").orderBy("month_idx").rangeBetween(-2, 0)
    w_boro_3m = Window.partitionBy("Borough").orderBy("month_idx").rangeBetween(-2, 0)
    borough_roll = (zone_month.groupBy("Borough", "month_idx")
                    .agg(F.sum("trip_count").alias("borough_count"))
                    .withColumn("borough_3m", F.sum("borough_count").over(w_boro_3m))
                    .drop("borough_count"))

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

    m3.write.mode("overwrite").parquet(os.path.join(args.output, "m3_zone_stats"))

    m3.explain()


# ## O1 — Anomalias de tarifa
# Por (PULocationID, hour_of_day): média e σ de fare/km; flag quando fare/km > média + 3σ; taxa de anomalias dia útil vs fim de semana por payment_type e borough.

# In[ ]:


with profiler.profile("O1"):
    # trip_distance está em milhas -> converter para km
    o1_base = (trips
        .withColumn("hour_of_day", F.hour("tpep_pickup_datetime"))
        .withColumn("is_weekend", F.dayofweek("tpep_pickup_datetime").isin(1, 7))
        .withColumn("fare_per_km", F.col("fare_amount") / (F.col("trip_distance") * 1.609344)))

    bucket_stats = (o1_base.groupBy("PULocationID", "hour_of_day")
                    .agg(F.avg("fare_per_km").alias("fpk_mean"),
                         F.stddev("fare_per_km").alias("fpk_std")))

    flagged = (o1_base
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

    o1.write.mode("overwrite").parquet(os.path.join(args.output, "o1_fare_anomalies"))


# ## O2 — Fluxos origem-destino
# Contagens mensais por par (PU, DO) com join duplo às zonas; quota de cada rota nas saídas da zona; top-20 rotas por borough; rotas cuja quota inverte a tendência entre Q1 (jan→mar) e Q3 (jul→set).
# 
# ⚠️ A parte Q1 vs Q3 precisa de dados de julho e setembro.

# In[ ]:


with profiler.profile("O2"):
    pu = zones.select(F.col("LocationID").alias("PULocationID"),
                      F.col("Borough").alias("pickup_borough"),
                      F.col("Zone").alias("pickup_zone"))
    do = zones.select(F.col("LocationID").alias("DOLocationID"),
                      F.col("Borough").alias("dropoff_borough"),
                      F.col("Zone").alias("dropoff_zone"))

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
    od.write.mode("overwrite").parquet(os.path.join(args.output, "o2_od_monthly"))

    # top-20 rotas por borough de pickup (volume total no período)
    w_top = Window.partitionBy("pickup_borough").orderBy(F.desc("total_trips"))
    top20 = (od.groupBy("pickup_borough", "PULocationID", "pickup_zone",
                        "DOLocationID", "dropoff_zone")
               .agg(F.sum("trips").alias("total_trips"))
               .withColumn("rank", F.row_number().over(w_top))
               .filter(F.col("rank") <= 20))
    top20.write.mode("overwrite").parquet(os.path.join(args.output, "o2_top20_routes"))

    # inversão de tendência: sinal(mar - jan) vs sinal(set - jul), por rota e ano
    reversals = (od.groupBy("PULocationID", "DOLocationID", "year")
        .agg(F.max(F.when(F.col("month") == 1, F.col("share"))).alias("s_jan"),
             F.max(F.when(F.col("month") == 3, F.col("share"))).alias("s_mar"),
             F.max(F.when(F.col("month") == 7, F.col("share"))).alias("s_jul"),
             F.max(F.when(F.col("month") == 9, F.col("share"))).alias("s_sep"))
        .withColumn("q1_trend", F.signum(F.col("s_mar") - F.col("s_jan")))
        .withColumn("q3_trend", F.signum(F.col("s_sep") - F.col("s_jul")))
        .filter(F.col("q1_trend") * F.col("q3_trend") < 0))
    reversals.write.mode("overwrite").parquet(os.path.join(args.output, "o2_q1_q3_reversals"))


# ## Métricas da run
# Acrescenta uma linha por operação a `metrics.csv` (para comparar baseline vs otimizações e as runs de escalabilidade).

# In[ ]:


rep = profiler.report()
print(rep.to_string())

conf = {"run_name": args.run_name,
        "years": " ".join(map(str, args.years)),
        "fraction": args.fraction,
        "master": spark.sparkContext.master,
        "default_parallelism": spark.sparkContext.defaultParallelism,
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "aqe": spark.conf.get("spark.sql.adaptive.enabled")}

metrics_path = os.path.join(args.output, "metrics.csv")
rows = [{**conf, "step": name, **m} for name, m in profiler.runs.items()]
rows.append({**conf, "step": "TOTAL",
             "wall_time_s": round(sum(m["wall_time_s"] for m in profiler.runs.values()), 2)})
# pandas junta as colunas novas do profiler às runs antigas (que ficam vazias nessas colunas)
import pandas as pd
new_df = pd.DataFrame(rows)
if os.path.exists(metrics_path):
    new_df = pd.concat([pd.read_csv(metrics_path), new_df], ignore_index=True)
new_df.to_csv(metrics_path, index=False)
print("Métricas acrescentadas a", metrics_path)


# ### Distribuição do trabalho (para comparar com a versão otimizada)
# - `read_tasks_empty`: tarefas de leitura que leram 0 linhas
# - `read_task_median_ms` vs `read_task_max_ms`: desequilíbrio da leitura
# - `shuffle_skew_max`: maior partição de shuffle / mediana (1 = equilibrado)
# - `files_read_bytes`: tamanho dos ficheiros abertos (o `input_bytes` vem subcontado com parquet local)

# In[ ]:


cols = ["wall_time_s", "tasks_executed", "read_tasks", "read_tasks_empty",
        "read_task_median_ms", "read_task_max_ms", "files_read", "files_read_bytes",
        "input_records", "shuffle_write_bytes", "shuffle_skew_max",
        "memory_spilled_bytes", "disk_spilled_bytes", "gc_time_ms", "executor_run_time_ms"]
rep[[c for c in cols if c in rep.columns]]


# ## Planos de execução (para o relatório)
# Procurar: `SortMergeJoin` vs `BroadcastHashJoin`, `Exchange`, colunas lidas no `FileScan`, nº de scans aos Parquet.

# In[ ]:


if IN_NOTEBOOK:
    m2.explain(mode="formatted")


# In[ ]:


if IN_NOTEBOOK:
    o1.explain(mode="formatted")


# In[ ]:


if IN_NOTEBOOK:
    od.explain(mode="formatted")


# ## Ver os resultados

# In[ ]:


if IN_NOTEBOOK:
    out = args.output
    spark.read.parquet(f"{out}/m2_temporal_demand").orderBy("pickup_borough", "day_of_week", "hour_of_day").show(10)
    spark.read.parquet(f"{out}/m3_zone_stats").filter("volume_anomaly").show(10)
    spark.read.parquet(f"{out}/o1_fare_anomalies").orderBy("pickup_borough", "payment_type", "is_weekend").show(10)
    spark.read.parquet(f"{out}/o2_top20_routes").orderBy("pickup_borough", "rank").show(10)


# In[ ]:


if not IN_NOTEBOOK:
    spark.stop()

