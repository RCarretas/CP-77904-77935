"""O2 — fluxos origem-destino."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

TOP_N = 20


def compute(trips: DataFrame, zones: DataFrame):
    pickup_zones = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Zone").alias("pickup_zone"),
        F.col("Borough").alias("pickup_borough"),
    )
    dropoff_zones = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Zone").alias("dropoff_zone"),
        F.col("Borough").alias("dropoff_borough"),
    )

    # contagens mensais por par (origem, destino), com nomes e boroughs
    routes = (trips
              .groupBy("PULocationID", "DOLocationID", "year", "month")
              .agg(F.count(F.lit(1)).alias("trip_count"))
              .join(pickup_zones, on="PULocationID")
              .join(dropoff_zones, on="DOLocationID"))

    # quota de cada rota no total de saídas da zona de pickup nesse mês
    outbound = Window.partitionBy("PULocationID", "year", "month")
    routes = routes.withColumn("outbound_share", F.col("trip_count") / F.sum("trip_count").over(outbound))

    # top-20 rotas por borough de pickup (no período todo)
    by_borough = Window.partitionBy("pickup_borough").orderBy(F.desc("total_trips"))
    top_routes = (routes
                  .groupBy("pickup_borough", "PULocationID", "pickup_zone", "DOLocationID", "dropoff_zone")
                  .agg(F.sum("trip_count").alias("total_trips"))
                  .withColumn("rank_in_borough", F.row_number().over(by_borough))
                  .filter(F.col("rank_in_borough") <= TOP_N))

    # rotas cuja tendência da quota se inverte entre Q1 e Q3:
    # tendência Q1 = quota(março) - quota(janeiro); tendência Q3 = quota(setembro) - quota(julho)
    reversals = (routes
                 .groupBy("PULocationID", "pickup_zone", "DOLocationID", "dropoff_zone", "year")
                 .pivot("month", [1, 3, 7, 9])
                 .agg(F.first("outbound_share"))
                 .withColumn("q1_trend", F.col("3") - F.col("1"))
                 .withColumn("q3_trend", F.col("9") - F.col("7"))
                 .filter(F.col("q1_trend") * F.col("q3_trend") < 0)
                 .withColumnRenamed("1", "share_jan").withColumnRenamed("3", "share_mar")
                 .withColumnRenamed("7", "share_jul").withColumnRenamed("9", "share_sep"))

    return routes, top_routes, reversals


def run(trips: DataFrame, zones: DataFrame, output_path: str) -> None:
    routes, top_routes, reversals = compute(trips, zones)
    routes.write.mode("overwrite").parquet(f"{output_path}/route_shares")
    top_routes.write.mode("overwrite").parquet(f"{output_path}/top_routes")
    reversals.write.mode("overwrite").parquet(f"{output_path}/q1_q3_reversals")
