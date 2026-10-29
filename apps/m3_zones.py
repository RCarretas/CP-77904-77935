"""M3 — estatísticas por zona de pickup e mês, com funções de janela."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

DEVIATION_THRESHOLD = 1.5  # desvios-padrão


def compute(trips: DataFrame, zones: DataFrame) -> DataFrame:
    pickup_zones = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
    )

    monthly = (trips
               .groupBy("PULocationID", "year", "month")
               .agg(F.count(F.lit(1)).alias("trip_count"),
                    F.avg("fare_amount").alias("mean_fare_amount"),
                    F.stddev("fare_amount").alias("std_fare_amount"),
                    F.avg("trip_distance").alias("mean_trip_distance"),
                    F.stddev("trip_distance").alias("std_trip_distance"))
               .join(pickup_zones, on="PULocationID")
               # índice contínuo de meses, para as janelas atravessarem anos
               .withColumn("month_idx", F.col("year") * 12 + F.col("month") - 1))

    # 1) ranking das zonas por volume mensal dentro de cada borough
    by_borough_month = Window.partitionBy("pickup_borough", "month_idx").orderBy(F.desc("trip_count"))
    monthly = monthly.withColumn("volume_rank_in_borough", F.rank().over(by_borough_month))

    # 2) quota móvel de 3 meses (mês atual + 2 anteriores) da zona dentro do borough
    zone_last_3 = Window.partitionBy("PULocationID").orderBy("month_idx").rangeBetween(-2, 0)
    monthly = monthly.withColumn("zone_trips_3m", F.sum("trip_count").over(zone_last_3))

    borough_last_3 = Window.partitionBy("pickup_borough").orderBy("month_idx").rangeBetween(-2, 0)
    borough_monthly = (monthly
                       .groupBy("pickup_borough", "month_idx")
                       .agg(F.sum("trip_count").alias("borough_trip_count"))
                       .withColumn("borough_trips_3m", F.sum("borough_trip_count").over(borough_last_3)))

    monthly = (monthly
               .join(borough_monthly, on=["pickup_borough", "month_idx"])
               .withColumn("rolling_3m_share", F.col("zone_trips_3m") / F.col("borough_trips_3m")))

    # 3) desvio face à média móvel da própria zona (3 meses anteriores, sem o atual)
    zone_previous_3 = Window.partitionBy("PULocationID").orderBy("month_idx").rangeBetween(-3, -1)
    monthly = (monthly
               .withColumn("rolling_mean_trips", F.avg("trip_count").over(zone_previous_3))
               .withColumn("rolling_std_trips", F.stddev("trip_count").over(zone_previous_3))
               .withColumn("volume_deviation_flag", F.coalesce(
                   F.abs(F.col("trip_count") - F.col("rolling_mean_trips"))
                   > DEVIATION_THRESHOLD * F.col("rolling_std_trips"),
                   F.lit(False))))

    return monthly


def run(trips: DataFrame, zones: DataFrame, output_path: str) -> None:
    compute(trips, zones).write.mode("overwrite").parquet(output_path)
