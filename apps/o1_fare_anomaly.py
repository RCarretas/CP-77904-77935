"""O1 — deteção de anomalias na tarifa por km."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

KM_PER_MILE = 1.609344
SIGMA_THRESHOLD = 3.0


def compute(trips: DataFrame, zones: DataFrame):
    trips = trips.withColumn("fare_per_km", F.col("fare_amount") / (F.col("trip_distance") * KM_PER_MILE))

    # média e desvio-padrão por (zona de pickup, hora)
    buckets = (trips
               .groupBy("PULocationID", "hour_of_day")
               .agg(F.count(F.lit(1)).alias("trip_count"),
                    F.avg("fare_per_km").alias("mean_fare_per_km"),
                    F.stddev("fare_per_km").alias("std_fare_per_km")))

    pickup_zones = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pickup_borough"),
    )

    # junta as estatísticas do bucket de volta a cada viagem e marca as anomalias
    flagged = (trips
               .join(buckets.drop("trip_count"), on=["PULocationID", "hour_of_day"])
               .withColumn("is_anomaly", F.coalesce(
                   F.col("fare_per_km") > F.col("mean_fare_per_km") + SIGMA_THRESHOLD * F.col("std_fare_per_km"),
                   F.lit(False)))
               .withColumn("day_type", F.when(F.col("day_of_week").isin(1, 7), "weekend").otherwise("weekday"))
               .join(pickup_zones, on="PULocationID"))

    rates = (flagged
             .groupBy("day_type", "payment_type", "pickup_borough")
             .agg(F.count(F.lit(1)).alias("trip_count"),
                  F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"))
             .withColumn("anomaly_rate", F.col("anomaly_count") / F.col("trip_count")))

    # dias úteis vs fim de semana lado a lado
    comparison = (rates
                  .groupBy("payment_type", "pickup_borough")
                  .pivot("day_type", ["weekday", "weekend"])
                  .agg(F.first("anomaly_rate"))
                  .withColumnRenamed("weekday", "weekday_anomaly_rate")
                  .withColumnRenamed("weekend", "weekend_anomaly_rate"))

    return buckets, rates, comparison


def run(trips: DataFrame, zones: DataFrame, output_path: str) -> None:
    buckets, rates, comparison = compute(trips, zones)
    buckets.write.mode("overwrite").parquet(f"{output_path}/bucket_stats")
    rates.write.mode("overwrite").parquet(f"{output_path}/anomaly_rates")
    comparison.write.mode("overwrite").parquet(f"{output_path}/weekday_vs_weekend")
