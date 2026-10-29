"""M2 — procura por (borough de pickup, dia da semana, hora do dia)."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute(trips: DataFrame, zones: DataFrame) -> DataFrame:
    pickup_zones = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pickup_borough"),
    )
    return (trips
            .join(pickup_zones, on="PULocationID")
            .groupBy("pickup_borough", "day_of_week", "hour_of_day")
            .agg(F.count(F.lit(1)).alias("trip_count"),
                 F.avg("fare_amount").alias("mean_fare_amount"),
                 F.avg("duration_min").alias("mean_duration_min"),
                 F.avg("trip_distance").alias("mean_trip_distance")))


def run(trips: DataFrame, zones: DataFrame, output_path: str) -> None:
    result = compute(trips, zones)
    result.write.mode("overwrite").partitionBy("pickup_borough").parquet(output_path)
