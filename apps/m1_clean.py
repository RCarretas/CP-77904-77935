"""M1 — limpeza dos dados e estatísticas de limpeza por ano."""
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingest import TRIP_SCHEMA

PICKUP = "tpep_pickup_datetime"
DROPOFF = "tpep_dropoff_datetime"

# Cada critério é uma expressão booleana que é True quando o registo é inválido.
# Os critérios são avaliados de forma independente: um registo que falha dois
# critérios conta nos dois. "null_values" é um critério extra, porque num filter
# uma comparação com null dá null e o registo desapareceria sem ser contado.
# "pickup_outside_file_month" também é extra: os ficheiros do TLC trazem algumas
# viagens com datas de outros meses/anos, que estragariam as séries mensais do M3.
def criteria():
    pickup, dropoff = F.col(PICKUP), F.col(DROPOFF)
    return {
        "null_values": reduce(lambda a, b: a | b, [F.col(c).isNull() for c in TRIP_SCHEMA]),
        "fare_amount_le_0": F.col("fare_amount") <= 0,
        "trip_distance_le_0": F.col("trip_distance") <= 0,
        "passenger_count_0_or_gt_6": (F.col("passenger_count") == 0) | (F.col("passenger_count") > 6),
        "dropoff_le_pickup": dropoff <= pickup,
        "location_out_of_range": ~F.col("PULocationID").between(1, 263)
                                 | ~F.col("DOLocationID").between(1, 263),
        "pickup_outside_file_month": (F.year(pickup) != F.col("source_year"))
                                     | (F.month(pickup) != F.col("source_month")),
    }


def _flag(expr):
    # null conta como "não falha este critério" (os nulos já são contados à parte)
    return F.coalesce(expr, F.lit(False))


def cleaning_stats(trips: DataFrame) -> DataFrame:
    """Número de registos removidos por critério, por ano (ano do ficheiro)."""
    invalid = F.lit(False)
    for expr in criteria().values():
        invalid = invalid | _flag(expr)

    aggs = [F.count(F.lit(1)).alias("total_records")]
    aggs += [F.sum(F.when(_flag(expr), 1).otherwise(0)).alias(name) for name, expr in criteria().items()]
    aggs += [F.sum(F.when(invalid, 1).otherwise(0)).alias("removed_any_criterion")]

    stats = trips.groupBy(F.col("source_year").alias("year")).agg(*aggs)
    return (stats
            .withColumn("valid_records", F.col("total_records") - F.col("removed_any_criterion"))
            .orderBy("year"))


def clean(trips: DataFrame) -> DataFrame:
    """Remove os registos inválidos e acrescenta as colunas derivadas usadas
    pelas operações seguintes."""
    valid = trips
    for expr in criteria().values():
        valid = valid.filter(~_flag(expr))

    pickup, dropoff = F.col(PICKUP), F.col(DROPOFF)
    return (valid
            .withColumn("year", F.year(pickup))
            .withColumn("month", F.month(pickup))
            .withColumn("day_of_week", F.dayofweek(pickup))  # 1 = domingo ... 7 = sábado
            .withColumn("hour_of_day", F.hour(pickup))
            .withColumn("duration_min",
                        (F.unix_timestamp(dropoff) - F.unix_timestamp(pickup)) / 60.0))


def run(trips: DataFrame, output_path: str) -> DataFrame:
    stats = cleaning_stats(trips)
    stats.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    return stats
