"""Leitura dos ficheiros mensais e da tabela de zonas, com esquema imposto."""
import glob
import os
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Colunas usadas pelo pipeline e o tipo que o enunciado define para cada uma.
# Os ficheiros do TLC mudam de tipos entre anos (ex.: passenger_count vem como
# double em 2020), por isso cada ficheiro é lido e convertido separadamente.
TRIP_SCHEMA = {
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "PULocationID": "int",
    "DOLocationID": "int",
    "passenger_count": "int",
    "trip_distance": "double",
    "RatecodeID": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "tip_amount": "double",
    "total_amount": "double",
}

FILE_PATTERN = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$")


def list_input_files(inputs, years=None, months=None):
    """Devolve [(caminho, ano, mês)] a partir de pastas e/ou ficheiros,
    filtrando pelos anos e meses pedidos."""
    candidates = []
    for path in inputs:
        if os.path.isdir(path):
            candidates.extend(glob.glob(os.path.join(path, "yellow_tripdata_*.parquet")))
        else:
            candidates.append(path)

    files = []
    for path in sorted(candidates):
        match = FILE_PATTERN.search(os.path.basename(path))
        if not match:
            raise ValueError(f"Nome de ficheiro inesperado: {path}")
        year, month = int(match.group(1)), int(match.group(2))
        if years and year not in years:
            continue
        if months and month not in months:
            continue
        files.append((path, year, month))

    if not files:
        raise ValueError("Nenhum ficheiro de entrada corresponde aos filtros dados.")
    return files


def load_trips(spark: SparkSession, files, sample=1.0) -> DataFrame:
    """Lê cada ficheiro mensal, impõe o esquema e junta tudo num DataFrame.
    Acrescenta source_year/source_month (do nome do ficheiro) para o M1."""
    trips = None
    for path, year, month in files:
        df = spark.read.parquet(path)
        df = df.select(
            *[F.col(name).cast(dtype).alias(name) for name, dtype in TRIP_SCHEMA.items()]
        )
        df = df.withColumn("source_year", F.lit(year)).withColumn("source_month", F.lit(month))
        trips = df if trips is None else trips.unionByName(df)

    if sample < 1.0:
        trips = trips.sample(fraction=sample, seed=42)
    return trips


def load_zones(spark: SparkSession, path) -> DataFrame:
    """Tabela de zonas (taxi_zone_lookup.csv)."""
    zones = spark.read.csv(path, header=True)
    return zones.select(
        F.col("LocationID").cast("int").alias("LocationID"),
        F.col("Borough"),
        F.col("Zone"),
        F.col("service_zone"),
    )
