from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .master("local[*]")
         .appName("smoke-test")
         .getOrCreate())

print("Spark", spark.version)
spark.range(10_000_000).selectExpr("sum(id) AS total").show()
input("Abre http://localhost:4040 e depois carrega Enter...")
spark.stop()