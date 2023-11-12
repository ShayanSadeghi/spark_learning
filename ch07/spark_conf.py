from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.config("spark.sql.shuffle.partitions", 5)
    .appName("view_set_conf")
    .getOrCreate()
)


spark.sql("SET -v").select("key", "value").filter(
    "key LIKE 'spark.sql.shuffle.partitions'"
).show()
