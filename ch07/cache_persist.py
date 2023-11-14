import time
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

spark = SparkSession.builder.appName("testing_cache_persist").getOrCreate()


print("---------PERSIST------------")
df = spark.range(1, 10000000).toDF("id")
t1 = time.time()
df.persist(StorageLevel.DISK_ONLY)
df.count()
t2 = time.time()
print(t2 - t1)

df.count()
t3 = time.time()
print(t3 - t2)

print("---------CACHE------------")
df2 = spark.range(1, 10000000).toDF("id")
t4 = time.time()
df2.cache()
df2.count()
t5 = time.time()
print(t5 - t4)

df2.count()
t6 = time.time()
print(t6 - t5)
