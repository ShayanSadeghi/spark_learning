from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
from pyspark.sql.types import *

# Create a Spark session
spark = SparkSession.builder.appName("WordCountStream").getOrCreate()


schema = StructType(
    [
        StructField("key", IntegerType(), False),
        StructField("value", StringType(), False),
    ]
)

# Read JSON files from the directory as a stream
json_stream_df = (
    spark.readStream.format("json").schema(schema).option("path", "files/").load()
)

# Extract the text field and split it into words
words_df = json_stream_df.select("value").withColumn(
    "word", explode(split(col("value"), " "))
)

# Group by words and count occurrences
word_count_df = words_df.groupBy("word").count()

# Start the streaming query
query = (
    word_count_df.writeStream.outputMode(
        "complete"
    )  # Specify the output mode (complete, append, or update)
    .format("console")  # You can change this to another sink (e.g., parquet, kafka)
    .start()
)

# Wait for the streaming query to finish
query.awaitTermination()
