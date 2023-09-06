import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print("add file address")
        exit()

    spark = SparkSession.builder.appName("read_json_files").getOrCreate()

    json_file = sys.argv[1]
    # define schema programmatically instead of data define language (DDL)
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("url", StringType(), False),
            StructField("published", StringType(), False),
            StructField("hits", IntegerType(), False),
            StructField("campaigns", ArrayType(StringType()), False),
        ]
    )

    blogs_df = spark.read.schema(schema).json(json_file)

    # Show the DataFrame
    blogs_df.show()

    # print the schema used by spark to process the DataFrame
    print(blogs_df.printSchema())
