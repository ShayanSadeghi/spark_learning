from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession.builder.appName("fireDataFrame").getOrCreate()
    fire_schema = StructType(
        [
            StructField("CallNumber", IntegerType(), True),
            StructField("UnitID", StringType(), True),
            StructField("IncidentNumber", IntegerType(), True),
            StructField("CallType", StringType(), True),
            StructField("CallDate", StringType(), True),
            StructField("WatchDate", StringType(), True),
            StructField("CallFinalDisposition", StringType(), True),
            StructField("AvailableDtTm", StringType(), True),
            StructField("Address", StringType(), True),
            StructField("City", StringType(), True),
            StructField("Zipcode", IntegerType(), True),
            StructField("Battalion", StringType(), True),
            StructField("StationArea", StringType(), True),
            StructField("Box", StringType(), True),
            StructField("OriginalPriority", StringType(), True),
            StructField("Priority", StringType(), True),
            StructField("FinalPriority", IntegerType(), True),
            StructField("ALSUnit", BooleanType(), True),
            StructField("CallTyperGroup", StringType(), True),
            StructField("NumAlarms", IntegerType(), True),
            StructField("UnitType", StringType(), True),
            StructField("UnitSequenceInCallDispatch", IntegerType(), True),
            StructField("FirePreventionDistrict", StringType(), True),
            StructField("SupervisorDistrict", StringType(), True),
            StructField("Neighborhood", StringType(), True),
            StructField("Location", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("Delay", FloatType(), True),
        ]
    )

    file_fire = "./large_files/Fire_Incidents.csv"
    fire_df = spark.read.csv(file_fire, header=True, schema=fire_schema)

    few_fire_df = fire_df.select("IncidentNumber", "AvailableDtTm", "CallType").where(
        col("CallType") != "Medical Incident"
    )
    fire_df.write.format("parquet").save("./fire_df.parquet")
    few_fire_df.show(5, truncate=False)

    few_fire_df.write.format("parquet").saveAsTable("few_fire_df")
