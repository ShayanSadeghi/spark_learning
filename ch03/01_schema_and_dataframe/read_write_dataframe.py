from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("fireDataFrame").getOrCreate()
    fire_schema = StructType(
        [
            StructField("IncidentNumber", IntegerType(), True),
            StructField("ExposureNumber", IntegerType(), True),
            StructField("RowID", StringType(), True),
            StructField("Address", StringType(), True),
            StructField("IncidentDate", StringType(), True),
            StructField("CallNumber", IntegerType(), True),
            StructField("AlarmDtTm ", StringType(), True),
            StructField("ArrivalDtTm ", StringType(), True),
            StructField("CloseDtTm  ", StringType(), True),
            StructField("City", StringType(), True),
            StructField("Zipcode", IntegerType(), True),
            StructField("Battalion", StringType(), True),
            StructField("StationArea", StringType(), True),
            StructField("Box", StringType(), True),
            StructField("SuppressionUnits", StringType(), True),
            StructField("SuppressionPersonnel", StringType(), True),
            StructField("EMSUnits", StringType(), True),
            StructField("EMSPersonnel", StringType(), True),
            StructField("OtherUnits", StringType(), True),
            StructField("OtherPersonnel", StringType(), True),
            StructField("FirstUnitOnScene", StringType(), True),
            StructField("EstimatedPropertyLoss", StringType(), True),
            StructField("EstimatedContentsLoss", StringType(), True),
            StructField("FireFatalities", StringType(), True),
            StructField("FireInjuries", StringType(), True),
            StructField("CivilianFatalities", StringType(), True),
            StructField("CivilianInjuries", StringType(), True),
            StructField("NumAlarms", IntegerType(), True),
            StructField("PrimarySituation", StringType(), True),
            StructField("MutualAid", StringType(), True),
            StructField("ActionTakenPrimary", StringType(), True),
            StructField("ActionTakenSecondary", StringType(), True),
            StructField("ActionTakenOther", StringType(), True),
            StructField("DetectorAlertedOccupants", StringType(), True),
            StructField("PropertyUse", StringType(), True),
            StructField("AreaOfFireOrigin", StringType(), True),
            StructField("IgnitionCause", StringType(), True),
            StructField("IgnitionFactorPrimary", StringType(), True),
            StructField("Ignition Factor Secondary", StringType(), True),
            StructField("HeatSource", StringType(), True),
            StructField("ItemFirstIgnited", StringType(), True),
            StructField("HumanFactorsAssociatedWithIgnition", StringType(), True),
            StructField("StructureType", StringType(), True),
            StructField("StructureStatus", StringType(), True),
            StructField("FloorOfFireOrigin", StringType(), True),
            StructField("FireSpread", StringType(), True),
            StructField("NoFlameSpread", StringType(), True),
            StructField("NumberOfFloorsWithMinimumDamage", StringType(), True),
            StructField("NumberOfFloorsWithSignificantDamage", StringType(), True),
            StructField("NumberOfFloorsWithHeavyDamage", StringType(), True),
            StructField("NumberOfFloorsWithExtremeDamage", StringType(), True),
            StructField("Detectors Present", StringType(), True),
            StructField("DetectorType", StringType(), True),
            StructField("Detector Operation", StringType(), True),
            StructField("Detector Effectiveness", StringType(), True),
            StructField("Detector Failure Reason", StringType(), True),
            StructField("Automatic Extinguishing System Present", StringType(), True),
            StructField("Automatic Extinguishing System Type", StringType(), True),
            StructField(
                "Automatic Extinguishing System Performance", StringType(), True
            ),
            StructField(
                "Automatic Extinguishing System Failure Reason", StringType(), True
            ),
            StructField("Number of Sprinkler Heads Operating", StringType(), True),
            StructField(
                "Supervisor District|neighborhood_district", StringType(), True
            ),
            StructField("point", StringType(), True),
        ]
    )

    file_fire = "./large_files/Fire_Incidents.csv"
    fire_df = spark.read.csv(file_fire, header=True, schema=fire_schema)

    few_fire_df = fire_df.select(
        "IncidentNumber", "IncidentDate", "PrimarySituation"
    ).where(col("PrimarySituation") != "Medical Incident")

    # fire_df.write.format("parquet").save("./fire_df.parquet")
    few_fire_df.show(5, truncate=False)

    # few_fire_df.write.format("parquet").saveAsTable("few_fire_df")

    fire_df.select("PrimarySituation").where(col("PrimarySituation").isNotNull()).agg(
        countDistinct("PrimarySituation").alias("DistinctPrimarySituations")
    ).show()

    fire_df.select("PrimarySituation").where(
        col("PrimarySituation").isNotNull()
    ).distinct().show(10, False)

    new_fire_df = fire_df.withColumnRenamed("City", "city")
    new_fire_df.select("city", "StationArea").where(col("FireInjuries") < 5).show(
        10, False
    )
