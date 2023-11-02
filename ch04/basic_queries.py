from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("SparkSQLExamplesApp").getOrCreate()

csv_file = "./large_files/airline_2m.csv"

# read and create temporary veiw
# infer schema. for larger files it's better to specify the schema
df = (
    spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(csv_file)
)

df.createOrReplaceTempView("us_delay_flights_tbl")

spark.sql(
    """ 
    SELECT Distance, Origin, DestCityName
      FROM us_delay_flights_tbl WHERE Distance > 1000
      ORDER BY Distance DESC
    """
).show(10)
# ------------------

spark.sql(
    """
    SELECT FlightDate, DepDelay, Origin, DestCityName
    FROM us_delay_flights_tbl
    WHERE DepDelay > 120 AND Origin = 'SFO' AND DestCityName LIKE 'Chicago%'
  """
).show(10)
# ------------------

spark.sql(
    """
    SELECT DepDelay, Origin, DestCityName,
      CASE
        WHEN DepDelay > 360 THEN 'Very Long Delays'
        WHEN DepDelay > 120 THEN 'Long Delays'
        WHEN DepDelay > 60 THEN 'Short delays'
        WHEN DepDelay > 0 THEN 'Tolerable Delays'
        WHEN DepDelay = 0 THEN 'No Delays'
        ELSE 'Early'
      END AS Flight_Delays
      FROM us_delay_flights_tbl
      ORDER BY Origin, DepDelay DESC
  """
).show(10)
# ------------------
