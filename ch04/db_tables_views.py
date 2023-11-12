from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("db_and_tables").enableHiveSupport().getOrCreate()


# create and use new database instead of default database
# spark.sql("CREATE DATABASE learn_spark_db")
# spark.sql("USE learn_spark_db")
# ------------------


# <<<------ manged table: changes affect on the data sources ------>>>
spark.sql(
    "CREATE TABLE IF NOT EXISTS managed_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)"
)
spark.sql(
    "INSERT INTO managed_delay_flights_tbl(date, delay, distance, origin, destination) VALUES ('2023-11-02', 10, 700, 'MHD', 'THR')"
)
spark.sql("SELECT * FROM managed_delay_flights_tbl").show()
# ------------------

# <<<------ unmanaged table: changes have no effect on data source ------>>>

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS us_delay_flights_tbl(FlightDate STRING, DepDelay INT, Distance INT, Origin STRING, Dest STRING)
        USING csv OPTIONS (
            PATH '../large_files/airline_2m.csv',
            HEADER true
        )
    """
)
spark.sql("UPDATE us_delay_flights_tbl SET FlightDate='2023'")
spark.sql("SELECT * from us_delay_flights_tbl").show(10)
