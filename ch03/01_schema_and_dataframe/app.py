from pyspark.sql import SparkSession

schema = "`id` INT, `first_name` STRING, `last_name` STRING, `url` STRING, `published` STRING, `HITS` INT, `campaigns` ARRAY<STRING>"

data = [
    [
        1,
        "Jules",
        "Damji",
        "https://test.com",
        "1/4/2023",
        4534,
        ["twitter", "linkedIn"],
    ],
    [2, "Brooke", "Wenig", "https://test.2", "5/5/2019", 8908, ["twitter", "linkedIn"]],
    [
        3,
        "Denny",
        "Lee",
        "https://test.3",
        "6/7/2022",
        7659,
        ["web", "twitter", "fb", "linkedIn"],
    ],
    [4, "Tathagata", "Das", "https://test.4", "7/9/2021", 10568, ["twitter", "fb"]],
    [
        5,
        "Matei",
        "Zaharia",
        "https:test.5",
        "4/2/2023",
        40578,
        ["web", "twitter", "fb", "linkedIn"],
    ],
    [6, "Reynold", "Xin", "https:test.6", "3/2/2015", 25568, ["twitter", "linkedIn"]],
]

if __name__ == "__main__":
    # Create Spark Session
    spark = SparkSession.builder.appName("Example_3_6").getOrCreate()

    # Create a dataframe using the schema defined above
    blogs_df = spark.createDataFrame(data, schema)

    # Show the DataFrame
    blogs_df.show()

    # print the schema used by spark to process the DataFrame
    print(blogs_df.printSchema())
