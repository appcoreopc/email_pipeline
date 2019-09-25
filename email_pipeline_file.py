from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

df = spark.read.format("csv").option("inferSchema", "false").option("header", "false").option("sep", ",").load("data/user.csv")

for eachrow in df.rdd.collect():
    print("sending email to ", eachrow[1])
