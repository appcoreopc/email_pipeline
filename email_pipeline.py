from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

df = spark.read.format("csv").option("inferSchema", "false").option("header", "false").option("sep", ",").load("data/user.csv")

df.show()

print(type(df))

for eachrow in df.rdd.collect():
    print(eachrow[1])
