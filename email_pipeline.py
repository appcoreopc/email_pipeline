from pyspark.sql import SparkSession
from email.emailer import *

spark = SparkSession.builder.appName("Test").getOrCreate()

df = spark.read.format("csv").option("inferSchema", "false").option("header", "false").option("sep", ",").load("data/user.csv")

for eachrow in df.rdd.collect():
    mailer = Emailer()
    mailer.send('test demo', eachrow[2])

