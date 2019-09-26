from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType
from email.emailer import *


spark = SparkSession.builder.appName("Email_pipeline_StreamFile").getOrCreate()

userSchema = StructType().add("id", StringType(), True).add("name", StringType(), True).add("email", StringType(), True)

df = spark.readStream.option("header", "true").schema(userSchema).csv("data") 

df.writeStream.format('console').outputMode('append').start().awaitTermination()
