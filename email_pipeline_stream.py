from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType
from email.emailer import *


spark = SparkSession.builder.appName("Email_pipeline_StreamFile").getOrCreate()


# df = spark.read.format("csv").option("inferSchema", "false").option("header", "false").option("sep", ",").load("data/user.csv")

# df = spark.readStream.option("inferSchema", "false").option("header", "false").option("sep", ",").csv("data/user.csv")

# schema = StructType (
#   Array(StructField("transactionId", StringType),
#         StructField("customerId", StringType),
#         StructField("itemId", StringType),
#         StructField("amountPaid", StringType)))

# df = spark.readStream.text('data/user.csv')


userSchema = StructType().add("id", StringType(), True).add("name", StringType(), True).add("email", StringType(), True)
df = spark.readStream.schema(userSchema).format("csv").load("data/user.csv") 
df.writeStream.format('console').outputMode('append').start()

# for eachrow in df.rdd.collect():
#     mailer = Emailer()
#     #mailer.send('test demo', eachrow[2])

df.awaitTermination()