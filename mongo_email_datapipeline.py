
# Start the code with the followings..

# pyspark --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/apptest.product?readPreference=primaryPreferred" \
#               --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/apptest.product" \
#               --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("mongo-email-pipeline").config("spark.mongodb.input.uri", "mongodb://127.0.0.1/apptest.product").config("spark.mongodb.output.uri", "mongodb://127.0.0.1/apptest.product").getOrCreate()

df = spark.read.format("mongo").load()

df.show()
