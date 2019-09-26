from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("mysql_email_pipeline").getOrCreate()


url = "jdbc:mongodb://localhost:27017/test"

properties = {
    "user": "foo",
    "password": "bar"
}

df = spark.read.jdbc(url, table="SELET * FROM CUSTOMER", properties=properties)

